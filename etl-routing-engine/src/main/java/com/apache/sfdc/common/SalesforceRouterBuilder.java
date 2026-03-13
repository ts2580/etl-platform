package com.apache.sfdc.common;

import com.apache.sfdc.streaming.repository.StreamingRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.camel.builder.RouteBuilder;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

// 왜 클래스로 뺐냐 :: instance 직접 선언하면 모듈화가 안됨.. 구조화가 안돼서 보기 힘들다
// 게터세터 / 기본생성자 => 생성자 주입으로 선택 .. 게터세터는 뭔가 빠질수가 있어서 생성자에서 넣어주는걸로 선택함 -> 타입과 순서 맞춰서 넣게 강제할 수 있음
public class SalesforceRouterBuilder extends RouteBuilder {
    private final String selectedObject;
    private final Map<String, Object> mapType;
    private final StreamingRepository streamingRepository;

    public SalesforceRouterBuilder(String selectedObject, Map<String, Object> mapType, StreamingRepository streamingRepository) {
        this.selectedObject = selectedObject;
        this.mapType = mapType;
        this.streamingRepository = streamingRepository;
    }

    @Override
    public void configure() throws Exception {
        from("sf:subscribe:" + selectedObject)
                // 메시지들을 5초 동안 모아서 리스트로 처리할거임..Thread.sleep 안쓰도록
                .aggregate(constant(true), new ArrayListAggregationStrategy())
                .completionInterval(5000) // 5초 동안 메시지를 모음
                .process((exchange) -> {

                    ObjectMapper objectMapper = new ObjectMapper();

                    // List<Object>가 아닌 Message 로 받으면 에러남.. 타입 변환해서 받자
                    Map<String, List<Object>> messageBodies = exchange.getIn().getBody(Map.class);
                    if (messageBodies == null || messageBodies.isEmpty()) {
                        return;
                    }

                    List<String> listDeleteIds = new ArrayList<>();
                    int insertedTotal = 0;
                    int updatedTotal = 0;

                    // todo CUD 쿼리 만들기
                    for (String key : messageBodies.keySet()) {
                        List<Object> messageBody = messageBodies.get(key);
                        if (messageBody == null || messageBody.isEmpty()) {
                            continue;
                        }

                        // Insert 한 PushTopic
                        if (key.equals("created")) {
                            List<String> listUnderQuery = new ArrayList<>();
                            StringBuilder fieldsBuilder = new StringBuilder();
                            boolean initialized = false;

                            for (Object body : messageBody) {
                                Map<String, Object> mapParam = objectMapper.convertValue(body, Map.class);
                                mapParam.put("sfid", mapParam.get("Id"));
                                mapParam.remove("Id");

                                JsonNode rootNode = objectMapper.valueToTree(mapParam);
                                StringBuilder underQuery = new StringBuilder("(");

                                if (!initialized) {
                                    Iterator<Map.Entry<String, JsonNode>> fields = rootNode.fields();
                                    while (fields.hasNext()) {
                                        String fieldName = fields.next().getKey();
                                        if (!isAllowedField(fieldName)) {
                                            continue;
                                        }
                                        fieldsBuilder.append(fieldName).append(",");
                                    }
                                    if (fieldsBuilder.length() > 0) {
                                        fieldsBuilder.deleteCharAt(fieldsBuilder.length() - 1);
                                        initialized = true;
                                    }
                                }

                                if (!initialized) {
                                    continue;
                                }

                                String[] fields = fieldsBuilder.toString().split(",");
                                for (String fieldName : fields) {
                                    JsonNode fieldValue = rootNode.get(fieldName);
                                    underQuery.append(toSqlValue(fieldName, fieldValue)).append(",");
                                }

                                underQuery.deleteCharAt(underQuery.length() - 1);
                                underQuery.append(")");
                                listUnderQuery.add(String.valueOf(underQuery));
                            }

                            if (!listUnderQuery.isEmpty() && fieldsBuilder.length() > 0) {
                                String upperQuery = "Insert Into config." + selectedObject + "(" + fieldsBuilder + ") values";

                                Instant start = Instant.now();
                                int insertedData = streamingRepository.insertObject(upperQuery, listUnderQuery);
                                Instant end = Instant.now();

                                insertedTotal += insertedData;

                                Duration interval = Duration.between(start, end);
                                System.out.println("[STREAMING] created inserted=" + insertedData + ", took=" + interval.toMillis() + "ms");
                            }
                        }
                        // Update 한 PushTopic
                        else if (key.equals("updated")) {
                            for (Object body : messageBody) {
                                Map<String, Object> mapParam = objectMapper.convertValue(body, Map.class);
                                mapParam.put("sfid", mapParam.get("Id"));
                                mapParam.remove("Id");

                                Object sfidObj = mapParam.get("sfid");
                                if (sfidObj == null) {
                                    continue;
                                }

                                JsonNode rootNode = objectMapper.valueToTree(mapParam);
                                StringBuilder strUpdate = new StringBuilder();
                                strUpdate.append("UPDATE config.").append(selectedObject).append(" SET ");

                                int assignmentCount = 0;
                                Iterator<Map.Entry<String, JsonNode>> fields = rootNode.fields();
                                while (fields.hasNext()) {
                                    Map.Entry<String, JsonNode> field = fields.next();
                                    String fieldName = field.getKey();
                                    JsonNode fieldValue = field.getValue();

                                    if (!isAllowedField(fieldName) || "sfid".equals(fieldName)) {
                                        continue;
                                    }

                                    strUpdate.append(fieldName).append(" = ").append(toSqlValue(fieldName, fieldValue)).append(",");
                                    assignmentCount++;
                                }

                                if (assignmentCount == 0) {
                                    continue;
                                }

                                strUpdate.deleteCharAt(strUpdate.length() - 1);
                                strUpdate.append(" WHERE sfid = '").append(sfidObj).append("';");

                                Instant start = Instant.now();
                                int updateData = streamingRepository.updateObject(strUpdate);
                                Instant end = Instant.now();

                                updatedTotal += updateData;

                                Duration interval = Duration.between(start, end);
                                System.out.println("[STREAMING] updated=" + updateData + ", took=" + interval.toMillis() + "ms");
                            }
                        }
                        // Delete 한 PushTopic
                        else if (key.equals("deleted")) {
                            for (Object body : messageBody) {
                                Map<String, Object> mapParam = objectMapper.convertValue(body, Map.class);
                                JsonNode rootNode = objectMapper.valueToTree(mapParam);

                                rootNode.fields().forEachRemaining(field -> {
                                    String id = field.getValue() == null || field.getValue().isNull() ? null : field.getValue().asText();
                                    if (id != null && !id.isBlank()) {
                                        listDeleteIds.add("'" + id + "'");
                                    }
                                });
                            }
                        }
                    }

                    if (!listDeleteIds.isEmpty()) {
                        Instant start = Instant.now();
                        int deletedData = streamingRepository.deleteObject("config." + selectedObject, listDeleteIds);
                        Instant end = Instant.now();
                        Duration interval = Duration.between(start, end);
                        System.out.println("[STREAMING] deleted=" + deletedData + ", took=" + interval.toMillis() + "ms");
                    }

                    System.out.println("[STREAMING] summary inserted=" + insertedTotal + ", updated=" + updatedTotal);
                });
    }

    private boolean isAllowedField(String fieldName) {
        return fieldName != null && !fieldName.isBlank() && mapType.containsKey(fieldName);
    }

    private String toSqlValue(String fieldName, JsonNode fieldValue) {
        if (fieldValue == null || fieldValue.isNull()) {
            return "null";
        }

        String type = String.valueOf(mapType.get(fieldName));
        if ("datetime".equals(type)) {
            return fieldValue.toString().replace(".000Z", "").replace("T", " ");
        }
        if ("time".equals(type)) {
            return fieldValue.toString().replace("Z", "");
        }

        return fieldValue.toString();
    }
}
