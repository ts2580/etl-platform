package com.apache.sfdc.common;

import com.apache.sfdc.streaming.repository.StreamingRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.builder.RouteBuilder;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

// 왜 클래스로 뺐냐 :: instance 직접 선언하면 모듈화가 안됨.. 구조화가 안돼서 보기 힘들다
// 게터세터 / 기본생성자 => 생성자 주입으로 선택 .. 게터세터는 뭔가 빠질수가 있어서 생성자에서 넣어주는걸로 선택함 -> 타입과 순서 맞춰서 넣어주는걸로 선택함
@Slf4j
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
        SqlSanitizer.validateTableName(selectedObject);

        from("sf:subscribe:" + selectedObject)
                .aggregate(constant(true), new ArrayListAggregationStrategy())
                .completionInterval(5000)
                .process((exchange) -> {

                    ObjectMapper objectMapper = new ObjectMapper();

                    Map<String, List<Object>> messageBodies = exchange.getIn().getBody(Map.class);
                    if (messageBodies == null || messageBodies.isEmpty()) {
                        return;
                    }

                    List<String> listDeleteIds = new ArrayList<>();
                    int insertedTotal = 0;
                    int updatedTotal = 0;

                    for (String key : messageBodies.keySet()) {
                        List<Object> messageBody = messageBodies.get(key);
                        if (messageBody == null || messageBody.isEmpty()) {
                            continue;
                        }

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
                                    underQuery.append(SqlSanitizer.sanitizeValue(fieldValue, String.valueOf(mapType.get(fieldName)))).append(",");
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
                                log.info("[STREAMING] created inserted={}, took={}ms", insertedData, interval.toMillis());
                            }
                        } else if (key.equals("updated")) {
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

                                    strUpdate.append(fieldName).append(" = ").append(SqlSanitizer.sanitizeValue(fieldValue, String.valueOf(mapType.get(fieldName)))).append(",");
                                    assignmentCount++;
                                }

                                if (assignmentCount == 0) {
                                    continue;
                                }

                                strUpdate.deleteCharAt(strUpdate.length() - 1);
                                strUpdate.append(" WHERE sfid = ").append(SqlSanitizer.quoteString(sfidObj.toString())).append(";");

                                Instant start = Instant.now();
                                int updateData = streamingRepository.updateObject(strUpdate);
                                Instant end = Instant.now();

                                updatedTotal += updateData;
                                Duration interval = Duration.between(start, end);
                                log.info("[STREAMING] updated={}, took={}ms", updateData, interval.toMillis());
                            }
                        } else if (key.equals("deleted")) {
                            for (Object body : messageBody) {
                                Map<String, Object> mapParam = objectMapper.convertValue(body, Map.class);
                                JsonNode rootNode = objectMapper.valueToTree(mapParam);

                                rootNode.fields().forEachRemaining(field -> {
                                    String id = field.getValue() == null || field.getValue().isNull() ? null : field.getValue().asText();
                                    if (id != null && !id.isBlank()) {
                                        listDeleteIds.add(SqlSanitizer.quoteSfid(id));
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
                        log.info("[STREAMING] deleted={}, took={}ms", deletedData, interval.toMillis());
                    }

                    log.info("[STREAMING] summary inserted={}, updated={}", insertedTotal, updatedTotal);
                });
    }

    private boolean isAllowedField(String fieldName) {
        return fieldName != null && !fieldName.isBlank() && mapType.containsKey(fieldName);
    }
}
