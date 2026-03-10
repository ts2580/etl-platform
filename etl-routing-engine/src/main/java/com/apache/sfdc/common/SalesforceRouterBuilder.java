package com.apache.sfdc.common;

import com.apache.sfdc.streaming.repository.StreamingRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.camel.builder.RouteBuilder;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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

                    System.out.println(objectMapper.writeValueAsString(messageBodies));

                    List<String> listUnderQuery = new ArrayList<>();
                    StringBuilder soql = new StringBuilder();

                    List<Object> messageBody;

                    // todo CUD 쿼리 만들기
                    for (String key : messageBodies.keySet()) {

                        messageBody = messageBodies.get(key);


                        // Insert 한 PushTopic
                        if (key.equals("created")) {
                            System.out.println("=====================created=============================");

                            // soql을 한번만 설정해주기 위한 변수
                            boolean isFirst = true;

                            for (Object body : messageBody) {
                                Map<String, Object> mapParam = objectMapper.convertValue(body, Map.class);
                                mapParam.put("sfid", mapParam.get("Id"));
                                mapParam.remove("Id");

                                JsonNode rootNode = objectMapper.valueToTree(mapParam);
                                StringBuilder underQuery = new StringBuilder("(");

                                // soql은 한번만 설정하기 (Insert에 들어갈 필드임)
                                if (isFirst) {
                                    rootNode.fields().forEachRemaining(field -> {
                                        String fieldName = field.getKey();
                                        soql.append(fieldName).append(",");
                                    });
                                    isFirst = false;
                                }

                                rootNode.fields().forEachRemaining(field -> {
                                    String fieldName = field.getKey();
                                    JsonNode fieldValue = field.getValue();

                                    if (mapType.get(fieldName).equals("datetime") && fieldValue != null) {
                                        underQuery.append(fieldValue.toString().replace(".000Z", "").replace("T", " ")).append(",");
                                    } else if (mapType.get(fieldName).equals("time") && fieldValue != null) {
                                        underQuery.append(fieldValue.toString().replace("Z", "")).append(",");
                                    } else {
                                        underQuery.append(fieldValue).append(",");
                                    }
                                });

                                underQuery.deleteCharAt(underQuery.length() - 1);
                                underQuery.append(")");
                                listUnderQuery.add(String.valueOf(underQuery));

                            }

                            soql.deleteCharAt(soql.length() - 1);

                            String upperQuery = "Insert Into config." + selectedObject + "(" + soql + ") " + "values";


                            Instant start = Instant.now();

                            int insertedData = streamingRepository.insertObject(upperQuery, listUnderQuery);

                            Instant end = Instant.now();
                            Duration interval = Duration.between(start, end);


                            long hours = interval.toHours();
                            long minutes = interval.toMinutesPart();
                            long seconds = interval.toSecondsPart();

                            System.out.println("=====================================SalesforceRouterBuilder=====================================");
                            System.out.println("테이블 : " + selectedObject + ". 처리한 데이터 수 : " + insertedData + ". 소요시간 : " + hours + "시간 " + minutes + "분 " + seconds + "초");

                        }
                        // Update 한 PushTopic
                        else if (key.equals("updated")) {
                            System.out.println("=====================updated=============================");




                            for (Object body : messageBody) {
                                System.out.println("body : ");
                                System.out.println(body);

                                StringBuilder strUpdate = new StringBuilder();
                                strUpdate.append("UPDATE config." + selectedObject + " SET ");

                                Map<String, Object> mapParam = objectMapper.convertValue(body, Map.class);
                                mapParam.put("sfid", mapParam.get("Id"));
                                mapParam.remove("Id");

                                JsonNode rootNode = objectMapper.valueToTree(mapParam);
                                System.out.println("rootNode :: " + rootNode);

                                rootNode.fields().forEachRemaining(field -> {
                                    String fieldName = field.getKey();
                                    JsonNode fieldValue = field.getValue();

                                    strUpdate.append(fieldName).append(" = ");

                                    if (mapType.get(fieldName).equals("datetime") && fieldValue != null) {
                                        strUpdate.append(fieldValue.toString().replace(".000Z", "").replace("T", " ")).append(",");
                                    } else if (mapType.get(fieldName).equals("time") && fieldValue != null) {
                                        strUpdate.append(fieldValue.toString().replace("Z", "")).append(",");
                                    } else {
                                        strUpdate.append(fieldValue).append(",");
                                    }
                                });

                                strUpdate.deleteCharAt(strUpdate.length() - 1);
                                strUpdate.append("WHERE sfid = '").append(mapParam.get("sfid")).append("';");
                                System.out.println("strUpdate : " + strUpdate);


                                Instant start = Instant.now();
                                int updateData = streamingRepository.updateObject(strUpdate);

                                Instant end = Instant.now();
                                Duration interval = Duration.between(start, end);


                                long hours = interval.toHours();
                                long minutes = interval.toMinutesPart();
                                long seconds = interval.toSecondsPart();

                                System.out.println("=====================================SalesforceRouterBuilder=====================================");
                                System.out.println("테이블 : " + selectedObject + ". 처리한 데이터 수 : " + updateData + ". 소요시간 : " + hours + "시간 " + minutes + "분 " + seconds + "초");

                            }


                        }
                        // Delete 한 PushTopic
                        else if (key.equals("deleted")) {
                            System.out.println("=====================deleted=============================");
                            List<String> listDeleteIds = new ArrayList<>();


                            for (Object body : messageBody) {
                                Map<String, Object> mapParam = objectMapper.convertValue(body, Map.class);

                                JsonNode rootNode = objectMapper.valueToTree(mapParam);
                                rootNode.fields().forEachRemaining(field -> {
                                    listDeleteIds.add(String.valueOf(field.getValue()));
                                });
                            }

                            System.out.println("listDeleteIds ==> " + listDeleteIds);

                            Instant start = Instant.now();
                            int deletedData = streamingRepository.deleteObject("config." + selectedObject, listDeleteIds);

                            Instant end = Instant.now();
                            Duration interval = Duration.between(start, end);


                            long hours = interval.toHours();
                            long minutes = interval.toMinutesPart();
                            long seconds = interval.toSecondsPart();

                            System.out.println("=====================================SalesforceRouterBuilder=====================================");
                            System.out.println("테이블 : " + selectedObject + ". 처리한 데이터 수 : " + deletedData + ". 소요시간 : " + hours + "시간 " + minutes + "분 " + seconds + "초");

                        }
                    }


                });
    }
}
