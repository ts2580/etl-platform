package com.apache.sfdc.streaming.service;

import com.apache.sfdc.common.SalesforceRouterBuilder;
import com.apache.sfdc.common.SalesforceRouterBuilderCDC;
import com.apache.sfdc.streaming.dto.FieldDefinition;
import com.apache.sfdc.streaming.repository.StreamingRepository;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import okhttp3.*;
import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.salesforce.SalesforceComponent;
import org.apache.camel.impl.DefaultCamelContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

@Service
@RequiredArgsConstructor
public class StreamingServiceImpl implements StreamingService {
    private final StreamingRepository streamingRepository;

    @Value("${camel.component.salesforce.instance-url}")
    private String instanceUrl;

    @Value("${camel.component.salesforce.api-version}")
    private String apiVersion;

    @Value("${camel.component.salesforce.login-url}")
    private String loginUrl;

    @Value("${camel.component.salesforce.client-id}")
    private String clientId;

    @Value("${camel.component.salesforce.client-secret}")
    private String clientSecret;

    @Override
    public Map<String, Object> setTable(Map<String, String> mapProperty, String token) {
        String selectedObject = mapProperty.get("selectedObject"); // 체크박스로 선택한 Object

        Map<String, Object> returnMap = new HashMap<>(); // 리턴에 담을거 => Type, Query

        List<FieldDefinition> listDef = new ArrayList<>();

        OkHttpClient client = new OkHttpClient();

        // 잭슨으로 역직렬화
        ObjectMapper objectMapper = new ObjectMapper();

        JsonNode rootNode;

        Request request = new Request.Builder()
                .url(instanceUrl + "/services/data/v"+apiVersion+"/sobjects/" + selectedObject + "/describe")
                .addHeader("Authorization", "Bearer " + token)
                .addHeader("Content-Type", "application/json")
                .build();

        // DDL 테이블 생성용
        StringBuilder ddl = new StringBuilder();

        // 순차적 values 구문 구성용
        List<String> listFields = new ArrayList<>();

        // soql로 받아온 값 변환용 맵
        Map<String, String> mapType = new HashMap<>();

        String label;
        
        try (Response response = client.newCall(request).execute()) {
            String responseBody = Objects.requireNonNull(response.body()).string();

            // 세일즈 포스로 따지면 JSON.deserializeUntyped();
            rootNode = objectMapper.readTree(responseBody);

            JsonNode fields = rootNode.get("fields"); // FieldDefinition 의 List

            listDef = objectMapper.convertValue(fields, new TypeReference<List<FieldDefinition>>() {});

            ddl.append("CREATE OR REPLACE table config.").append(selectedObject).append("(");

            for (FieldDefinition obj : listDef) {
                mapType.put(obj.name, obj.type);

                // 작은 따옴표는 백틱으로 이스케이프
                label = obj.label.replace("'","`");

                // salesforce에서 만드는 모든 type들은 하단의 case로 모인다. 특정 Object Type (Address 나 FirstName 생략)
                switch (obj.type) {
                    case "id" -> {
                        ddl.append("sfid VARCHAR(18) primary key not null comment '").append(label).append("',");
                    }
                    case "textarea" -> {
                        if (obj.length > 4000) {
                            ddl.append(obj.name).append(" TEXT comment '").append(label).append("',");
                        } else {
                            ddl.append(obj.name).append(" VARCHAR(").append(obj.length).append(") comment '").append(label).append("',");
                        }
                        listFields.add(obj.name);
                    }
                    case "reference" -> {
                        ddl.append(obj.name).append(" VARCHAR(18) comment '").append(label).append("',");
                        listFields.add(obj.name);
                    }
                    case "string", "picklist", "multipicklist", "phone", "url" -> {
                        ddl.append(obj.name).append(" VARCHAR(").append(obj.length).append(") comment '").append(label).append("',");
                        listFields.add(obj.name);
                    }
                    case "boolean" -> {
                        ddl.append(obj.name).append(" boolean comment '").append(label).append("',");
                        listFields.add(obj.name);
                    }
                    case "datetime" -> {
                        ddl.append(obj.name).append(" TIMESTAMP comment '").append(label).append("',");
                        listFields.add(obj.name);
                    }
                    case "date" -> {
                        ddl.append(obj.name).append(" date comment '").append(label).append("',");
                        listFields.add(obj.name);
                    }
                    case "time" -> {
                        ddl.append(obj.name).append(" time comment '").append(label).append("',");
                        listFields.add(obj.name);
                    }
                    case "double", "percent", "currency" -> {
                        ddl.append(obj.name).append(" double precision comment '").append(label).append("',");
                        listFields.add(obj.name);
                    }
                    case "int" -> {
                        ddl.append(obj.name).append(" int comment '").append(label).append("',");
                        listFields.add(obj.name);
                    }
                }
            }

            ddl.deleteCharAt(ddl.length() - 1);
            ddl.append(");");

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        returnMap.put("mapType", mapType);

        // 테이블 생성
        streamingRepository.setTable(ddl.toString());

        // 생성 후 바로 데이터 부어주기
        StringBuilder soql = new StringBuilder();
        // longtextarea의 경우 pushtopic 으로 사용 못함
        StringBuilder soqlForPushTopic = new StringBuilder();

        for (String field : listFields) {
            soql.append(field).append(",");
            if (!mapType.get(field).equals("textarea")) {
                soqlForPushTopic.append(field).append(",");
            }
        }

        soql.deleteCharAt(soql.length() - 1);
        soqlForPushTopic.deleteCharAt(soqlForPushTopic.length() - 1);

        returnMap.put("soqlForPushTopic", soqlForPushTopic);

        String query = "SELECT Id, " + soql + " FROM " + selectedObject;

        request = new Request.Builder()
                .url(instanceUrl + "/services/data/v"+apiVersion+"/query/?q=" + URLEncoder.encode(query, StandardCharsets.UTF_8))
                .addHeader("Authorization", "Bearer " + token)
                .addHeader("Content-Type", "application/json")
                .build();

        try (Response response = client.newCall(request).execute()){
            // 받아온 response를 JSON으로
            rootNode = objectMapper.readTree(Objects.requireNonNull(response.body()).string());
            // 레코드의 Array
            JsonNode records = rootNode.get("records");

            if(!records.isEmpty()){
                String upperQuery = "Insert Into config." + selectedObject + "(sfid, " + soql + ") " + "values";

                List<String> listUnderQuery = new ArrayList<>();
                StringBuilder underQuery;
                // JSONNode가 List인게 확실하면 for문 사용가능
                for (JsonNode record : records) {
                    underQuery = new StringBuilder();
                    underQuery.append("( ").append(record.get("Id")).append(",");

                    for (String field : listFields) {
                        if(mapType.get(field).equals("datetime")){
                            //  if (record.get(field) != null && record.get(field).asText() != null) {
                            //      System.out.println("======================================");
                            //      System.out.println(record.get(field));
                            //      // DateTimeFormatter를 사용하여 원본 문자열을 ZonedDateTime 객체로 변환
                            //      DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
                            //      ZonedDateTime zonedDateTime = ZonedDateTime.parse(record.get(field).asText(), formatter);
                            //
                            //      // MariaDB 형식의 문자열로 변환
                            //      DateTimeFormatter mariadbFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                            //      String mariadbDatetimeStr = zonedDateTime.withZoneSameInstant(ZoneOffset.UTC).format(mariadbFormatter);
                            //
                            //      underQuery.append(mariadbDatetimeStr).append(",");
                            //  }

                            if(mapType.get(field).equals("datetime")){
                                underQuery.append(record.get(field).toString().replace(".000+0000","").replace("T"," ")).append(",");
                            }else if(mapType.get(field).equals("time")){
                                underQuery.append(record.get(field).toString().replace("Z","")).append(",");
                            }else{
                                underQuery.append(record.get(field)).append(",");
                            }
                        }else if(mapType.get(field).equals("time")){
                            // todo Time 필드 바꾸기
                            underQuery.append(record.get(field).toString().replace("Z","")).append(",");
                        }else{
                            underQuery.append(record.get(field)).append(",");
                        }
                    }

                    underQuery.deleteCharAt(underQuery.length() - 1);
                    underQuery.append(")");
                    listUnderQuery.add(String.valueOf(underQuery));
                }

                // 시간 체크
                Instant start = Instant.now();

                int insertedData = streamingRepository.insertObject(upperQuery, listUnderQuery);

                Instant end = Instant.now();
                Duration interval = Duration.between(start, end);

                long hours = interval.toHours();
                long minutes = interval.toMinutesPart();
                long seconds = interval.toSecondsPart();

                System.out.println("테이블 : " + selectedObject + ". 삽입된 데이터 수 : " + insertedData + ". 소요시간 : " + hours + "시간 " + minutes + "분 " + seconds + "초");
            }else{
                System.out.println("테이블에 데이터 없음");
            }

            } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return returnMap;
    }

    @Override
    public String setPushTopic(Map<String, String> mapProperty, Map<String, Object> mapReturn, String token) throws Exception {
        String selectedObject = mapProperty.get("selectedObject");
        // 푸시토픽 생성하기
        Map<String, Object> pushTopic = new HashMap<>();
        pushTopic.put("Name", selectedObject);
        pushTopic.put("Query", "SELECT Id, " + String.valueOf(mapReturn.get("soqlForPushTopic")) + " FROM "  + selectedObject);
        pushTopic.put("ApiVersion", apiVersion);
        pushTopic.put("NotifyForOperationCreate", true);
        pushTopic.put("NotifyForOperationUpdate", true);
        pushTopic.put("NotifyForOperationUndelete", true);
        pushTopic.put("NotifyForOperationDelete", true);
        pushTopic.put("NotifyForFields", "Referenced");

        ObjectMapper objectMapper = new ObjectMapper();
        // JSON 문자열로 변환
        String json = objectMapper.writeValueAsString(pushTopic);

        // RequestBody 생성
        RequestBody body = RequestBody.create(json, MediaType.get("application/json; charset=utf-8"));

        Request request = new Request.Builder()
                .url(instanceUrl + "/services/data/v"+apiVersion+"/sobjects/PushTopic")
                .addHeader("Authorization", "Bearer " + token)
                .addHeader("Content-Type", "application/json")
                .post(body)
                .build();

        OkHttpClient client = new OkHttpClient();
        String returnMsg = "";
        try(Response response = client.newCall(request).execute()) {
            returnMsg = response.body().string();


        } catch (IOException e) {
            System.out.println(e.getMessage());
        }

        return returnMsg;
    }

    @Override
    public void subscribePushTopic(Map<String, String> mapProperty, String token, Map<String, Object> mapType) throws Exception {
        mapType.put("sfid", "Id");

        String selectedObject = mapProperty.get("selectedObject");

        // access token 을 직접 넣을수가 없군
        SalesforceComponent sfComponent = new SalesforceComponent();
        sfComponent.setLoginUrl(loginUrl);
        sfComponent.setClientId(clientId);
        sfComponent.setClientSecret(clientSecret);
        sfComponent.setRefreshToken(mapProperty.get("refreshToken"));
        sfComponent.setPackages("com.apache.sfdc.router.dto");

        RouteBuilder routeBuilder = new SalesforceRouterBuilder(selectedObject, mapType, streamingRepository);

        CamelContext myCamelContext = new DefaultCamelContext();
        myCamelContext.addRoutes(routeBuilder);
        myCamelContext.addComponent("sf", sfComponent);

        try{
            myCamelContext.start();
        }catch (Exception e){
            System.out.println(e.getMessage());
            myCamelContext.close();
        }
    }
}















