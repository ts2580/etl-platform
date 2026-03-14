package com.apache.sfdc.streaming.service;

import com.etlplatform.common.error.AppException;
import com.apache.sfdc.common.SalesforceRouterBuilder;
import com.apache.sfdc.common.SqlSanitizer;
import com.apache.sfdc.streaming.dto.FieldDefinition;
import com.apache.sfdc.streaming.repository.StreamingRepository;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import okhttp3.*;
import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.salesforce.SalesforceComponent;
import org.apache.camel.impl.DefaultCamelContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

@Service
@RequiredArgsConstructor
@Slf4j
@ConditionalOnProperty(name = "app.db.enabled", havingValue = "true")
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
        String selectedObject = mapProperty.get("selectedObject");
        if (selectedObject == null || selectedObject.isBlank()) {
            throw new AppException("selectedObject is required");
        }
        SqlSanitizer.validateTableName(selectedObject);

        Map<String, Object> returnMap = new HashMap<>();
        OkHttpClient client = new OkHttpClient();
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode rootNode;

        Request request = new Request.Builder()
                .url(instanceUrl + "/services/data/v" + apiVersion + "/sobjects/" + selectedObject + "/describe")
                .addHeader("Authorization", "Bearer " + token)
                .addHeader("Content-Type", "application/json")
                .build();

        StringBuilder ddl = new StringBuilder();
        List<String> listFields = new ArrayList<>();
        Map<String, String> mapType = new HashMap<>();

        try (Response response = client.newCall(request).execute()) {
            String responseBody = Objects.requireNonNull(response.body()).string();
            rootNode = objectMapper.readTree(responseBody);
            JsonNode fields = rootNode.get("fields");
            if (fields == null || !fields.isArray()) {
                throw new AppException("Invalid Salesforce describe response");
            }

            List<FieldDefinition> listDef = objectMapper.convertValue(fields, new TypeReference<List<FieldDefinition>>() {});
            ddl.append("CREATE OR REPLACE table config.").append(selectedObject).append("(");

            for (FieldDefinition obj : listDef) {
                mapType.put(obj.name, obj.type);
                String label = obj.label.replace("'", "`");

                switch (obj.type) {
                    case "id" -> ddl.append("sfid VARCHAR(18) primary key not null comment '").append(label).append("',");
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
                    default -> {
                        ddl.append(obj.name).append(" VARCHAR(255) comment '").append(label).append("',");
                        listFields.add(obj.name);
                    }
                }
            }

            ddl.deleteCharAt(ddl.length() - 1);
            ddl.append(");");
        } catch (IOException e) {
            throw new AppException("Failed to describe Salesforce object", e);
        }

        returnMap.put("mapType", mapType);
        streamingRepository.setTable(ddl.toString());

        StringBuilder soql = new StringBuilder();
        StringBuilder soqlForPushTopic = new StringBuilder();
        for (String field : listFields) {
            SqlSanitizer.validateIdentifier(field);
            soql.append(field).append(",");
            if (!"textarea".equals(mapType.get(field))) {
                soqlForPushTopic.append(field).append(",");
            }
        }

        if (soql.isEmpty()) {
            throw new AppException("No supported fields for object: " + selectedObject);
        }

        soql.deleteCharAt(soql.length() - 1);
        soqlForPushTopic.deleteCharAt(soqlForPushTopic.length() - 1);
        returnMap.put("soqlForPushTopic", soqlForPushTopic);

        String query = "SELECT Id, " + soql + " FROM " + selectedObject;

        request = new Request.Builder()
                .url(instanceUrl + "/services/data/v" + apiVersion + "/query/?q=" + URLEncoder.encode(query, StandardCharsets.UTF_8))
                .addHeader("Authorization", "Bearer " + token)
                .addHeader("Content-Type", "application/json")
                .build();

        try (Response response = client.newCall(request).execute()) {
            rootNode = objectMapper.readTree(Objects.requireNonNull(response.body()).string());
            JsonNode records = rootNode.get("records");

            if (records != null && !records.isEmpty()) {
                String upperQuery = "Insert Into config." + selectedObject + "(sfid, " + soql + ") values";
                List<String> listUnderQuery = new ArrayList<>();

                for (JsonNode record : records) {
                    StringBuilder underQuery = new StringBuilder();
                    underQuery.append("('").append(record.get("Id").asText()).append("',");
                    for (String field : listFields) {
                        String sfType = mapType.get(field);
                        underQuery.append(SqlSanitizer.sanitizeValue(record.get(field), sfType)).append(",");
                    }
                    underQuery.deleteCharAt(underQuery.length() - 1);
                    underQuery.append(")");
                    listUnderQuery.add(underQuery.toString());
                }

                Instant start = Instant.now();
                int insertedData = streamingRepository.insertObject(upperQuery, listUnderQuery);
                Instant end = Instant.now();
                Duration interval = Duration.between(start, end);

                log.info("테이블 : {}. 삽입된 데이터 수 : {}. 소요시간 : {}시간 {}분 {}초",
                        selectedObject, insertedData, interval.toHours(), interval.toMinutesPart(), interval.toSecondsPart());
            } else {
                log.warn("테이블에 데이터 없음");
            }
        } catch (IOException e) {
            throw new AppException("Failed to query Salesforce object records", e);
        }

        return returnMap;
    }

    @Override
    public String setPushTopic(Map<String, String> mapProperty, Map<String, Object> mapReturn, String token) throws Exception {
        String selectedObject = mapProperty.get("selectedObject");
        if (selectedObject == null || selectedObject.isBlank()) {
            throw new AppException("selectedObject is required");
        }

        Map<String, Object> pushTopic = new HashMap<>();
        pushTopic.put("Name", selectedObject);
        pushTopic.put("Query", "SELECT Id, " + mapReturn.get("soqlForPushTopic") + " FROM " + selectedObject);
        pushTopic.put("ApiVersion", apiVersion);
        pushTopic.put("NotifyForOperationCreate", true);
        pushTopic.put("NotifyForOperationUpdate", true);
        pushTopic.put("NotifyForOperationUndelete", true);
        pushTopic.put("NotifyForOperationDelete", true);
        pushTopic.put("NotifyForFields", "Referenced");

        String json = new ObjectMapper().writeValueAsString(pushTopic);
        RequestBody body = RequestBody.create(json, MediaType.get("application/json; charset=utf-8"));

        Request request = new Request.Builder()
                .url(instanceUrl + "/services/data/v" + apiVersion + "/sobjects/PushTopic")
                .addHeader("Authorization", "Bearer " + token)
                .addHeader("Content-Type", "application/json")
                .post(body)
                .build();

        OkHttpClient client = new OkHttpClient();
        try (Response response = client.newCall(request).execute()) {
            if (response.body() == null) {
                throw new AppException("setPushTopic 응답 본문이 비어있습니다.");
            }
            return response.body().string();
        } catch (IOException e) {
            throw new AppException("setPushTopic 호출 실패", e);
        }
    }

    @Override
    public void subscribePushTopic(Map<String, String> mapProperty, String token, Map<String, Object> mapType) throws Exception {
        mapType.put("sfid", "Id");

        String selectedObject = mapProperty.get("selectedObject");
        if (selectedObject == null || selectedObject.isBlank()) {
            throw new AppException("selectedObject is required");
        }
        SqlSanitizer.validateTableName(selectedObject);

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

        try {
            myCamelContext.start();
        } catch (Exception e) {
            log.error("subscribePushTopic 실패", e);
            myCamelContext.close();
            throw e;
        }
    }
}
