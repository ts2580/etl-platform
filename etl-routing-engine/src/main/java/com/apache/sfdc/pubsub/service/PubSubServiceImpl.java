package com.apache.sfdc.pubsub.service;

import com.etlplatform.common.error.AppException;
import com.apache.sfdc.common.SalesforceRouterBuilderCDC;
import com.apache.sfdc.common.SqlSanitizer;
import com.apache.sfdc.pubsub.repository.PubSubRepository;
import com.apache.sfdc.streaming.dto.FieldDefinition;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.salesforce.SalesforceComponent;
import org.apache.camel.impl.DefaultCamelContext;
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
public class PubSubServiceImpl implements PubSubService {

    private final PubSubRepository pubSubRepository;

    @Override
    public Map<String, Object> setTable(Map<String, String> mapProperty, String token) {
        String selectedObject = mapProperty.get("selectedObject");
        String instanceUrl = mapProperty.get("instanceUrl");

        if (selectedObject == null || selectedObject.isBlank()) {
            throw new AppException("selectedObject is required");
        }

        SqlSanitizer.validateTableName(selectedObject);

        Map<String, Object> returnMap = new HashMap<>();

        OkHttpClient client = new OkHttpClient();
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode rootNode;

        Request request = new Request.Builder()
                .url(instanceUrl + "/services/data/v61.0/sobjects/" + selectedObject + "/describe")
                .addHeader("Authorization", "Bearer " + token)
                .addHeader("Content-Type", "application/json")
                .build();

        StringBuilder ddl = new StringBuilder();
        List<String> listFields = new ArrayList<>();
        Map<String, String> mapType = new HashMap<>();

        String label;

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
                label = obj.label.replace("'", "`");

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
        pubSubRepository.setTable(ddl.toString());

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
                .url(instanceUrl + "/services/data/v61.0/query/?q=" + URLEncoder.encode(query, StandardCharsets.UTF_8))
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
                        JsonNode fieldValue = record.get(field);
                        underQuery.append(SqlSanitizer.sanitizeValue(fieldValue, sfType)).append(",");
                    }

                    underQuery.deleteCharAt(underQuery.length() - 1);
                    underQuery.append(")");
                    listUnderQuery.add(underQuery.toString());
                }

                Instant start = Instant.now();
                int insertedData = pubSubRepository.insertObject(upperQuery, listUnderQuery);
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
    public void subscribeCDC(Map<String, String> mapProperty, Map<String, Object> mapType) throws Exception {
        String selectedObject = mapProperty.get("selectedObject");
        if (selectedObject == null || selectedObject.isBlank()) {
            throw new AppException("selectedObject is required");
        }
        SqlSanitizer.validateTableName(selectedObject);

        SalesforceComponent sfEcology = new SalesforceComponent();
        sfEcology.setLoginUrl(mapProperty.get("loginUrl"));
        sfEcology.setClientId(mapProperty.get("client_id"));
        sfEcology.setClientSecret(mapProperty.get("client_secret"));
        sfEcology.setUserName(mapProperty.get("username"));
        sfEcology.setPassword(mapProperty.get("password"));
        sfEcology.setPackages("com.apache.sfdc.router.dto");

        RouteBuilder routeBuilder = new SalesforceRouterBuilderCDC(selectedObject, mapType, pubSubRepository);

        CamelContext myCamelContext = new DefaultCamelContext();
        myCamelContext.addRoutes(routeBuilder);
        myCamelContext.addComponent("sf", sfEcology);

        try {
            myCamelContext.start();
        } catch (Exception e) {
            log.error("subscribeCDC 실패", e);
            myCamelContext.close();
            throw e;
        }
    }
}
