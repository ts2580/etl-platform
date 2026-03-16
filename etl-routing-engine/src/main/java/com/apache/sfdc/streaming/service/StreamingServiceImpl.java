package com.apache.sfdc.streaming.service;

import com.etlplatform.common.error.AppException;
import com.apache.sfdc.common.SalesforceObjectSchemaBuilder;
import com.apache.sfdc.common.SalesforceObjectSchemaBuilder.SchemaResult;
import com.apache.sfdc.common.SalesforceRouterBuilder;
import com.apache.sfdc.common.SqlSanitizer;
import com.apache.sfdc.streaming.repository.StreamingRepository;
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
        String targetSchema = mapProperty.get("targetSchema");
        if (targetSchema == null || targetSchema.isBlank()) {
            throw new AppException("targetSchema is required");
        }

        SqlSanitizer.validateSchemaName(targetSchema);
        if (selectedObject == null || selectedObject.isBlank()) {
            throw new AppException("selectedObject is required");
        }
        SqlSanitizer.validateTableName(selectedObject);

        Map<String, Object> returnMap = new HashMap<>();
        OkHttpClient client = new OkHttpClient();
        ObjectMapper objectMapper = new ObjectMapper();
        SchemaResult schemaResult;

        Request request = new Request.Builder()
                .url(instanceUrl + "/services/data/v" + apiVersion + "/sobjects/" + selectedObject + "/describe")
                .addHeader("Authorization", "Bearer " + token)
                .addHeader("Content-Type", "application/json")
                .build();

        try (Response response = client.newCall(request).execute()) {
            JsonNode fields = objectMapper.readTree(response.body().string()).get("fields");
            schemaResult = SalesforceObjectSchemaBuilder.buildSchema(targetSchema, selectedObject, fields, objectMapper);
        } catch (IOException e) {
            throw new AppException("Failed to describe Salesforce object", e);
        }

        returnMap.put("mapType", schemaResult.mapType());
        streamingRepository.setTable(schemaResult.ddl());
        returnMap.put("soqlForPushTopic", schemaResult.soqlForPushTopic());

        String query = SalesforceObjectSchemaBuilder.buildInitialQuery(selectedObject, schemaResult.fields());
        request = new Request.Builder()
                .url(instanceUrl + "/services/data/v" + apiVersion + "/query/?q=" + URLEncoder.encode(query, StandardCharsets.UTF_8))
                .addHeader("Authorization", "Bearer " + token)
                .addHeader("Content-Type", "application/json")
                .build();

        try (Response response = client.newCall(request).execute()) {
            JsonNode records = objectMapper.readTree(response.body().string()).get("records");

            if (records != null && !records.isEmpty()) {
                String upperQuery = SalesforceObjectSchemaBuilder.buildInsertSql(targetSchema, selectedObject, schemaResult.soql());
                String tailQuery = SalesforceObjectSchemaBuilder.buildInsertTail(selectedObject, schemaResult.fields());
                List<String> listUnderQuery = collectInsertRows(records, schemaResult);

                Instant start = Instant.now();
                int insertedData = streamingRepository.insertObject(upperQuery, listUnderQuery, tailQuery);
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
        String targetSchema = mapProperty.get("targetSchema");
        if (targetSchema == null || targetSchema.isBlank()) {
            throw new AppException("targetSchema is required");
        }

        SqlSanitizer.validateSchemaName(targetSchema);
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
    public void dropTable(Map<String, String> mapProperty) throws Exception {
        String selectedObject = mapProperty.get("selectedObject");
        String targetSchema = mapProperty.get("targetSchema");
        if (targetSchema == null || targetSchema.isBlank()) {
            throw new AppException("targetSchema is required");
        }
        SqlSanitizer.validateSchemaName(targetSchema);
        if (selectedObject == null || selectedObject.isBlank()) {
            throw new AppException("selectedObject is required");
        }
        SqlSanitizer.validateTableName(selectedObject);

        String ddl = "DROP TABLE IF EXISTS `" + targetSchema + "`." + "`" + selectedObject + "`";
        streamingRepository.setTable(ddl);
    }

    @Override
    public void subscribePushTopic(Map<String, String> mapProperty, String token, Map<String, Object> mapType) throws Exception {
        mapType.put("sfid", "Id");

        String selectedObject = mapProperty.get("selectedObject");
        String targetSchema = mapProperty.get("targetSchema");
        if (targetSchema == null || targetSchema.isBlank()) {
            throw new AppException("targetSchema is required");
        }

        SqlSanitizer.validateSchemaName(targetSchema);
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

        RouteBuilder routeBuilder = new SalesforceRouterBuilder(targetSchema, selectedObject, mapType, streamingRepository);
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
    private List<String> collectInsertRows(JsonNode records, SchemaResult schemaResult) {
        List<String> listUnderQuery = new ArrayList<>();
        for (JsonNode record : records) {
            listUnderQuery.add(SalesforceObjectSchemaBuilder.buildInsertValues(record, schemaResult.fields(), schemaResult.mapType()));
        }
        return listUnderQuery;
    }
}
