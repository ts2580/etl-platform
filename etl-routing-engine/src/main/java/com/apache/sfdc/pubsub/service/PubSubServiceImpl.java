package com.apache.sfdc.pubsub.service;

import com.apache.sfdc.common.SalesforceObjectSchemaBuilder;
import com.apache.sfdc.common.SalesforceObjectSchemaBuilder.SchemaResult;
import com.apache.sfdc.common.SalesforceRouterBuilderCDC;
import com.apache.sfdc.common.SqlSanitizer;
import com.apache.sfdc.pubsub.repository.PubSubRepository;
import com.etlplatform.common.error.AppException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
@Slf4j
@ConditionalOnProperty(name = "app.db.enabled", havingValue = "true")
public class PubSubServiceImpl implements PubSubService {

    private final PubSubRepository pubSubRepository;

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
    public Map<String, Object> createCdcChannel(Map<String, String> mapProperty, String token) throws Exception {
        String selectedObject = mapProperty.get("selectedObject");
        String resolvedInstanceUrl = resolveInstanceUrl(mapProperty);

        if (selectedObject == null || selectedObject.isBlank()) {
            throw new AppException("selectedObject is required");
        }

        SqlSanitizer.validateTableName(selectedObject);

        OkHttpClient client = new OkHttpClient();
        ObjectMapper objectMapper = new ObjectMapper();
        String selectedEntity = toChangeEventEntity(selectedObject);

        String checkQuery = "SELECT Id FROM PlatformEventChannelMember WHERE SelectedEntity='" + selectedEntity + "'";
        Request checkRequest = new Request.Builder()
                .url(resolvedInstanceUrl + "/services/data/v" + apiVersion + "/tooling/query/?q=" + URLEncoder.encode(checkQuery, StandardCharsets.UTF_8))
                .addHeader("Authorization", "Bearer " + token)
                .addHeader("Content-Type", "application/json")
                .build();

        try (Response response = client.newCall(checkRequest).execute()) {
            if (!response.isSuccessful()) {
                throw new AppException("CDC 활성화 여부 조회 실패: " + response.code() + " " + response.message());
            }

            String responseBody = response.body() != null ? response.body().string() : "{}";
            Map<String, Object> queryResult = objectMapper.readValue(responseBody, new TypeReference<Map<String, Object>>() {});
            int totalSize = asInt(queryResult.get("totalSize"));
            if (totalSize > 0) {
                Map<String, Object> result = new HashMap<>();
                result.put("status", "ALREADY_EXISTS");
                result.put("message", selectedObject + " CDC 채널이 이미 존재해요.");
                result.put("selectedEntity", selectedEntity);
                return result;
            }
        }

        Map<String, Object> metadata = new HashMap<>();
        metadata.put("eventChannel", "ChangeEvents");
        metadata.put("selectedEntity", selectedEntity);

        Map<String, Object> requestPayload = new HashMap<>();
        requestPayload.put("FullName", "ChangeEvents_" + selectedEntity);
        requestPayload.put("Metadata", metadata);
        log.info("Creating CDC channel member. selectedObject={}, selectedEntity={}, payload={}", selectedObject, selectedEntity, requestPayload);

        RequestBody body = RequestBody.create(objectMapper.writeValueAsString(requestPayload), MediaType.get("application/json; charset=utf-8"));
        Request createRequest = new Request.Builder()
                .url(resolvedInstanceUrl + "/services/data/v" + apiVersion + "/tooling/sobjects/PlatformEventChannelMember")
                .addHeader("Authorization", "Bearer " + token)
                .addHeader("Content-Type", "application/json")
                .post(body)
                .build();

        try (Response response = client.newCall(createRequest).execute()) {
            String responseBody = response.body() != null ? response.body().string() : "";
            if (!response.isSuccessful()) {
                if (responseBody.contains("DUPLICATE") || responseBody.contains("duplicate")) {
                    Map<String, Object> result = new HashMap<>();
                    result.put("status", "ALREADY_EXISTS");
                    result.put("message", selectedObject + " CDC 채널이 이미 존재해요.");
                    result.put("responseBody", responseBody);
                    result.put("selectedEntity", selectedEntity);
                    return result;
                }
                throw new AppException("CDC 채널 생성 실패: " + response.code() + " " + response.message() + " / selectedEntity=" + selectedEntity + " / " + responseBody);
            }

            Map<String, Object> result = new HashMap<>();
            result.put("status", "CREATED");
            result.put("message", selectedObject + " CDC 채널 생성 완료");
            result.put("responseBody", responseBody);
            result.put("selectedEntity", selectedEntity);
            return result;
        }
    }

    @Override
    public Map<String, Object> setTable(Map<String, String> mapProperty, String token) {
        String selectedObject = mapProperty.get("selectedObject");
        String resolvedInstanceUrl = resolveInstanceUrl(mapProperty);

        if (selectedObject == null || selectedObject.isBlank()) {
            throw new AppException("selectedObject is required");
        }

        SqlSanitizer.validateTableName(selectedObject);

        Map<String, Object> returnMap = new HashMap<>();
        OkHttpClient client = new OkHttpClient();
        ObjectMapper objectMapper = new ObjectMapper();

        Request request = new Request.Builder()
                .url(resolvedInstanceUrl + "/services/data/v" + apiVersion + "/sobjects/" + selectedObject + "/describe")
                .addHeader("Authorization", "Bearer " + token)
                .addHeader("Content-Type", "application/json")
                .build();

        SchemaResult schemaResult;

        try (Response response = client.newCall(request).execute()) {
            JsonNode responseBody = objectMapper.readTree(response.body().string());
            schemaResult = SalesforceObjectSchemaBuilder.buildSchema(selectedObject, responseBody.get("fields"), objectMapper);
        } catch (IOException e) {
            throw new AppException("Failed to describe Salesforce object", e);
        }

        returnMap.put("mapType", schemaResult.mapType());
        pubSubRepository.setTable(schemaResult.ddl());

        String query = SalesforceObjectSchemaBuilder.buildInitialQuery(selectedObject, schemaResult.fields());
        request = new Request.Builder()
                .url(resolvedInstanceUrl + "/services/data/v" + apiVersion + "/query/?q=" + URLEncoder.encode(query, StandardCharsets.UTF_8))
                .addHeader("Authorization", "Bearer " + token)
                .addHeader("Content-Type", "application/json")
                .build();

        try (Response response = client.newCall(request).execute()) {
            JsonNode rootNode = objectMapper.readTree(response.body().string());
            JsonNode records = rootNode.get("records");

            if (records != null && !records.isEmpty()) {
                String upperQuery = SalesforceObjectSchemaBuilder.buildInsertSql(selectedObject, schemaResult.soql());
                String tailQuery = SalesforceObjectSchemaBuilder.buildInsertTail(selectedObject, schemaResult.fields());
                List<String> listUnderQuery = collectInsertRows(records, schemaResult);

                Instant start = Instant.now();
                int insertedData = pubSubRepository.insertObject(upperQuery, listUnderQuery, tailQuery);
                Instant end = Instant.now();
                Duration interval = Duration.between(start, end);

                log.info("테이블 : {}. 삽입된 데이터 수 : {}. 소요시간 : {}시간 {}분 {}초",
                        selectedObject, insertedData, interval.toHours(), interval.toMinutesPart(), interval.toSecondsPart());
                returnMap.put("initialLoadCount", insertedData);
            } else {
                log.warn("테이블에 데이터 없음");
                returnMap.put("initialLoadCount", 0);
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
        sfEcology.setLoginUrl(loginUrl);
        sfEcology.setClientId(clientId);
        sfEcology.setClientSecret(clientSecret);
        sfEcology.setRefreshToken(mapProperty.get("refreshToken"));
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

    @Override
    public void markCdcSlotActive(String selectedObject) {
        if (selectedObject == null || selectedObject.isBlank()) {
            throw new AppException("selectedObject is required");
        }
        SqlSanitizer.validateTableName(selectedObject);
        pubSubRepository.upsertActiveCdcSlot(selectedObject);
    }

    @Override
    public Map<String, Object> getCdcSlotSummary() {
        int used = pubSubRepository.countActiveCdcSlots();
        int limit = 5;

        Map<String, Object> result = new HashMap<>();
        result.put("used", used);
        result.put("limit", limit);
        result.put("available", Math.max(limit - used, 0));
        result.put("remaining", Math.max(limit - used, 0));
        return result;
    }

    private String resolveInstanceUrl(Map<String, String> mapProperty) {
        String override = mapProperty.get("instanceUrl");
        return override != null && !override.isBlank() ? override : instanceUrl;
    }

    private List<String> collectInsertRows(JsonNode records, SchemaResult schemaResult) {
        List<String> listUnderQuery = new ArrayList<>();
        for (JsonNode record : records) {
            listUnderQuery.add(SalesforceObjectSchemaBuilder.buildInsertValues(record, schemaResult.fields(), schemaResult.mapType()));
        }
        return listUnderQuery;
    }

    private String toChangeEventEntity(String selectedObject) {
        if (selectedObject == null || selectedObject.isBlank()) {
            throw new AppException("selectedObject is required");
        }
        if (selectedObject.endsWith("__c")) {
            return selectedObject.substring(0, selectedObject.length() - 3) + "__ChangeEvent";
        }
        if (selectedObject.endsWith("__ChangeEvent") || selectedObject.endsWith("ChangeEvent")) {
            return selectedObject;
        }
        return selectedObject + "ChangeEvent";
    }

    private int asInt(Object value) {
        if (value instanceof Number number) {
            return number.intValue();
        }
        if (value == null) {
            return 0;
        }
        try {
            return Integer.parseInt(String.valueOf(value));
        } catch (NumberFormatException e) {
            return 0;
        }
    }
}
