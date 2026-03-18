package com.apache.sfdc.pubsub.service;

import com.apache.sfdc.common.RoutingRuntimeKeyUtils;
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
import okhttp3.MediaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.salesforce.AuthenticationType;
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
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
@RequiredArgsConstructor
@ConditionalOnProperty(name = "app.db.enabled", havingValue = "true")
public class PubSubServiceImpl implements PubSubService {

    private static final Logger log = LoggerFactory.getLogger(PubSubServiceImpl.class);

    private final PubSubRepository pubSubRepository;
    private final Map<String, PubSubRuntimeState> runtimeStates = new ConcurrentHashMap<>();

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
        String targetSchema = mapProperty.get("targetSchema");
        String resolvedInstanceUrl = resolveInstanceUrl(mapProperty);

        if (targetSchema == null || targetSchema.isBlank()) {
            throw new AppException("targetSchema is required");
        }
        SqlSanitizer.validateSchemaName(targetSchema);

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
    public void dropTable(Map<String, String> mapProperty) {
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
        pubSubRepository.setTable(ddl);
    }

    @Override
    public Map<String, Object> setTable(Map<String, String> mapProperty, String token) {
        String selectedObject = mapProperty.get("selectedObject");
        String targetSchema = mapProperty.get("targetSchema");
        String resolvedInstanceUrl = resolveInstanceUrl(mapProperty);

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

        Request request = new Request.Builder()
                .url(resolvedInstanceUrl + "/services/data/v" + apiVersion + "/sobjects/" + selectedObject + "/describe")
                .addHeader("Authorization", "Bearer " + token)
                .addHeader("Content-Type", "application/json")
                .build();

        SchemaResult schemaResult;

        try (Response response = client.newCall(request).execute()) {
            JsonNode responseBody = objectMapper.readTree(response.body().string());
            schemaResult = SalesforceObjectSchemaBuilder.buildSchema(targetSchema, selectedObject, responseBody.get("fields"), objectMapper);
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
                String upperQuery = SalesforceObjectSchemaBuilder.buildInsertSql(targetSchema, selectedObject, schemaResult.soql());
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
        String targetSchema = mapProperty.get("targetSchema");

        if (targetSchema == null || targetSchema.isBlank()) {
            throw new AppException("targetSchema is required");
        }
        SqlSanitizer.validateSchemaName(targetSchema);

        if (selectedObject == null || selectedObject.isBlank()) {
            throw new AppException("selectedObject is required");
        }
        SqlSanitizer.validateTableName(selectedObject);

        try {
            startPubSubContext(mapProperty, mapType, AuthenticationType.CLIENT_CREDENTIALS);
        } catch (Exception primaryFailure) {
            log.error("subscribeCDC 실패", primaryFailure);
            throw primaryFailure;
        }
    }

    private void startPubSubContext(Map<String, String> mapProperty,
                                   Map<String, Object> mapType,
                                   AuthenticationType authType) throws Exception {
        String selectedObject = mapProperty.get("selectedObject");
        String targetSchema = mapProperty.get("targetSchema");
        String resolvedLoginUrl = resolveLoginUrl(mapProperty.get("instanceUrl"));

        SalesforceComponent sfEcology = new SalesforceComponent();
        sfEcology.setLoginUrl(resolvedLoginUrl == null || resolvedLoginUrl.isBlank() ? loginUrl : resolvedLoginUrl);
        applySalesforceAuth(sfEcology, mapProperty, authType);
        sfEcology.setPackages("com.apache.sfdc.router.dto");

        RouteBuilder routeBuilder = new SalesforceRouterBuilderCDC(targetSchema, selectedObject, mapType, pubSubRepository);
        CamelContext myCamelContext = new DefaultCamelContext();
        myCamelContext.addRoutes(routeBuilder);
        myCamelContext.addComponent("sf", sfEcology);

        try {
            myCamelContext.start();
            String routeKey = RoutingRuntimeKeyUtils.buildRouteKey(mapProperty.get("orgKey"), selectedObject, "CDC");
            runtimeStates.put(routeKey, new PubSubRuntimeState(new HashMap<>(mapProperty), new HashMap<>(mapType), myCamelContext));
            mapProperty.put("authenticationType", authType.name());
        } catch (Exception e) {
            log.warn("PubSub CamelContext start 실패(auth={})", authType, e);
            myCamelContext.close();
            throw e;
        }
    }

    @Override
    public boolean isRouteActive(String orgKey, String selectedObject) {
        String routeKey = RoutingRuntimeKeyUtils.buildRouteKey(orgKey, selectedObject, "CDC");
        PubSubRuntimeState state = runtimeStates.get(routeKey);
        return state != null && state.camelContext != null && state.camelContext.isStarted();
    }

    @Override
    public Map<String, Object> refreshCredentials(Map<String, String> mapProperty) throws Exception {
        String selectedObject = mapProperty.get("selectedObject");
        if (selectedObject == null || selectedObject.isBlank()) {
            throw new AppException("selectedObject is required");
        }
        SqlSanitizer.validateTableName(selectedObject);

        String routeKey = RoutingRuntimeKeyUtils.buildRouteKey(mapProperty.get("orgKey"), selectedObject, "CDC");
        PubSubRuntimeState state = runtimeStates.get(routeKey);
        Map<String, Object> result = new HashMap<>();
        result.put("selectedObject", selectedObject);

        if (state == null) {
            result.put("status", "NOT_FOUND");
            result.put("message", "활성 CDC 런타임이 없어서 credential refresh를 건너뛰었어요.");
            return result;
        }

        if (mapProperty.get("accessToken") != null) {
            state.mapProperty.put("accessToken", mapProperty.get("accessToken"));
        }
        if (mapProperty.get("clientId") != null) {
            state.mapProperty.put("clientId", mapProperty.get("clientId"));
        }
        if (mapProperty.get("clientSecret") != null) {
            state.mapProperty.put("clientSecret", mapProperty.get("clientSecret"));
        }
        if (mapProperty.get("instanceUrl") != null) {
            state.mapProperty.put("instanceUrl", mapProperty.get("instanceUrl"));
        }

        try {
            state.camelContext.stop();
        } catch (Exception e) {
            log.warn("기존 pubsub camelContext stop 실패. selectedObject={}", selectedObject, e);
        }
        try {
            state.camelContext.close();
        } catch (Exception e) {
            log.warn("기존 pubsub camelContext close 실패. selectedObject={}", selectedObject, e);
        }

        subscribeCDC(state.mapProperty, new HashMap<>(state.mapType));
        result.put("status", "SUCCESS");
        result.put("message", "CDC routing credentials refresh가 완료되었어요.");
        return result;
    }

    @Override
    public void markSlotActive(String selectedObject, String ingestionProtocol, String orgKey, Long routingRegistryId) {
        if (selectedObject == null || selectedObject.isBlank()) {
            throw new AppException("selectedObject is required");
        }
        SqlSanitizer.validateTableName(selectedObject);
        pubSubRepository.upsertActiveSlot(orgKey, selectedObject, ingestionProtocol, routingRegistryId, "routing slot active by pubsub");
    }

    @Override
    public void deactivateSlot(String orgKey, String selectedObject, String ingestionProtocol) {
        if (selectedObject == null || selectedObject.isBlank()) {
            throw new AppException("selectedObject is required");
        }
        SqlSanitizer.validateTableName(selectedObject);
        pubSubRepository.deactivateSlot(orgKey, ingestionProtocol, selectedObject);
    }

    @Override
    public Map<String, Object> getSlotSummary(String ingestionProtocol) {
        int used = pubSubRepository.countActiveSlots(ingestionProtocol);
        int limit = 5;

        Map<String, Object> result = new HashMap<>();
        result.put("used", used);
        result.put("limit", limit);
        result.put("available", Math.max(limit - used, 0));
        result.put("remaining", Math.max(limit - used, 0));
        return result;
    }

    private void applySalesforceAuth(SalesforceComponent sfEcology, Map<String, String> mapProperty, AuthenticationType authType) {
        String resolvedClientId = mapProperty != null ? mapProperty.get("clientId") : null;
        String resolvedClientSecret = mapProperty != null ? mapProperty.get("clientSecret") : null;

        sfEcology.setClientId(resolvedClientId != null && !resolvedClientId.isBlank() ? resolvedClientId : clientId);
        sfEcology.setClientSecret(resolvedClientSecret != null && !resolvedClientSecret.isBlank() ? resolvedClientSecret : clientSecret);
        sfEcology.setAuthenticationType(AuthenticationType.CLIENT_CREDENTIALS);
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

    private String resolveLoginUrl(String instanceUrl) {
        if (instanceUrl == null || instanceUrl.isBlank()) {
            return loginUrl;
        }
        if (instanceUrl.contains("test.salesforce.com") || instanceUrl.contains("sandbox.my.salesforce.com")) {
            return "https://test.salesforce.com";
        }
        if (instanceUrl.contains("/services/data")) {
            return "https://login.salesforce.com";
        }
        return instanceUrl;
    }

    private boolean isClientCredentialsNotSupported(Exception e) {
        String message = e == null || e.getMessage() == null ? "" : e.getMessage().toLowerCase(Locale.ROOT);
        return message.contains("invalid_grant")
                || message.contains("request not supported on this domain")
                || message.contains("request not supported")
                || message.contains("failed to start component sf")
                || message.contains("authentication")
                || message.contains("credentials");
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

    private record PubSubRuntimeState(Map<String, String> mapProperty,
                                      Map<String, Object> mapType,
                                      CamelContext camelContext) {
    }
}
