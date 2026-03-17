package com.apache.sfdc.streaming.service;

import com.etlplatform.common.error.AppException;
import com.apache.sfdc.common.RoutingRuntimeKeyUtils;
import com.apache.sfdc.common.SalesforceObjectSchemaBuilder;
import com.apache.sfdc.common.SalesforceObjectSchemaBuilder.SchemaResult;
import com.apache.sfdc.common.SalesforceRouterBuilder;
import com.apache.sfdc.common.SqlSanitizer;
import com.apache.sfdc.streaming.repository.StreamingRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import okhttp3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Service
@RequiredArgsConstructor
@ConditionalOnProperty(name = "app.db.enabled", havingValue = "true")
public class StreamingServiceImpl implements StreamingService {

    private static final Logger log = LoggerFactory.getLogger(StreamingServiceImpl.class);
    private final StreamingRepository streamingRepository;
    private final Map<String, StreamingRuntimeState> runtimeStates = new ConcurrentHashMap<>();

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

        try {
            startStreamingContext(mapProperty, token, mapType, AuthenticationType.CLIENT_CREDENTIALS);
        } catch (Exception primaryFailure) {
            log.error("subscribePushTopic 실패", primaryFailure);
            throw primaryFailure;
        }
    }

    private void startStreamingContext(Map<String, String> mapProperty,
                                      String token,
                                      Map<String, Object> mapType,
                                      AuthenticationType authType) throws Exception {
        String selectedObject = mapProperty.get("selectedObject");
        String targetSchema = mapProperty.get("targetSchema");
        String resolvedLoginUrl = resolveLoginUrl(mapProperty.get("instanceUrl"));

        SalesforceComponent sfComponent = new SalesforceComponent();
        sfComponent.setLoginUrl(resolvedLoginUrl == null || resolvedLoginUrl.isBlank() ? loginUrl : resolvedLoginUrl);
        applySalesforceAuth(sfComponent, mapProperty, authType);
        sfComponent.setPackages("com.apache.sfdc.router.dto");

        RouteBuilder routeBuilder = new SalesforceRouterBuilder(targetSchema, selectedObject, mapType, streamingRepository);
        CamelContext myCamelContext = new DefaultCamelContext();
        myCamelContext.addRoutes(routeBuilder);
        myCamelContext.addComponent("sf", sfComponent);

        try {
            myCamelContext.start();
            String routeKey = RoutingRuntimeKeyUtils.buildRouteKey(mapProperty.get("orgKey"), selectedObject, "STREAMING");
            runtimeStates.put(routeKey, new StreamingRuntimeState(new HashMap<>(mapProperty), new HashMap<>(mapType), myCamelContext));
            mapProperty.put("authenticationType", authType.name());
        } catch (Exception e) {
            log.warn("Streaming CamelContext start 실패(auth={})", authType, e);
            myCamelContext.close();
            throw e;
        }
    }

    @Override
    public boolean isRouteActive(String orgKey, String selectedObject) {
        String routeKey = RoutingRuntimeKeyUtils.buildRouteKey(orgKey, selectedObject, "STREAMING");
        StreamingRuntimeState state = runtimeStates.get(routeKey);
        return state != null && state.camelContext != null && state.camelContext.isStarted();
    }

    @Override
    public Map<String, Object> refreshCredentials(Map<String, String> mapProperty) throws Exception {
        String selectedObject = mapProperty.get("selectedObject");
        if (selectedObject == null || selectedObject.isBlank()) {
            throw new AppException("selectedObject is required");
        }
        SqlSanitizer.validateTableName(selectedObject);

        String routeKey = RoutingRuntimeKeyUtils.buildRouteKey(mapProperty.get("orgKey"), selectedObject, "STREAMING");
        StreamingRuntimeState state = runtimeStates.get(routeKey);
        Map<String, Object> result = new HashMap<>();
        result.put("selectedObject", selectedObject);

        if (state == null) {
            result.put("status", "NOT_FOUND");
            result.put("message", "활성 Streaming 런타임이 없어서 credential refresh를 건너뛰었어요.");
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
            log.warn("기존 streaming camelContext stop 실패. selectedObject={}", selectedObject, e);
        }
        try {
            state.camelContext.close();
        } catch (Exception e) {
            log.warn("기존 streaming camelContext close 실패. selectedObject={}", selectedObject, e);
        }

        subscribePushTopic(state.mapProperty, state.mapProperty.get("accessToken"), new HashMap<>(state.mapType));
        result.put("status", "SUCCESS");
        result.put("message", "Streaming routing credentials refresh가 완료되었어요.");
        return result;
    }

    private void applySalesforceAuth(SalesforceComponent sfComponent, Map<String, String> mapProperty, AuthenticationType authType) {
        String resolvedClientId = mapProperty != null ? mapProperty.get("clientId") : null;
        String resolvedClientSecret = mapProperty != null ? mapProperty.get("clientSecret") : null;

        sfComponent.setClientId(resolvedClientId != null && !resolvedClientId.isBlank() ? resolvedClientId : clientId);
        sfComponent.setClientSecret(resolvedClientSecret != null && !resolvedClientSecret.isBlank() ? resolvedClientSecret : clientSecret);
        sfComponent.setAuthenticationType(AuthenticationType.CLIENT_CREDENTIALS);
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

    private record StreamingRuntimeState(Map<String, String> mapProperty,
                                         Map<String, Object> mapType,
                                         CamelContext camelContext) {
    }
}
