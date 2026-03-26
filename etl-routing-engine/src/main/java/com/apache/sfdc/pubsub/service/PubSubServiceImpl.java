package com.apache.sfdc.pubsub.service;

import com.apache.sfdc.common.RoutingRuntimeKeyUtils;
import com.apache.sfdc.common.SalesforceObjectSchemaBuilder;
import com.apache.sfdc.common.SalesforceObjectSchemaBuilder.SchemaResult;
import com.apache.sfdc.common.SalesforceOrgCredential;
import com.apache.sfdc.common.SalesforceOrgCredentialRepository;
import com.apache.sfdc.common.SalesforceRouterBuilderCDC;
import com.apache.sfdc.common.SqlSanitizer;
import com.apache.sfdc.pubsub.repository.PubSubRepository;
import com.apache.sfdc.storage.service.ExternalStorageRoutingJdbcExecutor;
import com.etlplatform.common.error.AppException;
import com.etlplatform.common.storage.database.DatabaseVendor;
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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@Service
@RequiredArgsConstructor
@ConditionalOnProperty(name = "app.db.enabled", havingValue = "true")
public class PubSubServiceImpl implements PubSubService {

    private static final Logger log = LoggerFactory.getLogger(PubSubServiceImpl.class);
    private static final long PUBSUB_WATCHDOG_INTERVAL_MILLIS = TimeUnit.MINUTES.toMillis(1);
    private static final long PUBSUB_RESTART_COOLDOWN_MILLIS = TimeUnit.MINUTES.toMillis(3);

    private final PubSubRepository pubSubRepository;
    private final ExternalStorageRoutingJdbcExecutor routingJdbcExecutor;
    private final SalesforceOrgCredentialRepository salesforceOrgCredentialRepository;
    private final Map<String, PubSubRuntimeState> runtimeStates = new ConcurrentHashMap<>();
    private final ScheduledExecutorService pubSubWatchdog = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread thread = new Thread(r, "pubsub-watchdog");
        thread.setDaemon(true);
        return thread;
    });
    private final AtomicBoolean pubSubWatchdogStarted = new AtomicBoolean(false);

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

        Long targetStorageId = resolveTargetStorageId(mapProperty);
        String ddl = SalesforceObjectSchemaBuilder.buildDropTableSql(
                targetSchema,
                selectedObject,
                mapProperty.get("orgName"),
                routingJdbcExecutor.resolveStrategy(targetStorageId)
        );
        routingJdbcExecutor.executeDdl("CDC", ddl, targetStorageId, mapProperty.get("targetSchema"));
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
        Long targetStorageId = resolveTargetStorageId(mapProperty);

        try (Response response = client.newCall(request).execute()) {
            JsonNode responseBody = objectMapper.readTree(response.body().string());
            schemaResult = SalesforceObjectSchemaBuilder.buildSchema(
                    targetSchema,
                    selectedObject,
                    mapProperty.get("orgName"),
                    responseBody.get("fields"),
                    objectMapper,
                    routingJdbcExecutor.resolveStrategy(targetStorageId)
            );
        } catch (IOException e) {
            throw new AppException("Failed to describe Salesforce object", e);
        }

        returnMap.put("mapType", schemaResult.mapType());
        log.warn("[CDC-SCHEMA] selectedObject={}, mapTypeSize={}, mapTypeKeys={}",
                selectedObject,
                schemaResult.mapType() == null ? 0 : schemaResult.mapType().size(),
                schemaResult.mapType() == null ? java.util.List.of() : schemaResult.mapType().keySet().stream().sorted().limit(80).toList());
        logOracleRecoveryDdlIfNeeded("CDC", mapProperty, targetStorageId, schemaResult.ddl());
        routingJdbcExecutor.executeDdl("CDC", schemaResult.ddl(), targetStorageId, targetSchema);

        boolean skipInitialLoad = Boolean.parseBoolean(mapProperty.getOrDefault("skipInitialLoad", "false"));
        if (skipInitialLoad) {
            log.info("Skipping initial load during startup recovery. selectedObject={}, orgKey={}", selectedObject, mapProperty.get("orgKey"));
            returnMap.put("initialLoadCount", 0);
            return returnMap;
        }

        ensureExternalTargetTableReadyIfNeeded(targetStorageId, targetSchema, schemaResult.ddl());

        String query = SalesforceObjectSchemaBuilder.buildInitialQuery(selectedObject, schemaResult.fields());
        request = new Request.Builder()
                .url(resolvedInstanceUrl + "/services/data/v" + apiVersion + "/query/?q=" + URLEncoder.encode(query, StandardCharsets.UTF_8))
                .addHeader("Authorization", "Bearer " + token)
                .addHeader("Content-Type", "application/json")
                .build();

        if (Boolean.parseBoolean(String.valueOf(mapProperty.getOrDefault("skipInitialLoad", "false")))) {
            returnMap.put("initialLoadCount", 0);
            log.info("CDC startup recovery mode: initial load skipped. selectedObject={}", selectedObject);
            return returnMap;
        }

        try (Response response = client.newCall(request).execute()) {
            JsonNode rootNode = objectMapper.readTree(response.body().string());
            JsonNode records = rootNode.get("records");

            if (records != null && !records.isEmpty()) {
                Instant start = Instant.now();
                int insertedData;
                if (routingJdbcExecutor.usesExternalStorage(targetStorageId)) {
                    insertedData = routingJdbcExecutor.insert(
                            SalesforceObjectSchemaBuilder.buildPreparedInsertBatch(
                                    targetSchema,
                                    resolvePhysicalTableName(mapProperty, selectedObject, targetStorageId),
                                    schemaResult,
                                    records,
                                    routingJdbcExecutor.resolveStrategy(targetStorageId)
                            ),
                            targetStorageId
                    );
                } else {
                    String physicalTableName = resolvePhysicalTableName(mapProperty, selectedObject, targetStorageId);
                    String upperQuery = SalesforceObjectSchemaBuilder.buildInsertSql(targetSchema, physicalTableName, schemaResult.soql());
                    String tailQuery = SalesforceObjectSchemaBuilder.buildInsertTail(physicalTableName, schemaResult.fields());
                    List<String> listUnderQuery = collectInsertRows(records, schemaResult);
                    insertedData = routingJdbcExecutor.insert("CDC", upperQuery, listUnderQuery, tailQuery, targetStorageId);
                }
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

        ensurePubSubWatchdogStarted();
        String routeKey = RoutingRuntimeKeyUtils.buildRouteKey(mapProperty.get("orgKey"), selectedObject, "CDC");
        AtomicLong lastEventAt = new AtomicLong(System.currentTimeMillis());
        AtomicLong lastRestartAttemptAt = new AtomicLong(0L);
        AtomicBoolean restartInProgress = new AtomicBoolean(false);

        SalesforceComponent sfEcology = new SalesforceComponent();
        sfEcology.setLoginUrl(resolvedLoginUrl == null || resolvedLoginUrl.isBlank() ? loginUrl : resolvedLoginUrl);
        applySalesforceAuth(sfEcology, mapProperty, authType);
        sfEcology.setPackages("com.apache.sfdc.router.dto");

        RouteBuilder routeBuilder = new SalesforceRouterBuilderCDC(
                targetSchema,
                selectedObject,
                mapProperty.get("orgName"),
                mapProperty.get("targetTable"),
                mapType,
                routingJdbcExecutor,
                resolveTargetStorageId(mapProperty),
                processedCount -> {
                    if (processedCount > 0) {
                        lastEventAt.set(System.currentTimeMillis());
                    }
                }
        );
        CamelContext myCamelContext = new DefaultCamelContext();
        myCamelContext.addRoutes(routeBuilder);
        myCamelContext.addComponent("sf", sfEcology);

        try {
            myCamelContext.start();
            long credentialVersion = readCredentialVersion(mapProperty.get("orgKey"));
            runtimeStates.put(routeKey, new PubSubRuntimeState(
                    new HashMap<>(mapProperty),
                    new HashMap<>(mapType),
                    myCamelContext,
                    credentialVersion,
                    lastEventAt,
                    lastRestartAttemptAt,
                    restartInProgress
            ));
            mapProperty.put("authenticationType", authType.name());
            log.info("PubSub route started. routeKey={}, loginUrl={}, instanceUrl={}", routeKey, resolvedLoginUrl, mapProperty.get("instanceUrl"));
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
    public void stopRoute(String orgKey, String selectedObject, String reason) {
        if (orgKey == null || orgKey.isBlank() || selectedObject == null || selectedObject.isBlank()) {
            return;
        }
        String routeKey = RoutingRuntimeKeyUtils.buildRouteKey(orgKey, selectedObject, "CDC");
        PubSubRuntimeState state = runtimeStates.remove(routeKey);
        if (state == null) {
            log.info("PubSub route already absent. routeKey={}, reason={}", routeKey, reason);
            return;
        }
        stopCamelContextQuietly(state.camelContext(), routeKey);
        closeCamelContextQuietly(state.camelContext(), routeKey);
        log.info("PubSub route stopped. routeKey={}, reason={}", routeKey, reason);
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

        mergeRefreshableProperties(state.mapProperty, mapProperty);
        restartPubSubRoute(routeKey, state, "manual credential refresh");
        result.put("status", "SUCCESS");
        result.put("message", "CDC routing credentials refresh가 완료되었어요.");
        return result;
    }

    @Override
    public Map<String, Object> restartRoutesForOrg(String orgKey, String reason) throws Exception {
        if (orgKey == null || orgKey.isBlank()) {
            throw new AppException("orgKey is required");
        }

        Map<String, Object> result = new HashMap<>();
        List<String> restartedRoutes = new ArrayList<>();

        for (Map.Entry<String, PubSubRuntimeState> entry : runtimeStates.entrySet()) {
            String routeKey = entry.getKey();
            PubSubRuntimeState state = entry.getValue();
            if (state == null || state.mapProperty() == null) {
                continue;
            }
            if (!orgKey.equals(state.mapProperty().get("orgKey"))) {
                continue;
            }
            restartPubSubRoute(routeKey, state, reason == null || reason.isBlank() ? "credential updated" : reason);
            restartedRoutes.add(routeKey);
        }

        result.put("orgKey", orgKey);
        result.put("protocol", "CDC");
        result.put("restartedCount", restartedRoutes.size());
        result.put("restartedRoutes", restartedRoutes);
        result.put("status", restartedRoutes.isEmpty() ? "NOT_FOUND" : "SUCCESS");
        result.put("message", restartedRoutes.isEmpty()
                ? "재시작할 CDC route가 없어요."
                : "CDC route 재시작이 완료되었어요.");
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

    private void ensureExternalTargetTableReadyIfNeeded(Long targetStorageId, String targetSchema, String ddl) {
        if (targetStorageId == null || ddl == null || ddl.isBlank()) {
            return;
        }
        routingJdbcExecutor.executeDdl("CDC", ddl, targetStorageId, targetSchema);
    }

    private void mergeRefreshableProperties(Map<String, String> current, Map<String, String> updates) {
        if (current == null || updates == null) {
            return;
        }
        copyIfPresent(current, updates, "accessToken");
        copyIfPresent(current, updates, "clientId");
        copyIfPresent(current, updates, "clientSecret");
        copyIfPresent(current, updates, "instanceUrl");
        copyIfPresent(current, updates, "orgKey");
        copyIfPresent(current, updates, "orgName");
        copyIfPresent(current, updates, "targetSchema");
        copyIfPresent(current, updates, "targetTable");
        copyIfPresent(current, updates, "targetStorageId");
        copyIfPresent(current, updates, "targetStorageName");
        copyIfPresent(current, updates, "instanceName");
        copyIfPresent(current, updates, "orgType");
        copyIfPresent(current, updates, "isSandbox");
        copyIfPresent(current, updates, "objectLabel");
    }

    private void copyIfPresent(Map<String, String> target, Map<String, String> source, String key) {
        String value = source.get(key);
        if (value != null && !value.isBlank()) {
            target.put(key, value);
        }
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

    private void logOracleRecoveryDdlIfNeeded(String protocol, Map<String, String> mapProperty, Long targetStorageId, String ddl) {
        if (!log.isDebugEnabled() || targetStorageId == null || ddl == null || ddl.isBlank()) {
            return;
        }
        if (!Boolean.parseBoolean(mapProperty.getOrDefault("skipInitialLoad", "false"))) {
            return;
        }
        if (routingJdbcExecutor.resolveStrategy(targetStorageId).vendor() != DatabaseVendor.ORACLE) {
            return;
        }

        log.debug("[ORACLE-RECOVERY-DDL] protocol={}, orgKey={}, selectedObject={}, targetSchema={}, storageId={}, ddl=\n{}",
                protocol,
                mapProperty.get("orgKey"),
                mapProperty.get("selectedObject"),
                mapProperty.get("targetSchema"),
                targetStorageId,
                ddl);
    }

    private List<String> collectInsertRows(JsonNode records, SchemaResult schemaResult) {
        List<String> listUnderQuery = new ArrayList<>();
        for (JsonNode record : records) {
            listUnderQuery.add(SalesforceObjectSchemaBuilder.buildInsertValues(record, schemaResult.fields(), schemaResult.mapType()));
        }
        return listUnderQuery;
    }

    private Long resolveTargetStorageId(Map<String, String> mapProperty) {
        if (mapProperty == null) {
            return null;
        }
        String raw = mapProperty.get("targetStorageId");
        if (raw == null || raw.isBlank()) {
            return null;
        }
        try {
            return Long.parseLong(raw.trim());
        } catch (NumberFormatException e) {
            throw new AppException("targetStorageId 값이 올바르지 않습니다: " + raw, e);
        }
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

    private void ensurePubSubWatchdogStarted() {
        if (pubSubWatchdogStarted.compareAndSet(false, true)) {
            pubSubWatchdog.scheduleWithFixedDelay(this::runPubSubWatchdog,
                    PUBSUB_WATCHDOG_INTERVAL_MILLIS,
                    PUBSUB_WATCHDOG_INTERVAL_MILLIS,
                    TimeUnit.MILLISECONDS);
        }
    }

    private void runPubSubWatchdog() {
        int activeRoutes = 0;
        int restartedRoutes = 0;
        int versionMismatches = 0;
        int stoppedRoutes = 0;

        for (Map.Entry<String, PubSubRuntimeState> entry : runtimeStates.entrySet()) {
            String routeKey = entry.getKey();
            PubSubRuntimeState state = entry.getValue();
            if (state == null || state.camelContext() == null) {
                continue;
            }

            if (shouldSkipRuntime(state.mapProperty())) {
                stopRoute(state.mapProperty().get("orgKey"), state.mapProperty().get("selectedObject"), "source_status inactive or route released");
                continue;
            }
            activeRoutes++;

            try {
                if (!state.camelContext().isStarted()) {
                    stoppedRoutes++;
                    restartedRoutes++;
                    restartPubSubRoute(routeKey, state, "camelContext not started");
                    continue;
                }

                long latestCredentialVersion = readCredentialVersion(state.mapProperty().get("orgKey"));
                if (state.credentialVersion() < latestCredentialVersion) {
                    versionMismatches++;
                    restartedRoutes++;
                    restartPubSubRoute(routeKey, state, "credential version updated");
                }
            } catch (Exception e) {
                log.warn("PubSub watchdog check 실패. routeKey={}", routeKey, e);
            }
        }

        if (activeRoutes > 0) {
            log.info("[PUBSUB-WATCHDOG] activeRoutes={}, restartedRoutes={}, versionMismatches={}, stoppedRoutes={}",
                    activeRoutes, restartedRoutes, versionMismatches, stoppedRoutes);
        }
    }

    private void restartPubSubRoute(String routeKey, PubSubRuntimeState state, String reason) {
        long now = System.currentTimeMillis();
        long lastAttempt = state.lastRestartAttemptAt().get();
        if (now - lastAttempt < PUBSUB_RESTART_COOLDOWN_MILLIS) {
            return;
        }
        if (!state.lastRestartAttemptAt().compareAndSet(lastAttempt, now)) {
            return;
        }
        if (!state.restartInProgress().compareAndSet(false, true)) {
            return;
        }

        try {
            mergeLatestCredentialSnapshot(state.mapProperty());
            if (shouldSkipRuntime(state.mapProperty())) {
                runtimeStates.remove(routeKey);
                stopCamelContextQuietly(state.camelContext(), routeKey);
                closeCamelContextQuietly(state.camelContext(), routeKey);
                log.info("PubSub route restart skipped because route is inactive. routeKey={}, reason={}", routeKey, reason);
                return;
            }
            log.warn("PubSub route restart 시작. routeKey={}, reason={}, lastEventAt={}, instanceUrl={}",
                    routeKey,
                    reason,
                    Instant.ofEpochMilli(state.lastEventAt().get()),
                    state.mapProperty().get("instanceUrl"));

            stopCamelContextQuietly(state.camelContext(), routeKey);
            closeCamelContextQuietly(state.camelContext(), routeKey);
            subscribeCDC(state.mapProperty(), new HashMap<>(state.mapType()));
            log.info("PubSub route restart 완료. routeKey={}, reason={}", routeKey, reason);
        } catch (Exception e) {
            log.error("PubSub route restart 실패. routeKey={}, reason={}", routeKey, reason, e);
        } finally {
            state.restartInProgress().set(false);
        }
    }

    private void mergeLatestCredentialSnapshot(Map<String, String> mapProperty) {
        if (mapProperty == null) {
            return;
        }
        String orgKey = mapProperty.get("orgKey");
        if (orgKey == null || orgKey.isBlank()) {
            return;
        }
        SalesforceOrgCredential credential = salesforceOrgCredentialRepository.findActiveByOrgKey(orgKey);
        if (credential == null) {
            return;
        }
        copyIfText(mapProperty, "orgKey", credential.getOrgKey());
        copyIfText(mapProperty, "orgName", credential.getOrgName());
        copyIfText(mapProperty, "instanceUrl", credential.getMyDomain());
        copyIfText(mapProperty, "clientId", credential.getClientId());
        copyIfText(mapProperty, "clientSecret", credential.getClientSecret());
        copyIfText(mapProperty, "accessToken", credential.getAccessToken());
    }

    private boolean shouldSkipRuntime(Map<String, String> mapProperty) {
        String sourceStatus = mapProperty == null ? null : mapProperty.get("sourceStatus");
        String routingStatus = mapProperty == null ? null : mapProperty.get("routingStatus");
        return "INACTIVE".equalsIgnoreCase(sourceStatus)
                || "RELEASED".equalsIgnoreCase(routingStatus)
                || "FAILED".equalsIgnoreCase(routingStatus);
    }

    private String resolvePhysicalTableName(Map<String, String> mapProperty, String selectedObject, Long targetStorageId) {
        var strategy = routingJdbcExecutor.resolveStrategy(targetStorageId);
        String orgName = mapProperty == null ? null : mapProperty.get("orgName");
        String physicalTableName = SalesforceObjectSchemaBuilder.resolvePhysicalTableName(
                mapProperty.get("targetSchema"),
                selectedObject,
                orgName,
                strategy
        );
        if (mapProperty != null) {
            mapProperty.put("targetTable", physicalTableName);
        }
        return physicalTableName;
    }

    private void copyIfText(Map<String, String> target, String key, String value) {
        if (target != null && value != null && !value.isBlank()) {
            target.put(key, value);
        }
    }

    private long readCredentialVersion(String orgKey) {
        if (orgKey == null || orgKey.isBlank()) {
            return 0L;
        }
        SalesforceOrgCredential credential = salesforceOrgCredentialRepository.findActiveByOrgKey(orgKey);
        return credential != null && credential.getCredentialVersion() != null ? credential.getCredentialVersion() : 0L;
    }

    private void stopCamelContextQuietly(CamelContext camelContext, String routeKey) {
        if (camelContext == null) {
            return;
        }
        try {
            camelContext.stop();
        } catch (Exception e) {
            log.warn("PubSub camelContext stop 실패. routeKey={}", routeKey, e);
        }
    }

    private void closeCamelContextQuietly(CamelContext camelContext, String routeKey) {
        if (camelContext == null) {
            return;
        }
        try {
            camelContext.close();
        } catch (Exception e) {
            log.warn("PubSub camelContext close 실패. routeKey={}", routeKey, e);
        }
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
                                      CamelContext camelContext,
                                      long credentialVersion,
                                      AtomicLong lastEventAt,
                                      AtomicLong lastRestartAttemptAt,
                                      AtomicBoolean restartInProgress) {
    }
}
