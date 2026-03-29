package com.apache.sfdc.common;

import com.apache.sfdc.common.SalesforceObjectSchemaBuilder.SchemaResult;
import com.apache.sfdc.storage.service.ExternalStorageRoutingJdbcExecutor;
import com.etlplatform.common.error.AppException;
import com.etlplatform.common.storage.database.DatabaseVendor;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.camel.CamelContext;
import org.apache.camel.component.salesforce.AuthenticationType;
import org.apache.camel.component.salesforce.SalesforceComponent;
import org.slf4j.Logger;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Base class for route runtime lifecycle orchestration shared between CDC/Streaming services.
 */
public abstract class AbstractSalesforceRouteService {

    protected static final long WATCHDOG_INTERVAL_MILLIS = TimeUnit.MINUTES.toMillis(1);
    protected static final long RESTART_COOLDOWN_MILLIS = TimeUnit.MINUTES.toMillis(3);

    protected final Map<String, SalesforceRouteRuntimeState> runtimeStates = new ConcurrentHashMap<>();
    private final ScheduledExecutorService routeWatchdog;
    private final AtomicBoolean routeWatchdogStarted = new AtomicBoolean(false);

    protected final SalesforceOrgCredentialRepository salesforceOrgCredentialRepository;
    protected final Logger log;

    protected AbstractSalesforceRouteService(SalesforceOrgCredentialRepository salesforceOrgCredentialRepository,
                                            Logger log,
                                            String watchdogThreadName) {
        this.salesforceOrgCredentialRepository = salesforceOrgCredentialRepository;
        this.log = log;
        this.routeWatchdog = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, watchdogThreadName);
            thread.setDaemon(true);
            return thread;
        });
    }

    protected abstract String protocol();

    protected abstract String routeLabel();

    protected abstract String clientId();

    protected abstract String clientSecret();

    protected abstract void startRoute(Map<String, String> mapProperty, Map<String, Object> mapType) throws Exception;

    protected abstract ExternalStorageRoutingJdbcExecutor routingJdbcExecutor();

    protected abstract SalesforceInitialLoadService salesforceInitialLoadService();

    protected abstract String instanceUrl();

    protected abstract String apiVersion();

    protected abstract String routeNotFoundMessage();

    protected abstract String routeRestartCompletedMessage();

    protected void ensureRouteWatchdogStarted() {
        if (routeWatchdogStarted.compareAndSet(false, true)) {
            routeWatchdog.scheduleWithFixedDelay(this::runWatchdog,
                    WATCHDOG_INTERVAL_MILLIS,
                    WATCHDOG_INTERVAL_MILLIS,
                    TimeUnit.MILLISECONDS);
        }
    }

    protected void registerRouteState(String routeKey,
                                     Map<String, String> mapProperty,
                                     Map<String, Object> mapType,
                                     CamelContext camelContext,
                                     long credentialVersion) {
        AtomicLong lastEventAt = new AtomicLong(System.currentTimeMillis());
        AtomicLong lastRestartAttemptAt = new AtomicLong(0L);
        AtomicBoolean restartInProgress = new AtomicBoolean(false);

        runtimeStates.put(routeKey, new SalesforceRouteRuntimeState(
                new HashMap<>(mapProperty),
                new HashMap<>(mapType),
                camelContext,
                credentialVersion,
                lastEventAt,
                lastRestartAttemptAt,
                restartInProgress
        ));
    }

    protected String buildRouteKey(String orgKey, String selectedObject) {
        return RoutingRuntimeKeyUtils.buildRouteKey(orgKey, selectedObject, protocol());
    }

    public boolean isRouteActive(String orgKey, String selectedObject) {
        SalesforceRouteRuntimeState state = runtimeStates.get(buildRouteKey(orgKey, selectedObject));
        return state != null && state.camelContext() != null && state.camelContext().isStarted();
    }

    public void stopRoute(String orgKey, String selectedObject, String reason) {
        if (orgKey == null || orgKey.isBlank() || selectedObject == null || selectedObject.isBlank()) {
            return;
        }
        String routeKey = buildRouteKey(orgKey, selectedObject);
        SalesforceRouteRuntimeState state = runtimeStates.remove(routeKey);
        if (state == null) {
            log.info("{} route already absent. routeKey={}, reason={}", routeLabel(), routeKey, reason);
            return;
        }
        stopCamelContextQuietly(state.camelContext(), routeKey);
        closeCamelContextQuietly(state.camelContext(), routeKey);
        log.info("{} route stopped. routeKey={}, reason={}", routeLabel(), routeKey, reason);
    }

    protected void runWatchdog() {
        int activeRoutes = 0;
        int restartedRoutes = 0;
        int versionMismatches = 0;
        int stoppedRoutes = 0;

        for (Map.Entry<String, SalesforceRouteRuntimeState> entry : runtimeStates.entrySet()) {
            String routeKey = entry.getKey();
            SalesforceRouteRuntimeState state = entry.getValue();
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
                    restartRoute(routeKey, state, "camelContext not started");
                    continue;
                }

                long latestCredentialVersion = readCredentialVersion(state.mapProperty().get("orgKey"));
                if (state.credentialVersion() < latestCredentialVersion) {
                    versionMismatches++;
                    restartedRoutes++;
                    restartRoute(routeKey, state, "credential version updated");
                }
            } catch (Exception e) {
                log.warn("{} watchdog check 실패. routeKey={}", routeLabel(), routeKey, e);
            }
        }

        if (activeRoutes > 0) {
            log.info("[{}-WATCHDOG] activeRoutes={}, restartedRoutes={}, versionMismatches={}, stoppedRoutes={}",
                    protocol(), activeRoutes, restartedRoutes, versionMismatches, stoppedRoutes);
        }
    }

    protected void restartRoute(String routeKey, SalesforceRouteRuntimeState state, String reason) {
        long now = System.currentTimeMillis();
        long lastAttempt = state.lastRestartAttemptAt().get();
        if (now - lastAttempt < RESTART_COOLDOWN_MILLIS) {
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
                log.info("{} route restart skipped because route is inactive. routeKey={}, reason={}",
                        routeLabel(), routeKey, reason);
                return;
            }

            log.warn("{} route restart 시작. routeKey={}, reason={}, lastEventAt={}, instanceUrl={}",
                    routeLabel(),
                    routeKey,
                    reason,
                    Instant.ofEpochMilli(state.lastEventAt().get()),
                    state.mapProperty().get("instanceUrl"));

            stopCamelContextQuietly(state.camelContext(), routeKey);
            closeCamelContextQuietly(state.camelContext(), routeKey);
            startRoute(state.mapProperty(), new HashMap<>(state.mapType()));
            log.info("{} route restart 완료. routeKey={}, reason={}",
                    routeLabel(),
                    routeKey,
                    reason);
        } catch (Exception e) {
            log.error("{} route restart 실패. routeKey={}, reason={}", routeLabel(), routeKey, reason, e);
        } finally {
            state.restartInProgress().set(false);
        }
    }

    public Map<String, Object> restartRoutesForOrgCommon(String orgKey, String reason) {
        if (orgKey == null || orgKey.isBlank()) {
            throw new IllegalArgumentException("orgKey is required");
        }

        Map<String, Object> result = new HashMap<>();
        List<String> restartedRoutes = new ArrayList<>();

        for (Map.Entry<String, SalesforceRouteRuntimeState> entry : runtimeStates.entrySet()) {
            String routeKey = entry.getKey();
            SalesforceRouteRuntimeState state = entry.getValue();
            if (state == null || state.mapProperty() == null) {
                continue;
            }
            if (!orgKey.equals(state.mapProperty().get("orgKey"))) {
                continue;
            }
            restartRoute(routeKey, state, reason == null || reason.isBlank() ? "credential updated" : reason);
            restartedRoutes.add(routeKey);
        }

        result.put("orgKey", orgKey);
        result.put("protocol", protocol());
        result.put("restartedCount", restartedRoutes.size());
        result.put("restartedRoutes", restartedRoutes);
        result.put("status", restartedRoutes.isEmpty() ? "NOT_FOUND" : "SUCCESS");
        result.put("message", restartedRoutes.isEmpty()
                ? routeNotFoundMessage()
                : routeRestartCompletedMessage());
        return result;
    }

    protected void mergeRefreshableProperties(Map<String, String> current, Map<String, String> updates) {
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

    protected void copyIfPresent(Map<String, String> target, Map<String, String> source, String key) {
        String value = source.get(key);
        if (value != null && !value.isBlank()) {
            target.put(key, value);
        }
    }

    protected void mergeLatestCredentialSnapshot(Map<String, String> mapProperty) {
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

    protected boolean shouldSkipRuntime(Map<String, String> mapProperty) {
        String sourceStatus = mapProperty == null ? null : mapProperty.get("sourceStatus");
        String routingStatus = mapProperty == null ? null : mapProperty.get("routingStatus");
        return "INACTIVE".equalsIgnoreCase(sourceStatus)
                || "RELEASED".equalsIgnoreCase(routingStatus)
                || "FAILED".equalsIgnoreCase(routingStatus);
    }

    protected long readCredentialVersion(String orgKey) {
        if (orgKey == null || orgKey.isBlank()) {
            return 0L;
        }
        SalesforceOrgCredential credential = salesforceOrgCredentialRepository.findActiveByOrgKey(orgKey);
        return credential != null && credential.getCredentialVersion() != null ? credential.getCredentialVersion() : 0L;
    }

    protected void applySalesforceAuth(SalesforceComponent sfComponent, Map<String, String> mapProperty) {
        String resolvedClientId = mapProperty != null ? mapProperty.get("clientId") : null;
        String resolvedClientSecret = mapProperty != null ? mapProperty.get("clientSecret") : null;

        sfComponent.setClientId(resolvedClientId != null && !resolvedClientId.isBlank() ? resolvedClientId : clientId());
        sfComponent.setClientSecret(resolvedClientSecret != null && !resolvedClientSecret.isBlank() ? resolvedClientSecret : clientSecret());
        sfComponent.setAuthenticationType(AuthenticationType.CLIENT_CREDENTIALS);
    }

    protected Map<String, Object> setTableCommon(Map<String, String> mapProperty,
                                                 String token,
                                                 boolean includePushTopicSoql) {
        String selectedObject = requiredSelectedObject(mapProperty);
        String targetSchema = requiredTargetSchema(mapProperty);
        String resolvedInstanceUrl = resolveInstanceUrl(mapProperty);
        Long targetStorageId = resolveTargetStorageId(mapProperty);

        Map<String, Object> returnMap = new HashMap<>();
        OkHttpClient client = new OkHttpClient();
        ObjectMapper objectMapper = new ObjectMapper();
        SchemaResult schemaResult;

        Request request = new Request.Builder()
                .url(resolvedInstanceUrl + "/services/data/v" + apiVersion() + "/sobjects/" + selectedObject + "/describe")
                .addHeader("Authorization", "Bearer " + token)
                .addHeader("Content-Type", "application/json")
                .build();

        Map<String, Object> errorContext = SalesforceHttpErrorHelper.context(
                protocol(),
                selectedObject,
                mapProperty.get("orgKey"),
                resolvedInstanceUrl,
                targetStorageId
        );

        try (Response response = client.newCall(request).execute()) {
            String responseBody = response.body() != null ? response.body().string() : "";
            if (!response.isSuccessful()) {
                SalesforceHttpErrorHelper.logHttpFailure(log, "describe Salesforce object", errorContext, response.code(), responseBody);
                throw SalesforceHttpErrorHelper.httpFailure("Failed to describe Salesforce object", errorContext, response.code(), responseBody);
            }

            JsonNode describeNode = objectMapper.readTree(responseBody);
            JsonNode fields = describeNode.get("fields");
            if (fields == null || !fields.isArray()) {
                throw new AppException("Invalid Salesforce describe response: context={"
                        + SalesforceHttpErrorHelper.formatContext(errorContext)
                        + ", detail=fields is missing");
            }
            schemaResult = SalesforceObjectSchemaBuilder.buildSchema(
                    targetSchema,
                    selectedObject,
                    mapProperty.get("targetTable"),
                    mapProperty.get("orgName"),
                    fields,
                    objectMapper,
                    routingJdbcExecutor().resolveStrategy(targetStorageId)
            );
        } catch (IOException e) {
            throw SalesforceHttpErrorHelper.failure("Failed to describe Salesforce object", errorContext, e);
        }

        returnMap.put("mapType", schemaResult.mapType());
        if (includePushTopicSoql) {
            returnMap.put("soqlForPushTopic", schemaResult.soqlForPushTopic());
        }

        logOracleRecoveryDdlIfNeeded(mapProperty, targetStorageId, schemaResult.ddl());
        routingJdbcExecutor().executeDdl(protocol(), schemaResult.ddl(), targetStorageId, targetSchema);

        boolean skipInitialLoad = Boolean.parseBoolean(mapProperty.getOrDefault("skipInitialLoad", "false"));
        if (skipInitialLoad) {
            log.info("Skipping initial load during startup recovery. selectedObject={}, orgKey={}", selectedObject, mapProperty.get("orgKey"));
            returnMap.put("initialLoadCount", 0);
            return returnMap;
        }

        ensureExternalTargetTableReadyIfNeeded(targetStorageId, targetSchema, schemaResult.ddl());

        int insertedData = salesforceInitialLoadService().load(
                protocol(),
                mapProperty,
                token,
                resolvedInstanceUrl,
                apiVersion(),
                selectedObject,
                schemaResult,
                targetStorageId
        );
        if (insertedData == 0) {
            log.warn("테이블에 데이터 없음");
        }
        returnMap.put("initialLoadCount", insertedData);
        return returnMap;
    }

    protected void dropTableCommon(Map<String, String> mapProperty) {
        String selectedObject = requiredSelectedObject(mapProperty);
        String targetSchema = requiredTargetSchema(mapProperty);
        Long targetStorageId = resolveTargetStorageId(mapProperty);
        String ddl = SalesforceObjectSchemaBuilder.buildDropTableSql(
                targetSchema,
                selectedObject,
                mapProperty.get("orgName"),
                routingJdbcExecutor().resolveStrategy(targetStorageId)
        );
        routingJdbcExecutor().executeDdl(protocol(), ddl, targetStorageId, targetSchema);
    }

    protected String requiredSelectedObject(Map<String, String> mapProperty) {
        String selectedObject = mapProperty != null ? mapProperty.get("selectedObject") : null;
        if (selectedObject == null || selectedObject.isBlank()) {
            throw new AppException("selectedObject is required");
        }
        SqlSanitizer.validateTableName(selectedObject);
        return selectedObject;
    }

    protected String requiredTargetSchema(Map<String, String> mapProperty) {
        String targetSchema = mapProperty != null ? mapProperty.get("targetSchema") : null;
        if (targetSchema == null || targetSchema.isBlank()) {
            throw new AppException("targetSchema is required");
        }
        SqlSanitizer.validateSchemaName(targetSchema);
        return targetSchema;
    }

    protected Long resolveTargetStorageId(Map<String, String> mapProperty) {
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

    protected String resolveInstanceUrl(Map<String, String> mapProperty) {
        String configured = mapProperty != null ? mapProperty.get("instanceUrl") : null;
        if (configured == null || configured.isBlank()) {
            return instanceUrl();
        }
        if (configured.contains("/services/data")) {
            return configured.substring(0, configured.indexOf("/services/data"));
        }
        return configured;
    }

    protected String resolveLoginUrl(String instanceUrl) {
        if (instanceUrl == null || instanceUrl.isBlank()) {
            return instanceUrl();
        }
        if (instanceUrl.contains("test.salesforce.com") || instanceUrl.contains("sandbox.my.salesforce.com")) {
            return "https://test.salesforce.com";
        }
        if (instanceUrl.contains("/services/data")) {
            return "https://login.salesforce.com";
        }
        return instanceUrl;
    }

    protected void ensureExternalTargetTableReadyIfNeeded(Long targetStorageId, String targetSchema, String ddl) {
        if (targetStorageId == null || ddl == null || ddl.isBlank()) {
            return;
        }
        routingJdbcExecutor().executeDdl(protocol(), ddl, targetStorageId, targetSchema);
    }

    protected void logOracleRecoveryDdlIfNeeded(Map<String, String> mapProperty, Long targetStorageId, String ddl) {
        if (!log.isDebugEnabled() || targetStorageId == null || ddl == null || ddl.isBlank()) {
            return;
        }
        if (!Boolean.parseBoolean(mapProperty.getOrDefault("skipInitialLoad", "false"))) {
            return;
        }
        if (routingJdbcExecutor().resolveStrategy(targetStorageId).vendor() != DatabaseVendor.ORACLE) {
            return;
        }

        log.debug("[ORACLE-RECOVERY-DDL] protocol={}, orgKey={}, selectedObject={}, targetSchema={}, storageId={}, ddl=\n{}",
                protocol(),
                mapProperty.get("orgKey"),
                mapProperty.get("selectedObject"),
                mapProperty.get("targetSchema"),
                targetStorageId,
                ddl);
    }

    protected void stopCamelContextQuietly(CamelContext camelContext, String routeKey) {
        if (camelContext == null) {
            return;
        }
        try {
            camelContext.stop();
        } catch (Exception e) {
            log.warn("{} camelContext stop 실패. routeKey={}", routeLabel(), routeKey, e);
        }
    }

    private void closeCamelContextQuietly(CamelContext camelContext, String routeKey) {
        if (camelContext == null) {
            return;
        }
        try {
            camelContext.close();
        } catch (Exception e) {
            log.warn("{} camelContext close 실패. routeKey={}", routeLabel(), routeKey, e);
        }
    }

    private void copyIfText(Map<String, String> target, String key, String value) {
        if (target != null && value != null && !value.isBlank()) {
            target.put(key, value);
        }
    }
}
