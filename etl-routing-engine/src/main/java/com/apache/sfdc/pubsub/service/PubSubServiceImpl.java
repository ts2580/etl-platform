package com.apache.sfdc.pubsub.service;

import com.apache.sfdc.common.AbstractSalesforceRouteService;
import com.apache.sfdc.common.SalesforceInitialLoadService;
import com.apache.sfdc.common.SalesforceHttpErrorHelper;
import com.apache.sfdc.common.SalesforceObjectSchemaBuilder;
import com.apache.sfdc.common.SalesforceObjectSchemaBuilder.SchemaResult;
import com.apache.sfdc.common.SalesforceOrgCredentialRepository;
import com.apache.sfdc.common.SalesforceRouteRuntimeState;
import com.apache.sfdc.common.SalesforceRouterBuilderCDC;
import com.apache.sfdc.common.SqlSanitizer;
import com.apache.sfdc.pubsub.repository.PubSubRepository;
import com.apache.sfdc.storage.service.ExternalStorageRoutingJdbcExecutor;
import com.etlplatform.common.error.AppException;
import com.etlplatform.common.storage.database.DatabaseVendor;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.MediaType;
import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.salesforce.AuthenticationType;
import org.apache.camel.component.salesforce.SalesforceComponent;
import org.apache.camel.impl.DefaultCamelContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@ConditionalOnProperty(name = "app.db.enabled", havingValue = "true")
public class PubSubServiceImpl extends AbstractSalesforceRouteService implements PubSubService {

    private static final Logger log = LoggerFactory.getLogger(PubSubServiceImpl.class);

    private final PubSubRepository pubSubRepository;
    private final ExternalStorageRoutingJdbcExecutor routingJdbcExecutor;
    private final SalesforceInitialLoadService salesforceInitialLoadService;

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

    public PubSubServiceImpl(PubSubRepository pubSubRepository,
                            ExternalStorageRoutingJdbcExecutor routingJdbcExecutor,
                            SalesforceOrgCredentialRepository salesforceOrgCredentialRepository,
                            SalesforceInitialLoadService salesforceInitialLoadService) {
        super(salesforceOrgCredentialRepository, log, "pubsub-watchdog");
        this.pubSubRepository = pubSubRepository;
        this.routingJdbcExecutor = routingJdbcExecutor;
        this.salesforceInitialLoadService = salesforceInitialLoadService;
    }

    @Override
    protected String protocol() {
        return "CDC";
    }

    @Override
    protected String routeLabel() {
        return "PubSub";
    }

    @Override
    protected String clientId() {
        return clientId;
    }

    @Override
    protected String clientSecret() {
        return clientSecret;
    }

    @Override
    protected void startRoute(Map<String, String> mapProperty, Map<String, Object> mapType) throws Exception {
        subscribeCDC(mapProperty, mapType);
    }

    @Override
    protected ExternalStorageRoutingJdbcExecutor routingJdbcExecutor() {
        return routingJdbcExecutor;
    }

    @Override
    protected SalesforceInitialLoadService salesforceInitialLoadService() {
        return salesforceInitialLoadService;
    }

    @Override
    protected String instanceUrl() {
        return instanceUrl;
    }

    @Override
    protected String apiVersion() {
        return apiVersion;
    }

    @Override
    protected String routeNotFoundMessage() {
        return "재시작할 CDC route가 없어요.";
    }

    @Override
    protected String routeRestartCompletedMessage() {
        return "CDC route 재시작이 완료되었어요.";
    }

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

        Map<String, Object> checkContext = SalesforceHttpErrorHelper.with(
                SalesforceHttpErrorHelper.context(
                        "CDC",
                        selectedObject,
                        mapProperty.get("orgKey"),
                        resolvedInstanceUrl,
                        resolveTargetStorageId(mapProperty)
                ),
                "selectedEntity",
                selectedEntity
        );

        try (Response response = client.newCall(checkRequest).execute()) {
            String responseBody = response.body() != null ? response.body().string() : "{}";
            if (!response.isSuccessful()) {
                SalesforceHttpErrorHelper.logHttpFailure(log, "query CDC activation", checkContext, response.code(), responseBody);
                throw SalesforceHttpErrorHelper.httpFailure("CDC 활성화 여부 조회 실패", checkContext, response.code(), responseBody);
            }

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
                SalesforceHttpErrorHelper.logHttpFailure(log, "create CDC channel member", checkContext, response.code(), responseBody);
                throw SalesforceHttpErrorHelper.httpFailure("CDC 채널 생성 실패", checkContext, response.code(), responseBody);
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
        dropTableCommon(mapProperty);
    }

    @Override
    public Map<String, Object> setTable(Map<String, String> mapProperty, String token) {
        return setTableCommon(mapProperty, token, false);
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

        ensureRouteWatchdogStarted();
        String routeKey = buildRouteKey(mapProperty.get("orgKey"), selectedObject);

        SalesforceComponent sfEcology = new SalesforceComponent();
        sfEcology.setLoginUrl(resolvedLoginUrl == null || resolvedLoginUrl.isBlank() ? loginUrl : resolvedLoginUrl);
        applySalesforceAuth(sfEcology, mapProperty);
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
                        SalesforceRouteRuntimeState runtimeState = runtimeStates.get(routeKey);
                        if (runtimeState != null) {
                            runtimeState.lastEventAt().set(System.currentTimeMillis());
                        }
                    }
                }
        );
        CamelContext myCamelContext = new DefaultCamelContext();
        myCamelContext.addRoutes(routeBuilder);
        myCamelContext.addComponent("sf", sfEcology);

        try {
            myCamelContext.start();
            long credentialVersion = readCredentialVersion(mapProperty.get("orgKey"));
            registerRouteState(routeKey, mapProperty, mapType, myCamelContext, credentialVersion);
            mapProperty.put("authenticationType", authType.name());
            log.info("PubSub route started. routeKey={}, loginUrl={}, instanceUrl={}", routeKey, resolvedLoginUrl, mapProperty.get("instanceUrl"));
        } catch (Exception e) {
            log.warn("PubSub CamelContext start 실패(auth={})", authType, e);
            myCamelContext.close();
            throw e;
        }
    }

    @Override
    public Map<String, Object> refreshCredentials(Map<String, String> mapProperty) throws Exception {
        String selectedObject = mapProperty.get("selectedObject");
        if (selectedObject == null || selectedObject.isBlank()) {
            throw new AppException("selectedObject is required");
        }
        SqlSanitizer.validateTableName(selectedObject);

        String routeKey = buildRouteKey(mapProperty.get("orgKey"), selectedObject);
        SalesforceRouteRuntimeState state = runtimeStates.get(routeKey);
        Map<String, Object> result = new HashMap<>();
        result.put("selectedObject", selectedObject);

        if (state == null) {
            result.put("status", "NOT_FOUND");
            result.put("message", "활성 CDC 런타임이 없어서 credential refresh를 건너뛰었어요.");
            return result;
        }

        mergeRefreshableProperties(state.mapProperty(), mapProperty);
        restartRoute(routeKey, state, "manual credential refresh");
        result.put("status", "SUCCESS");
        result.put("message", "CDC routing credentials refresh가 완료되었어요.");
        return result;
    }

    @Override
    public Map<String, Object> restartRoutesForOrg(String orgKey, String reason) throws Exception {
        return restartRoutesForOrgCommon(orgKey, reason);
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
