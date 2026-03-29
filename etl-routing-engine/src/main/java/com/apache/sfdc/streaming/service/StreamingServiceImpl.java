package com.apache.sfdc.streaming.service;

import com.etlplatform.common.error.AppException;
import com.apache.sfdc.common.AbstractSalesforceRouteService;
import com.apache.sfdc.common.SalesforceObjectSchemaBuilder;
import com.apache.sfdc.common.SalesforceObjectSchemaBuilder.SchemaResult;
import com.apache.sfdc.common.SalesforceInitialLoadService;
import com.apache.sfdc.common.SalesforceHttpErrorHelper;
import com.apache.sfdc.common.SalesforceOrgCredentialRepository;
import com.apache.sfdc.common.SalesforceRouteRuntimeState;
import com.apache.sfdc.common.SalesforceRouterBuilder;
import com.apache.sfdc.common.SqlSanitizer;
import com.apache.sfdc.storage.service.ExternalStorageRoutingJdbcExecutor;
import com.etlplatform.common.storage.database.DatabaseVendor;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.OkHttpClient;
import okhttp3.MediaType;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.salesforce.AuthenticationType;
import org.apache.camel.component.salesforce.SalesforceComponent;
import org.apache.camel.impl.DefaultCamelContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@ConditionalOnProperty(name = "app.db.enabled", havingValue = "true")
public class StreamingServiceImpl extends AbstractSalesforceRouteService implements StreamingService {

    private static final Logger log = LoggerFactory.getLogger(StreamingServiceImpl.class);

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

    public StreamingServiceImpl(ExternalStorageRoutingJdbcExecutor routingJdbcExecutor,
                               SalesforceOrgCredentialRepository salesforceOrgCredentialRepository,
                               SalesforceInitialLoadService salesforceInitialLoadService) {
        super(salesforceOrgCredentialRepository, log, "streaming-watchdog");
        this.routingJdbcExecutor = routingJdbcExecutor;
        this.salesforceInitialLoadService = salesforceInitialLoadService;
    }

    @Override
    protected String protocol() {
        return "STREAMING";
    }

    @Override
    protected String routeLabel() {
        return "Streaming";
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
        subscribePushTopic(mapProperty, mapProperty.get("accessToken"), mapType);
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
        return "재시작할 Streaming route가 없어요.";
    }

    @Override
    protected String routeRestartCompletedMessage() {
        return "Streaming route 재시작이 완료되었어요.";
    }

    @Override
    public Map<String, Object> setTable(Map<String, String> mapProperty, String token) {
        return setTableCommon(mapProperty, token, true);
    }

    @Override
    public String setPushTopic(Map<String, String> mapProperty, Map<String, Object> mapReturn, String token) throws Exception {
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
                .url(resolvedInstanceUrl + "/services/data/v" + apiVersion + "/sobjects/PushTopic")
                .addHeader("Authorization", "Bearer " + token)
                .addHeader("Content-Type", "application/json")
                .post(body)
                .build();

        OkHttpClient client = new OkHttpClient();
        Map<String, Object> errorContext = SalesforceHttpErrorHelper.with(
                SalesforceHttpErrorHelper.context(
                        "STREAMING",
                        selectedObject,
                        mapProperty.get("orgKey"),
                        resolvedInstanceUrl,
                        resolveTargetStorageId(mapProperty)
                ),
                "targetSchema",
                targetSchema
        );
        try (Response response = client.newCall(request).execute()) {
            String responseBody = response.body() != null ? response.body().string() : "";
            if (!response.isSuccessful()) {
                SalesforceHttpErrorHelper.logHttpFailure(log, "set PushTopic", errorContext, response.code(), responseBody);
                throw SalesforceHttpErrorHelper.httpFailure("setPushTopic 호출 실패", errorContext, response.code(), responseBody);
            }
            if (responseBody == null || responseBody.isBlank()) {
                throw new AppException("setPushTopic 응답 본문이 비어있습니다. context={"
                        + SalesforceHttpErrorHelper.formatContext(errorContext)
                        + "}");
            }
            return responseBody;
        } catch (IOException e) {
            throw SalesforceHttpErrorHelper.failure("setPushTopic 호출 실패", errorContext, e);
        }
    }

    @Override
    public void dropTable(Map<String, String> mapProperty) {
        dropTableCommon(mapProperty);
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

        ensureRouteWatchdogStarted();
        String routeKey = buildRouteKey(mapProperty.get("orgKey"), selectedObject);

        SalesforceComponent sfComponent = new SalesforceComponent();
        sfComponent.setLoginUrl(resolvedLoginUrl == null || resolvedLoginUrl.isBlank() ? loginUrl : resolvedLoginUrl);
        applySalesforceAuth(sfComponent, mapProperty);
        sfComponent.setPackages("com.apache.sfdc.router.dto");

        RouteBuilder routeBuilder = new SalesforceRouterBuilder(
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
        myCamelContext.addComponent("sf", sfComponent);

        try {
            myCamelContext.start();
            long credentialVersion = readCredentialVersion(mapProperty.get("orgKey"));
            registerRouteState(routeKey, mapProperty, mapType, myCamelContext, credentialVersion);
            mapProperty.put("authenticationType", authType.name());
            log.info("Streaming route started. routeKey={}, loginUrl={}, instanceUrl={}", routeKey, resolvedLoginUrl, mapProperty.get("instanceUrl"));
        } catch (Exception e) {
            log.warn("Streaming CamelContext start 실패(auth={})", authType, e);
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
            result.put("message", "활성 Streaming 런타임이 없어서 credential refresh를 건너뛰었어요.");
            return result;
        }

        mergeRefreshableProperties(state.mapProperty(), mapProperty);
        restartRoute(routeKey, state, "manual credential refresh");
        result.put("status", "SUCCESS");
        result.put("message", "Streaming routing credentials refresh가 완료되었어요.");
        return result;
    }

    @Override
    public Map<String, Object> restartRoutesForOrg(String orgKey, String reason) throws Exception {
        return restartRoutesForOrgCommon(orgKey, reason);
    }

}
