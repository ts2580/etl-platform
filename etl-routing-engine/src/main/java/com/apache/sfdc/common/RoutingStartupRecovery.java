package com.apache.sfdc.common;

import com.apache.sfdc.pubsub.service.PubSubService;
import com.apache.sfdc.streaming.service.StreamingService;
import com.etlplatform.common.error.AppException;
import com.etlplatform.common.salesforce.SalesforceClientCredentialsClient;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "app.db.enabled", havingValue = "true")
public class RoutingStartupRecovery {

    private static final Logger log = LoggerFactory.getLogger(RoutingStartupRecovery.class);

    private final RoutingRegistryRepository routingRegistryRepository;
    private final RoutingRegistrySupport routingRegistrySupport;
    private final SalesforceOrgCredentialRepository salesforceOrgCredentialRepository;
    private final SalesforceClientCredentialsClient salesforceOAuthClient;
    private final StreamingService streamingService;
    private final PubSubService pubSubService;

    @org.springframework.beans.factory.annotation.Value("${salesforce.tokenUrl:/services/oauth2/token}")
    private String configuredTokenUrl;

    @EventListener(ApplicationReadyEvent.class)
    public void recoverRoutesOnStartup() {
        List<Map<String, Object>> activeRoutes = routingRegistryRepository.findActiveRoutes();
        if (activeRoutes == null || activeRoutes.isEmpty()) {
            log.info("[routing-recovery] startup skip: no ACTIVE routes found in routing_registry");
            return;
        }

        AtomicInteger successCount = new AtomicInteger();
        AtomicInteger skippedCount = new AtomicInteger();
        AtomicInteger failedCount = new AtomicInteger();

        log.info("[routing-recovery] startup begin: targetRoutes={}", activeRoutes.size());
        for (Map<String, Object> route : activeRoutes) {
            recoverSingleRoute(route, successCount, skippedCount, failedCount);
        }
        log.info("[routing-recovery] startup done: targetRoutes={}, recovered={}, skipped={}, failed={}",
                activeRoutes.size(), successCount.get(), skippedCount.get(), failedCount.get());
    }

    private void recoverSingleRoute(Map<String, Object> route,
                                    AtomicInteger successCount,
                                    AtomicInteger skippedCount,
                                    AtomicInteger failedCount) {
        String orgKey = text(route.get("orgKey"));
        String selectedObject = text(route.get("objectName"));
        String protocol = text(route.get("protocol")).toUpperCase(Locale.ROOT);
        String actor = "routing-startup-recovery";

        if (orgKey.isBlank() || selectedObject.isBlank() || protocol.isBlank()) {
            skippedCount.incrementAndGet();
            log.warn("[routing-recovery] skip: invalid route metadata. route={}", route);
            return;
        }

        if (isAlreadyActive(orgKey, protocol, selectedObject)) {
            skippedCount.incrementAndGet();
            log.info("[routing-recovery] skip: already active. orgKey={}, selectedObject={}, protocol={}", orgKey, selectedObject, protocol);
            return;
        }

        try {
            String myDomain = text(route.get("myDomain"));
            String normalizedMyDomain = normalizeCredentialDomain(myDomain);
            SalesforceOrgCredential credential = salesforceOrgCredentialRepository.findActiveByOrgIdentifier(orgKey, myDomain, normalizedMyDomain);
            if (credential == null) {
                throw new AppException("활성 org credential을 찾지 못했어요. orgKey=" + orgKey + ", myDomain=" + myDomain);
            }

            SalesforceClientCredentialsClient.TokenResponse tokenResponse = refreshOrgToken(credential);
            String targetOrgKey = firstNonBlank(credential.getOrgKey(), orgKey);
            salesforceOrgCredentialRepository.updateAccessToken(targetOrgKey, tokenResponse.accessToken());

            Map<String, String> mapProperty = buildMapProperty(route, credential, tokenResponse);
            if ("CDC".equals(protocol)) {
                recoverCdcRoute(mapProperty, actor);
            } else {
                recoverStreamingRoute(mapProperty, actor);
            }
            successCount.incrementAndGet();
            log.info("[routing-recovery] recovered: orgKey={}, selectedObject={}, protocol={}", orgKey, selectedObject, protocol);
        } catch (Exception e) {
            failedCount.incrementAndGet();
            String failureMessage = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
            log.error("[routing-recovery] failed: orgKey={}, selectedObject={}, protocol={}, message={}", orgKey, selectedObject, protocol, failureMessage, e);
        }
    }

    private boolean isAlreadyActive(String orgKey, String protocol, String selectedObject) {
        String normalizedOrgKey = RoutingRuntimeKeyUtils.normalizeOrgKey(orgKey);
        return "CDC".equals(protocol)
                ? pubSubService.isRouteActive(normalizedOrgKey, selectedObject)
                : streamingService.isRouteActive(normalizedOrgKey, selectedObject);
    }

    private void recoverStreamingRoute(Map<String, String> mapProperty, String actor) throws Exception {
        String accessToken = mapProperty.get("accessToken");

        Map<String, Object> mapReturn = streamingService.setTable(mapProperty, accessToken);
        streamingService.setPushTopic(mapProperty, mapReturn, accessToken);
        streamingService.subscribePushTopic(mapProperty, accessToken, castMap(mapReturn.get("mapType")));
        log.info("[routing-recovery] streaming route revived without registry rewrite. orgKey={}, selectedObject={}",
                mapProperty.get("orgKey"), mapProperty.get("selectedObject"));
    }

    private void recoverCdcRoute(Map<String, String> mapProperty, String actor) throws Exception {
        String selectedObject = mapProperty.get("selectedObject");
        String orgKey = mapProperty.get("orgKey");
        String accessToken = mapProperty.get("accessToken");

        pubSubService.createCdcChannel(mapProperty, accessToken);
        Map<String, Object> mapReturn = pubSubService.setTable(mapProperty, accessToken);
        pubSubService.subscribeCDC(mapProperty, castMap(mapReturn.get("mapType")));
        pubSubService.markSlotActive(selectedObject, "CDC", orgKey, null);
        log.info("[routing-recovery] cdc route revived without registry rewrite. orgKey={}, selectedObject={}", orgKey, selectedObject);
    }

    private SalesforceClientCredentialsClient.TokenResponse refreshOrgToken(SalesforceOrgCredential credential) throws Exception {
        if ((credential.getClientId() == null || credential.getClientId().isBlank()) ||
                (credential.getClientSecret() == null || credential.getClientSecret().isBlank())) {
            throw new AppException("clientId/clientSecret이 없어서 startup recovery를 진행할 수 없어요. orgKey=" + credential.getOrgKey());
        }

        String tokenUrl = resolveTokenUrl(credential.getMyDomain(), configuredTokenUrl);

        log.info("Using client_credentials grant for startup recovery. orgKey={}", credential.getOrgKey());
        return salesforceOAuthClient.exchangeClientCredentials(tokenUrl, credential.getClientId(), credential.getClientSecret());
    }

    private Map<String, String> buildMapProperty(Map<String, Object> route,
                                                 SalesforceOrgCredential credential,
                                                 SalesforceClientCredentialsClient.TokenResponse tokenResponse) {
        Map<String, String> mapProperty = new HashMap<>();
        String selectedObject = text(route.get("objectName"));
        String instanceUrl = firstNonBlank(tokenResponse.instanceUrl(), normalizeInstanceUrl(credential.getMyDomain()), text(route.get("myDomain")));
        String orgKey = firstNonBlank(credential.getOrgKey(), text(route.get("orgKey")));
        String orgName = firstNonBlank(credential.getOrgName(), text(route.get("orgName")));
        String targetSchema = firstNonBlank(text(route.get("targetSchema")), credential.getSchemaName(), "config");
        String targetTable = firstNonBlank(text(route.get("targetTable")), selectedObject);

        mapProperty.put("selectedObject", selectedObject);
        mapProperty.put("accessToken", tokenResponse.accessToken());
        mapProperty.put("clientId", credential.getClientId());
        mapProperty.put("clientSecret", credential.getClientSecret());
        mapProperty.put("instanceUrl", instanceUrl);
        mapProperty.put("orgKey", orgKey);
        mapProperty.put("orgName", orgName);
        mapProperty.put("targetSchema", targetSchema);
        mapProperty.put("targetTable", targetTable);
        mapProperty.put("instanceName", extractHost(instanceUrl));
        mapProperty.put("orgType", firstNonBlank(text(route.get("orgType")), "-"));
        mapProperty.put("isSandbox", String.valueOf(Boolean.parseBoolean(text(route.get("isSandbox")))));
        mapProperty.put("objectLabel", firstNonBlank(text(route.get("objectLabel")), selectedObject));
        mapProperty.put("actor", "routing-startup-recovery");
        return mapProperty;
    }

    private String normalizeCredentialDomain(String myDomain) {
        if (myDomain == null || myDomain.isBlank()) {
            return "";
        }
        String trimmed = myDomain.trim().replaceAll("/+$", "");
        try {
            if (trimmed.startsWith("http://") || trimmed.startsWith("https://")) {
                URI uri = URI.create(trimmed);
                return uri.getHost() == null ? trimmed : uri.getHost();
            }
        } catch (Exception ignore) {
            // fallthrough
        }
        return trimmed;
    }

    private String resolveTokenUrl(String myDomain, String tokenUrl) {
        String path = resolveSalesforcePath(tokenUrl);
        if (path.startsWith("http://") || path.startsWith("https://")) {
            return path;
        }
        return resolveSalesforceBaseUrl(myDomain) + path;
    }

    private String resolveSalesforcePath(String value) {
        String raw = value == null ? "" : value.trim();
        if (raw.isBlank()) {
            return "/services/oauth2/token";
        }
        if (raw.startsWith("http://") || raw.startsWith("https://")) {
            return raw;
        }
        return raw.startsWith("/") ? raw : "/" + raw;
    }

    private String resolveSalesforceBaseUrl(String myDomain) {
        String base = myDomain == null || myDomain.isBlank() ? null : myDomain.trim();
        String scheme = "https";
        String host;
        if (base != null) {
            try {
                if (base.startsWith("http://") || base.startsWith("https://")) {
                    URI uri = URI.create(base);
                    host = uri.getHost();
                    if (uri.getScheme() != null && !uri.getScheme().isBlank()) {
                        scheme = uri.getScheme();
                    }
                } else {
                    host = base;
                }
            } catch (Exception e) {
                host = base;
            }

            if (host != null && !host.isBlank()) {
                String normalizedHost = host.replaceAll("/+$", "").trim();
                return scheme + "://" + normalizedHost;
            }
        }
        return "https://login.salesforce.com";
    }

    private String normalizeLoginUrl(String myDomain) {
        if (myDomain == null || myDomain.isBlank()) {
            return "https://login.salesforce.com";
        }
        String lower = myDomain.toLowerCase(Locale.ROOT);
        if (lower.contains("test") || lower.contains("sandbox")) {
            return "https://test.salesforce.com";
        }
        return "https://login.salesforce.com";
    }

    private String normalizeInstanceUrl(String myDomain) {
        if (myDomain == null || myDomain.isBlank()) {
            return "";
        }
        if (myDomain.startsWith("http://") || myDomain.startsWith("https://")) {
            return myDomain.replaceAll("/+$", "");
        }
        return "https://" + myDomain.replaceAll("/+$", "");
    }

    private String extractHost(String url) {
        try {
            return URI.create(normalizeInstanceUrl(url)).getHost();
        } catch (Exception e) {
            return url;
        }
    }

    private String text(Object value) {
        return value == null ? "" : String.valueOf(value);
    }

    private String firstNonBlank(String... values) {
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value;
            }
        }
        return "";
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> castMap(Object value) {
        if (value instanceof Map<?, ?> map) {
            return (Map<String, Object>) map;
        }
        return new HashMap<>();
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
