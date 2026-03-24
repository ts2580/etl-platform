package com.apache.sfdc.common;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

@Component
@RequiredArgsConstructor
@Slf4j
public class RoutingRegistrySupport {

    private final ObjectProvider<RoutingRegistryRepository> routingRegistryRepositoryProvider;

    public Map<String, Object> buildRouteMetadata(Map<String, String> mapProperty,
                                                  String routingProtocol,
                                                  String routingEndpoint,
                                                  String routingStatus,
                                                  String sourceStatus,
                                                  int initialLoadCount,
                                                  String lastErrorMessage,
                                                  String actor) {
        String instanceUrl = mapProperty.getOrDefault("instanceUrl", "");
        String orgKey = normalizeOrgKey(firstNonBlank(mapProperty.get("orgKey"), instanceUrl, "default-org"));
        String orgName = firstNonBlank(mapProperty.get("orgName"), extractHost(instanceUrl), orgKey);
        String selectedObject = mapProperty.getOrDefault("selectedObject", "");
        String objectLabel = firstNonBlank(mapProperty.get("objectLabel"), selectedObject);
        String targetStorageId = mapProperty.get("targetStorageId");
        String targetStorageName = mapProperty.get("targetStorageName");
        String targetSchema = firstNonBlank(mapProperty.get("targetSchema"), "config");
        String targetTable = firstNonBlank(mapProperty.get("targetTable"), selectedObject);

        Map<String, Object> params = new HashMap<>();
        params.put("orgKey", orgKey);
        params.put("orgName", orgName);
        params.put("myDomain", instanceUrl);
        params.put("targetStorageId", targetStorageId == null || targetStorageId.isBlank() ? null : Long.valueOf(targetStorageId));
        params.put("targetStorageName", firstNonBlank(targetStorageName, null));
        params.put("targetSchema", targetSchema);
        params.put("targetTable", targetTable);
        params.put("instanceName", firstNonBlank(mapProperty.get("instanceName"), extractHost(instanceUrl)));
        params.put("orgType", firstNonBlank(mapProperty.get("orgType"), "-"));
        params.put("sandbox", Boolean.parseBoolean(String.valueOf(mapProperty.getOrDefault("isSandbox", "false"))));
        params.put("selectedObject", selectedObject);
        params.put("objectLabel", objectLabel);
        params.put("routingProtocol", routingProtocol);
        params.put("routingEndpoint", routingEndpoint);
        params.put("routingStatus", routingStatus);
        params.put("sourceStatus", sourceStatus);
        params.put("initialLoadCount", initialLoadCount);
        params.put("lastErrorMessage", lastErrorMessage);
        params.put("activatedAt", null);
        params.put("releasedAt", null);
        params.put("lastSyncedAt", null);
        params.put("createdBy", actor);
        params.put("updatedBy", actor);
        return params;
    }

    public void upsertRegistry(Map<String, Object> params) {
        RoutingRegistryRepository repository = routingRegistryRepositoryProvider.getIfAvailable();
        if (repository == null) {
            log.debug("RoutingRegistryRepository unavailable. Skipping upsertRegistry. params={}", params);
            return;
        }
        repository.upsertRoutingRegistry(params);
    }

    public void insertHistory(String orgKey,
                              String selectedObject,
                              String routingProtocol,
                              String eventType,
                              String eventStatus,
                              String eventStage,
                              String endpoint,
                              String message,
                              String detailText,
                              int initialLoadCount,
                              String actor) {
        RoutingRegistryRepository repository = routingRegistryRepositoryProvider.getIfAvailable();
        if (repository == null) {
            log.debug("RoutingRegistryRepository unavailable. Skipping insertHistory. orgKey={}, selectedObject={}, routingProtocol={}", orgKey, selectedObject, routingProtocol);
            return;
        }

        Map<String, Object> params = new HashMap<>();
        params.put("routingRegistryId", null);
        params.put("orgKey", orgKey);
        params.put("selectedObject", selectedObject);
        params.put("routingProtocol", routingProtocol);
        params.put("eventType", eventType);
        params.put("eventStatus", eventStatus);
        params.put("eventStage", eventStage);
        params.put("endpoint", endpoint);
        params.put("message", message);
        params.put("detailText", detailText);
        params.put("initialLoadCount", initialLoadCount);
        params.put("actor", actor);
        repository.insertRoutingHistory(params);
    }

    public void markReleased(String orgKey, String selectedObject, String routingProtocol, String message, String actor) {
        RoutingRegistryRepository repository = routingRegistryRepositoryProvider.getIfAvailable();
        if (repository == null) {
            log.debug("RoutingRegistryRepository unavailable. Skipping markReleased. orgKey={}, selectedObject={}, routingProtocol={}", orgKey, selectedObject, routingProtocol);
            return;
        }
        repository.markRoutingReleased(orgKey, selectedObject, routingProtocol, message, actor);
    }

    public void markFailed(String orgKey, String selectedObject, String routingProtocol, String message, String actor) {
        RoutingRegistryRepository repository = routingRegistryRepositoryProvider.getIfAvailable();
        if (repository == null) {
            log.debug("RoutingRegistryRepository unavailable. Skipping markFailed. orgKey={}, selectedObject={}, routingProtocol={}", orgKey, selectedObject, routingProtocol);
            return;
        }
        repository.markRoutingFailed(orgKey, selectedObject, routingProtocol, message, actor);
    }

    private String normalizeOrgKey(String value) {
        String normalized = RoutingRuntimeKeyUtils.normalizeOrgKey(value);
        return normalized.isBlank() ? "default-org" : normalized;
    }

    private String extractHost(String url) {
        if (url == null || url.isBlank()) {
            return "-";
        }
        try {
            return URI.create(url).getHost();
        } catch (Exception e) {
            return url;
        }
    }

    private String firstNonBlank(String... values) {
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value;
            }
        }
        return "-";
    }
}
