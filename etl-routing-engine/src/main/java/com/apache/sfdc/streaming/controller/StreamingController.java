package com.apache.sfdc.streaming.controller;

import com.apache.sfdc.common.RoutingRegistrySupport;
import com.apache.sfdc.streaming.service.StreamingService;
import com.etlplatform.common.validation.RequestValidationUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

@RestController
@RequiredArgsConstructor
@Slf4j
public class StreamingController {
    private final StreamingService routerService;
    private final RoutingRegistrySupport routingRegistrySupport;

    @PostMapping("/streaming/runtime/stop")
    public Map<String, Object> stopStreamingRuntime(@RequestParam("orgKey") String orgKey,
                                                    @RequestParam("selectedObject") String selectedObject,
                                                    @RequestParam(value = "reason", required = false) String reason) {
        String sanitizedObject = RequestValidationUtils.requireIdentifier(selectedObject, "selectedObject");
        String sanitizedOrgKey = RequestValidationUtils.requireText(orgKey, "orgKey").trim();
        routerService.stopRoute(sanitizedOrgKey, sanitizedObject, reason == null ? "manual stop" : reason);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("status", "SUCCESS");
        result.put("selectedObject", sanitizedObject);
        result.put("message", "Streaming 런타임 중지 완료");
        return result;
    }

    @PostMapping("/streaming/drop")
    public ResponseEntity<Map<String, Object>> dropTable(@RequestParam("selectedObject") String selectedObject,
                                                         @RequestParam("targetSchema") String targetSchema,
                                                         @RequestParam(value = "orgName", required = false) String orgName,
                                                         @RequestParam(value = "targetStorageId", required = false) Long targetStorageId) {
        String sanitizedObject = RequestValidationUtils.requireIdentifier(selectedObject, "selectedObject");
        String resolvedSchema = RequestValidationUtils.requireText(targetSchema, "targetSchema").trim();

        Map<String, Object> result = new LinkedHashMap<>();
        try {
            Map<String, String> mapProperty = new java.util.HashMap<>();
            mapProperty.put("selectedObject", sanitizedObject);
            mapProperty.put("targetSchema", resolvedSchema);
            if (orgName != null && !orgName.isBlank()) {
                mapProperty.put("orgName", orgName.trim());
            }
            if (targetStorageId != null) {
                mapProperty.put("targetStorageId", String.valueOf(targetStorageId));
            }
            routerService.dropTable(mapProperty);
            result.put("status", "SUCCESS");
            result.put("selectedObject", sanitizedObject);
            result.put("targetSchema", resolvedSchema);
            result.put("message", "Streaming 테이블 삭제 완료");
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            result.put("status", "FAILED");
            result.put("selectedObject", sanitizedObject);
            result.put("targetSchema", resolvedSchema);
            result.put("message", "Streaming 테이블 삭제 실패: " + (e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage()));
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(result);
        }
    }

    @PostMapping("/streaming/credentials/refresh")
    public ResponseEntity<Map<String, Object>> refreshStreamingCredentials(@RequestBody String strJson) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, String> mapProperty = objectMapper.readValue(strJson, Map.class);
        String selectedObject = RequestValidationUtils.requireIdentifier(mapProperty.get("selectedObject"), "selectedObject");
        Map<String, Object> result = routerService.refreshCredentials(mapProperty);
        log.info("Refreshed streaming credentials. selectedObject={}, status={}", selectedObject, result.get("status"));
        return ResponseEntity.ok(result);
    }

    @PostMapping("/streaming")
    public ResponseEntity<Map<String, Object>> setPushTopic(@RequestBody String strJson) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, String> mapProperty = objectMapper.readValue(strJson, Map.class);
        String selectedObject = RequestValidationUtils.requireIdentifier(mapProperty.get("selectedObject"), "selectedObject");
        String token = RequestValidationUtils.requireText(mapProperty.get("accessToken"), "accessToken");
        String actor = mapProperty.getOrDefault("actor", "system");
        String orgKey = mapProperty.getOrDefault("orgKey", mapProperty.getOrDefault("instanceUrl", "default-org"));

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("status", "FAILED");
        result.put("selectedObject", selectedObject);
        result.put("message", "Streaming 설정 중 문제가 발생했어요.");
        result.put("pushTopicStatus", "NOT_STARTED");
        result.put("initialLoadCount", 0);
        result.put("subscribeStatus", "NOT_STARTED");
        result.put("failureStage", "STARTING");

        log.info("Starting streaming pipeline. selectedObject={}", selectedObject);
        try {
            Map<String, Object> mapReturn = routerService.setTable(mapProperty, token);
            result.put("failureStage", "PUSH_TOPIC");
            String pushTopicResult = routerService.setPushTopic(mapProperty, mapReturn, token);
            result.put("pushTopicStatus", "CREATED");
            result.put("initialLoadCount", mapReturn.getOrDefault("initialLoadCount", 0));
            routerService.subscribePushTopic(mapProperty, token, (Map<String, Object>) mapReturn.get("mapType"));
            result.put("subscribeStatus", "STARTED");

            Map<String, Object> registry = routingRegistrySupport.buildRouteMetadata(
                    mapProperty,
                    "STREAMING",
                    "/streaming",
                    "ACTIVE",
                    "ACTIVE",
                    ((Number) result.get("initialLoadCount")).intValue(),
                    null,
                    actor
            );
            routingRegistrySupport.upsertRegistry(registry);
            routingRegistrySupport.insertHistory(orgKey, selectedObject, "STREAMING", "REGISTER", "SUCCESS", "COMPLETE", "/streaming",
                    "Streaming 설정이 완료되었어요.", pushTopicResult, ((Number) result.get("initialLoadCount")).intValue(), actor);

            log.info("Completed streaming pipeline setup. selectedObject={}", selectedObject);
            result.put("status", "SUCCESS");
            result.put("message", "Streaming 설정이 완료되었어요.");
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            String failureMessage = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
            Map<String, Object> registry = routingRegistrySupport.buildRouteMetadata(
                    mapProperty,
                    "STREAMING",
                    "/streaming",
                    "FAILED",
                    "ERROR",
                    0,
                    failureMessage,
                    actor
            );
            routingRegistrySupport.upsertRegistry(registry);
            routingRegistrySupport.markFailed(orgKey, selectedObject, "STREAMING", failureMessage, actor);
            routingRegistrySupport.insertHistory(orgKey, selectedObject, "STREAMING", "REGISTER", "FAILED",
                    String.valueOf(result.getOrDefault("failureStage", "UNKNOWN")), "/streaming", "Streaming 설정 실패", failureMessage, 0, actor);
            result.put("failureDetail", failureMessage);
            log.error("Streaming stage failed. selectedObject={}, message={}", selectedObject, failureMessage, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(result);
        }
    }
}
