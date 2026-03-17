package com.apache.sfdc.pubsub.controller;

import com.apache.sfdc.common.RoutingRegistrySupport;
import com.apache.sfdc.pubsub.service.PubSubService;
import com.etlplatform.common.validation.RequestValidationUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

@RestController
@RequiredArgsConstructor
@Slf4j
public class PubSubController {

    private final PubSubService pubSubService;
    private final RoutingRegistrySupport routingRegistrySupport;

    @GetMapping("/pubsub/slots/summary")
    public Map<String, Object> getCdcSlotSummary() {
        return pubSubService.getSlotSummary("CDC");
    }

    @PostMapping("/pubsub/drop")
    public ResponseEntity<Map<String, Object>> dropTable(@RequestParam("selectedObject") String selectedObject,
                                                       @RequestParam("targetSchema") String targetSchema) {
        String sanitizedObject = RequestValidationUtils.requireIdentifier(selectedObject, "selectedObject");
        String resolvedSchema = RequestValidationUtils.requireText(targetSchema, "targetSchema").trim();
        Map<String, Object> result = new LinkedHashMap<>();
        try {
            Map<String, String> mapProperty = new java.util.HashMap<>();
            mapProperty.put("selectedObject", sanitizedObject);
            mapProperty.put("targetSchema", resolvedSchema);
            pubSubService.dropTable(mapProperty);
            result.put("status", "SUCCESS");
            result.put("selectedObject", sanitizedObject);
            result.put("targetSchema", resolvedSchema);
            result.put("message", "CDC 테이블 삭제 완료");
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            result.put("status", "FAILED");
            result.put("selectedObject", sanitizedObject);
            result.put("targetSchema", resolvedSchema);
            result.put("message", "CDC 테이블 삭제 실패: " + (e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage()));
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(result);
        }
    }

    @PostMapping("/pubsub/slots/deactivate")
    public Map<String, Object> deactivateCdcSlot(@RequestParam("selectedObject") String selectedObject) {
        String sanitizedObject = RequestValidationUtils.requireIdentifier(selectedObject, "selectedObject");
        pubSubService.deactivateSlot(sanitizedObject, "CDC");
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("status", "SUCCESS");
        result.put("selectedObject", sanitizedObject);
        result.put("message", "CDC 슬롯 비활성화 완료");
        return result;
    }

    @PostMapping("/pubsub/credentials/refresh")
    public ResponseEntity<Map<String, Object>> refreshPubSubCredentials(@RequestBody String strJson) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, String> mapProperty = objectMapper.readValue(strJson, Map.class);
        String selectedObject = RequestValidationUtils.requireIdentifier(mapProperty.get("selectedObject"), "selectedObject");
        Map<String, Object> result = pubSubService.refreshCredentials(mapProperty);
        log.info("Refreshed pubsub credentials. selectedObject={}, status={}", selectedObject, result.get("status"));
        return ResponseEntity.ok(result);
    }

    @PostMapping("/pubsub")
    public ResponseEntity<Map<String, Object>> setCDC(@RequestBody String strJson) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, String> mapProperty = objectMapper.readValue(strJson, Map.class);
        String selectedObject = RequestValidationUtils.requireIdentifier(mapProperty.get("selectedObject"), "selectedObject");
        String token = RequestValidationUtils.requireText(mapProperty.get("accessToken"), "accessToken");
        String actor = mapProperty.getOrDefault("actor", "system");
        String orgKey = mapProperty.getOrDefault("orgKey", mapProperty.getOrDefault("instanceUrl", "default-org"));

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("status", "FAILED");
        result.put("selectedObject", selectedObject);
        result.put("message", "CDC 설정 중 문제가 발생했어요.");
        result.put("failureStage", "STARTING");
        result.put("failureDetail", "아직 실행 전입니다.");
        result.put("cdcCreationStatus", "NOT_STARTED");
        result.put("cdcCreationMessage", "CDC 생성 전입니다.");
        result.put("initialLoadCount", 0);
        result.put("subscribeStatus", "NOT_STARTED");
        result.put("slotRegistryStatus", "NOT_STARTED");

        log.info("Starting pubsub pipeline. selectedObject={}", selectedObject);
        result.put("failureStage", "CDC_CHANNEL");

        try {
            Map<String, Object> cdcResult = pubSubService.createCdcChannel(mapProperty, token);
            result.put("cdcCreationStatus", cdcResult.getOrDefault("status", "UNKNOWN"));
            result.put("cdcCreationMessage", cdcResult.getOrDefault("message", "CDC 생성 결과를 확인해주세요."));
            result.put("cdcResponse", cdcResult.get("responseBody"));
            log.info("Pubsub stage success. selectedObject={}, stage=CDC_CHANNEL, status={}", selectedObject, result.get("cdcCreationStatus"));

            Map<String, Object> mapReturn = pubSubService.setTable(mapProperty, token);
            result.put("failureStage", "INITIAL_LOAD");
            result.put("initialLoadCount", mapReturn.getOrDefault("initialLoadCount", 0));
            log.info("Pubsub stage success. selectedObject={}, stage=INITIAL_LOAD, initialLoadCount={}", selectedObject, result.get("initialLoadCount"));

            pubSubService.subscribeCDC(mapProperty, (Map<String, Object>) mapReturn.get("mapType"));
            result.put("failureStage", "SUBSCRIBE");
            result.put("subscribeStatus", "STARTED");
            log.info("Pubsub stage success. selectedObject={}, stage=SUBSCRIBE", selectedObject);

            pubSubService.markSlotActive(selectedObject, "CDC", orgKey, null);
            result.put("slotRegistryStatus", "ACTIVE");
            log.info("Pubsub stage success. selectedObject={}, stage=SLOT_REGISTRY", selectedObject);

            Map<String, Object> registry = routingRegistrySupport.buildRouteMetadata(
                    mapProperty,
                    "CDC",
                    "/pubsub",
                    "ACTIVE",
                    "ACTIVE",
                    ((Number) result.get("initialLoadCount")).intValue(),
                    null,
                    actor
            );
            routingRegistrySupport.upsertRegistry(registry);
            routingRegistrySupport.insertHistory(orgKey, selectedObject, "CDC", "REGISTER", "SUCCESS", "COMPLETE", "/pubsub",
                    "CDC 설정이 완료되었어요.", String.valueOf(result.getOrDefault("cdcResponse", "")), ((Number) result.get("initialLoadCount")).intValue(), actor);

            result.put("status", "SUCCESS");
            result.put("message", "CDC 설정이 완료되었어요.");
            result.put("failureStage", "-");
            result.put("failureDetail", "-");
            log.info("Completed pubsub pipeline setup. selectedObject={}", selectedObject);
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            String stage = String.valueOf(result.getOrDefault("failureStage", "UNKNOWN"));
            String failureMessage = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
            Map<String, Object> registry = routingRegistrySupport.buildRouteMetadata(
                    mapProperty,
                    "CDC",
                    "/pubsub",
                    "FAILED",
                    "ERROR",
                    0,
                    failureMessage,
                    actor
            );
            routingRegistrySupport.upsertRegistry(registry);
            routingRegistrySupport.markFailed(orgKey, selectedObject, "CDC", failureMessage, actor);
            routingRegistrySupport.insertHistory(orgKey, selectedObject, "CDC", "REGISTER", "FAILED", stage, "/pubsub",
                    "CDC 설정 실패", failureMessage, 0, actor);
            result.put("status", "FAILED");
            result.put("message", "CDC 설정 중 문제가 발생했어요.");
            result.put("failureDetail", failureMessage);
            log.error("Pubsub stage failed. selectedObject={}, stage={}, message={}", selectedObject, stage, result.get("failureDetail"), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(result);
        }
    }
}
