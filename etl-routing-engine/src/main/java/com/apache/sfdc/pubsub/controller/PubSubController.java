package com.apache.sfdc.pubsub.controller;

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
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

@RestController
@RequiredArgsConstructor
@Slf4j
public class PubSubController {

    private final PubSubService pubSubService;

    @GetMapping("/pubsub/slots/summary")
    public Map<String, Object> getCdcSlotSummary() {
        return pubSubService.getCdcSlotSummary();
    }

    @PostMapping("/pubsub")
    public ResponseEntity<Map<String, Object>> setCDC(@RequestBody String strJson) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, String> mapProperty = objectMapper.readValue(strJson, Map.class);
        String selectedObject = RequestValidationUtils.requireIdentifier(mapProperty.get("selectedObject"), "selectedObject");
        String token = RequestValidationUtils.requireText(mapProperty.get("accessToken"), "accessToken");

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

            pubSubService.markCdcSlotActive(selectedObject);
            result.put("slotRegistryStatus", "ACTIVE");
            log.info("Pubsub stage success. selectedObject={}, stage=SLOT_REGISTRY", selectedObject);

            result.put("status", "SUCCESS");
            result.put("message", "CDC 설정이 완료되었어요.");
            result.put("failureStage", "-");
            result.put("failureDetail", "-");
            log.info("Completed pubsub pipeline setup. selectedObject={}", selectedObject);
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            String stage = String.valueOf(result.getOrDefault("failureStage", "UNKNOWN"));
            result.put("status", "FAILED");
            result.put("message", "CDC 설정 중 문제가 발생했어요.");
            result.put("failureDetail", e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage());
            log.error("Pubsub stage failed. selectedObject={}, stage={}, message={}", selectedObject, stage, result.get("failureDetail"), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(result);
        }
    }
}
