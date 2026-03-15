package com.apache.sfdc.streaming.controller;

import com.apache.sfdc.streaming.service.StreamingService;
import com.etlplatform.common.validation.RequestValidationUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequiredArgsConstructor
@Slf4j
public class StreamingController {
    private final StreamingService routerService;

    @PostMapping("/streaming")
    public Map<String, Object> setPushTopic(@RequestBody String strJson) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, String> mapProperty = objectMapper.readValue(strJson, Map.class);
        String selectedObject = RequestValidationUtils.requireIdentifier(mapProperty.get("selectedObject"), "selectedObject");
        String token = RequestValidationUtils.requireText(mapProperty.get("accessToken"), "accessToken");

        log.info("Starting streaming pipeline. selectedObject={}", selectedObject);
        Map<String, Object> mapReturn = routerService.setTable(mapProperty, token);
        String pushTopicResult = routerService.setPushTopic(mapProperty, mapReturn, token);
        routerService.subscribePushTopic(mapProperty, token, (Map<String, Object>) mapReturn.get("mapType"));
        log.info("Completed streaming pipeline setup. selectedObject={}", selectedObject);

        Map<String, Object> result = new java.util.LinkedHashMap<>();
        result.put("status", "SUCCESS");
        result.put("selectedObject", selectedObject);
        result.put("message", "Streaming 설정이 완료되었어요.");
        result.put("pushTopicStatus", "CREATED");
        result.put("initialLoadCount", mapReturn.getOrDefault("initialLoadCount", 0));
        result.put("subscribeStatus", "STARTED");
        result.put("pushTopicResponse", pushTopicResult);
        return result;
    }
}
