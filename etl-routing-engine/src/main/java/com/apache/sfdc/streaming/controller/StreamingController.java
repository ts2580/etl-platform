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
    public String setPushTopic(@RequestBody String strJson) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, String> mapProperty = objectMapper.readValue(strJson, Map.class);
        String selectedObject = RequestValidationUtils.requireIdentifier(mapProperty.get("selectedObject"), "selectedObject");
        String token = RequestValidationUtils.requireText(mapProperty.get("accessToken"), "accessToken");

        log.info("Starting streaming pipeline. selectedObject={}", selectedObject);
        Map<String, Object> mapReturn = routerService.setTable(mapProperty, token);
        routerService.setPushTopic(mapProperty, mapReturn, token);
        routerService.subscribePushTopic(mapProperty, token, (Map<String, Object>) mapReturn.get("mapType"));
        log.info("Completed streaming pipeline setup. selectedObject={}", selectedObject);
        return "모든 시퀸스 성공";
    }
}
