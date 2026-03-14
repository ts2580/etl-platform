package com.apache.sfdc.pubsub.controller;

import com.apache.sfdc.common.SalesforceOAuth;
import com.apache.sfdc.pubsub.service.PubSubService;
import com.etlplatform.common.validation.RequestValidationUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequiredArgsConstructor
@Slf4j
public class PubSubController {

    private final PubSubService pubSubService;
    private final SalesforceOAuth salesforceOAuth;

    @PostMapping("/pubsub")
    public String setCDC(@RequestBody String strJson) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, String> mapProperty = objectMapper.readValue(strJson, Map.class);
        String selectedObject = RequestValidationUtils.requireIdentifier(mapProperty.get("selectedObject"), "selectedObject");

        log.info("Starting pubsub pipeline. selectedObject={}", selectedObject);
        String token = salesforceOAuth.getAccessToken(mapProperty);
        Map<String, Object> mapReturn = pubSubService.setTable(mapProperty, token);
        pubSubService.subscribeCDC(mapProperty, (Map<String, Object>) mapReturn.get("mapType"));
        log.info("Completed pubsub pipeline setup. selectedObject={}", selectedObject);
        return "모든 시퀸스 성공";
    }
}
