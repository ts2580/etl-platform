package com.apache.sfdc.health.controller;

import com.etlplatform.common.health.HealthStatusResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;

@RestController
@RequestMapping("/health")
public class HealthController {

    @Value("${app.db.enabled:false}")
    private boolean dbEnabled;

    @Value("${camel.component.salesforce.client-id:}")
    private String clientId;

    @GetMapping
    public ResponseEntity<HealthStatusResponse> health() {
        boolean salesforceConfigured = !clientId.isBlank();
        return ResponseEntity.ok(new HealthStatusResponse("etl-routing-engine", "UP", dbEnabled, salesforceConfigured, Instant.now()));
    }
}
