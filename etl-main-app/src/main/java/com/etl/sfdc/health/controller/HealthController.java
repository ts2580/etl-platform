package com.etl.sfdc.health.controller;

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

    @Value("${salesforce.redirectUri:}")
    private String redirectUri;

    @GetMapping
    public ResponseEntity<HealthStatusResponse> health() {
        boolean salesforceConfigured = !redirectUri.isBlank();
        return ResponseEntity.ok(new HealthStatusResponse("etl-main-app", "UP", dbEnabled, salesforceConfigured, Instant.now()));
    }
}
