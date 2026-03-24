package com.sfdcupload.storage.controller;

import com.sfdcupload.storage.dto.DatabaseConnectionTestResponse;
import com.sfdcupload.storage.dto.DatabaseStorageRegistrationRequest;
import com.sfdcupload.storage.dto.DatabaseStorageRegistrationResponse;
import com.sfdcupload.storage.service.DatabaseConnectionTestService;
import com.sfdcupload.storage.service.DatabaseStorageRegistrationService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
@ConditionalOnProperty(name = "app.db.enabled", havingValue = "true")
@RequestMapping("/api/storages/databases")
public class DatabaseStorageController {

    private final DatabaseStorageRegistrationService registrationService;
    private final DatabaseConnectionTestService connectionTestService;

    @PostMapping("/test")
    public ResponseEntity<DatabaseConnectionTestResponse> test(@Valid @RequestBody DatabaseStorageRegistrationRequest request) {
        return ResponseEntity.ok(connectionTestService.test(request));
    }

    @PostMapping
    public ResponseEntity<DatabaseStorageRegistrationResponse> register(@Valid @RequestBody DatabaseStorageRegistrationRequest request) {
        return ResponseEntity.ok(registrationService.register(request));
    }
}
