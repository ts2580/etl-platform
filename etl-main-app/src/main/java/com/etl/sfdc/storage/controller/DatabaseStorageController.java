package com.etl.sfdc.storage.controller;

import com.etl.sfdc.storage.dto.DatabaseConnectionTestResponse;
import com.etl.sfdc.storage.dto.DatabaseStorageDetailResponse;
import com.etl.sfdc.storage.dto.DatabaseStorageListPageResponse;
import com.etl.sfdc.storage.dto.DatabaseStorageRegistrationRequest;
import com.etl.sfdc.storage.dto.DatabaseStorageRegistrationResponse;
import com.etl.sfdc.storage.dto.DatabaseStoredCredentialDecryptRequest;
import com.etl.sfdc.storage.dto.DatabaseStoredCredentialDecryptResponse;
import com.etl.sfdc.storage.service.DatabaseConnectionTestService;
import com.etl.sfdc.storage.service.DatabaseStorageManagementService;
import com.etl.sfdc.storage.service.DatabaseStorageQueryService;
import com.etl.sfdc.storage.service.DatabaseStorageRegistrationService;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/storages/databases")
public class DatabaseStorageController {

    private final DatabaseStorageRegistrationService registrationService;
    private final DatabaseConnectionTestService connectionTestService;
    private final DatabaseStorageQueryService queryService;
    private final DatabaseStorageManagementService managementService;

    @PostMapping("/test")
    public ResponseEntity<DatabaseConnectionTestResponse> test(@Valid @RequestBody DatabaseStorageRegistrationRequest request) {
        return ResponseEntity.ok(connectionTestService.test(request));
    }

    @GetMapping
    public ResponseEntity<DatabaseStorageListPageResponse> list(
            @RequestParam(value = "page", defaultValue = "0") int page,
            @RequestParam(value = "size", defaultValue = "20") int size,
            @RequestParam(value = "sortBy", defaultValue = "id") String sortBy,
            @RequestParam(value = "sortDirection", defaultValue = "asc") String sortDirection) {
        return ResponseEntity.ok(queryService.getList(page, size, sortBy, sortDirection));
    }

    @GetMapping("/{storageId}")
    public ResponseEntity<DatabaseStorageDetailResponse> detail(@PathVariable("storageId") Long storageId) {
        return ResponseEntity.ok(queryService.getDetail(storageId));
    }

    @PostMapping("/{storageId}/decrypt")
    public ResponseEntity<DatabaseStoredCredentialDecryptResponse> decrypt(@PathVariable("storageId") Long storageId,
                                                                          @RequestBody DatabaseStoredCredentialDecryptRequest request,
                                                                          HttpServletRequest httpRequest) {
        String requesterIp = resolveRequester(httpRequest);
        return ResponseEntity.ok(queryService.decryptStoredCredentials(storageId, request, requesterIp));
    }

    @PostMapping("/{storageId}/revalidate")
    public ResponseEntity<DatabaseConnectionTestResponse> revalidate(@PathVariable("storageId") Long storageId) {
        return ResponseEntity.ok(managementService.revalidate(storageId));
    }

    @PutMapping("/{storageId}")
    public ResponseEntity<DatabaseStorageDetailResponse> update(@PathVariable("storageId") Long storageId,
                                                                @Valid @RequestBody DatabaseStorageRegistrationRequest request) {
        return ResponseEntity.ok(managementService.update(storageId, request));
    }

    @DeleteMapping("/{storageId}")
    public ResponseEntity<Void> delete(@PathVariable("storageId") Long storageId) {
        managementService.delete(storageId);
        return ResponseEntity.noContent().build();
    }

    @PostMapping
    public ResponseEntity<DatabaseStorageRegistrationResponse> register(@Valid @RequestBody DatabaseStorageRegistrationRequest request) {
        return ResponseEntity.ok(registrationService.register(request));
    }

    private String resolveRequester(HttpServletRequest request) {
        String xff = request.getHeader("X-Forwarded-For");
        if (xff != null && !xff.isBlank()) {
            return xff.split(",")[0].trim();
        }
        return request.getRemoteAddr();
    }
}
