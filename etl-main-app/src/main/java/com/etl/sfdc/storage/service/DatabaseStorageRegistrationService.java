package com.etl.sfdc.storage.service;

import com.etl.sfdc.storage.dto.DatabaseStorageRegistrationRequest;
import com.etl.sfdc.storage.dto.DatabaseStorageRegistrationResponse;
import com.etl.sfdc.storage.model.dto.ExternalDatabaseStorage;
import com.etl.sfdc.storage.model.dto.ExternalStorage;
import com.etl.sfdc.storage.model.repository.ExternalDatabaseStorageRepository;
import com.etl.sfdc.storage.model.repository.ExternalStorageConnectionHistoryRepository;
import com.etl.sfdc.storage.model.repository.ExternalStorageRepository;
import com.etlplatform.common.storage.database.DatabaseStorageRegistrationDraft;
import com.etlplatform.common.storage.database.DatabaseStorageRegistrationSupport;
import com.etlplatform.common.storage.database.DatabaseVendor;
import com.etlplatform.common.storage.database.StorageType;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Locale;

@Service
@RequiredArgsConstructor
@Slf4j
public class DatabaseStorageRegistrationService {

    private final ExternalStorageRepository externalStorageRepository;
    private final ExternalDatabaseStorageRepository externalDatabaseStorageRepository;
    private final ExternalStorageConnectionHistoryRepository historyRepository;
    private final DatabaseConnectionTestService connectionTestService;
    private final DatabaseStorageRegistrationSupport registrationSupport = new DatabaseStorageRegistrationSupport();

    @Transactional
    public DatabaseStorageRegistrationResponse register(DatabaseStorageRegistrationRequest request) {
        String start = String.format("[DB 저장소 등록] 시작. name=%s, vendor=%s, authMethod=%s, url=%s, port=%s",
                request.getName(), request.getVendor(), request.getAuthMethod(), request.getJdbcUrl(), request.getPort());
        log.info(start);
        System.out.println(start);
        try {
            normalizeRoutingSchemaName(request);
            connectionTestService.validate(request);
            DatabaseStorageRegistrationDraft draft = registrationSupport.buildDraft(request);

            ExternalStorage storage = new ExternalStorage();
            storage.setStorageType(StorageType.DATABASE);
            storage.setName(request.getName());
            storage.setDescription(request.getDescription());
            storage.setEnabled(Boolean.TRUE);
            storage.setConnectionStatus("PENDING");
            externalStorageRepository.insert(storage);

            ExternalDatabaseStorage databaseStorage = new ExternalDatabaseStorage();
            databaseStorage.setStorageId(storage.getId());
            databaseStorage.setVendor(draft.vendor());
            databaseStorage.setJdbcUrl(draft.jdbcUrl());
            databaseStorage.setAuthMethod(draft.authMethod());
            databaseStorage.setHost(draft.host());
            databaseStorage.setPort(draft.port());
            databaseStorage.setDatabaseName(draft.databaseName());
            databaseStorage.setServiceName(draft.serviceName());
            databaseStorage.setSid(draft.sid());
            databaseStorage.setUsername(draft.username());
            databaseStorage.setPasswordEncrypted(draft.passwordEncrypted());
            databaseStorage.setSchemaName(draft.schemaName());
            databaseStorage.setCredentialMetaJson(draft.credentialMetaJson());
            externalDatabaseStorageRepository.insert(databaseStorage);

            historyRepository.insert(storage.getId(), "REGISTER", true, "DB 저장소 등록 완료", request.getAuthMethod() + " 방식으로 저장소가 등록됐어요.");

            String success = String.format("[DB 저장소 등록] 완료. storageId=%s", storage.getId());
            log.info(success);
            System.out.println(success);
            return DatabaseStorageRegistrationResponse.builder()
                    .id(storage.getId())
                    .name(storage.getName())
                    .storageType(storage.getStorageType())
                    .vendor(databaseStorage.getVendor())
                    .authMethod(databaseStorage.getAuthMethod())
                    .jdbcUrl(databaseStorage.getJdbcUrl())
                    .username(databaseStorage.getUsername())
                    .connectionStatus(storage.getConnectionStatus())
                    .enabled(Boolean.TRUE.equals(storage.getEnabled()))
                    .build();
        } catch (RuntimeException e) {
            String err = String.format("[DB 저장소 등록] 실패. name=%s, vendor=%s, url=%s, port=%s, reason=%s",
                    request.getName(), request.getVendor(), request.getJdbcUrl(), request.getPort(), e.getMessage());
            log.error("[DB 저장소 등록] 실패. name={}, vendor={}, url={}, port={}",
                    request.getName(), request.getVendor(), request.getJdbcUrl(), request.getPort(), e);
            System.err.println(err);
            e.printStackTrace(System.err);
            throw e;
        }
    }

    private void normalizeRoutingSchemaName(DatabaseStorageRegistrationRequest request) {
        if (request == null || request.getVendor() == null) {
            return;
        }
        if (!(request.getVendor() == DatabaseVendor.MARIADB
                || request.getVendor() == DatabaseVendor.MYSQL
                || request.getVendor() == DatabaseVendor.POSTGRESQL)) {
            return;
        }
        if (request.getVendor() == DatabaseVendor.ORACLE) {
            request.setSchemaName(null);
            return;
        }
        request.setDatabaseName("etl_sfdc");
        String schemaName = request.getSchemaName();
        if (schemaName == null || schemaName.isBlank()) {
            return;
        }
        String normalized = schemaName.trim().toLowerCase(Locale.ROOT)
                .replaceAll("[\\s-]+", "_")
                .replaceAll("[^a-z0-9_]", "");
        if (!normalized.startsWith("org_")) {
            normalized = "org_" + normalized;
        }
        normalized = normalized.replaceAll("_+", "_");
        if (normalized.endsWith("_")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        request.setSchemaName(normalized);
    }
}
