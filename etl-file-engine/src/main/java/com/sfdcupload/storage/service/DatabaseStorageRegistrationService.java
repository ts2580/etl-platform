package com.sfdcupload.storage.service;

import com.etlplatform.common.storage.database.DatabaseStorageRegistrationDraft;
import com.etlplatform.common.storage.database.DatabaseStorageRegistrationSupport;
import com.etlplatform.common.storage.database.StorageType;
import com.sfdcupload.storage.dto.DatabaseStorageRegistrationRequest;
import com.sfdcupload.storage.dto.DatabaseStorageRegistrationResponse;
import com.sfdcupload.storage.model.dto.ExternalDatabaseStorage;
import com.sfdcupload.storage.model.dto.ExternalStorage;
import com.sfdcupload.storage.model.repository.ExternalDatabaseStorageRepository;
import com.sfdcupload.storage.model.repository.ExternalStorageConnectionHistoryRepository;
import com.sfdcupload.storage.model.repository.ExternalStorageRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
@ConditionalOnProperty(name = "app.db.enabled", havingValue = "true")
public class DatabaseStorageRegistrationService {

    private final ExternalStorageRepository externalStorageRepository;
    private final ExternalDatabaseStorageRepository externalDatabaseStorageRepository;
    private final ExternalStorageConnectionHistoryRepository historyRepository;
    private final DatabaseConnectionTestService connectionTestService;
    private final DatabaseStorageRegistrationSupport registrationSupport = new DatabaseStorageRegistrationSupport();

    @Transactional
    public DatabaseStorageRegistrationResponse register(DatabaseStorageRegistrationRequest request) {
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
        databaseStorage.setDatabaseName(null);
        databaseStorage.setServiceName(null);
        databaseStorage.setSid(null);
        databaseStorage.setUsername(draft.username());
        databaseStorage.setPasswordEncrypted(draft.passwordEncrypted());
        databaseStorage.setSchemaName(draft.schemaName());
        databaseStorage.setCredentialMetaJson(draft.credentialMetaJson());
        externalDatabaseStorageRepository.insert(databaseStorage);

        historyRepository.insert(storage.getId(), "REGISTER", true, "DB 저장소 등록 완료", request.getAuthMethod() + " 방식으로 저장소가 등록됐어요.");

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
    }
}
