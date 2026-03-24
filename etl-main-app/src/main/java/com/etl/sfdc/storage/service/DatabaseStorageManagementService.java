package com.etl.sfdc.storage.service;

import com.etl.sfdc.storage.dto.DatabaseConnectionTestResponse;
import com.etl.sfdc.storage.dto.DatabaseStorageDetailResponse;
import com.etl.sfdc.storage.dto.DatabaseStorageRegistrationRequest;
import com.etl.sfdc.storage.model.dto.ExternalDatabaseStorage;
import com.etl.sfdc.storage.model.repository.ExternalDatabaseStorageRepository;
import com.etl.sfdc.storage.model.repository.ExternalStorageConnectionHistoryRepository;
import com.etl.sfdc.storage.model.repository.ExternalStorageRepository;
import com.etl.sfdc.storage.support.DatabaseCredentialEncryptor;
import com.etlplatform.common.error.AppException;
import com.etlplatform.common.storage.database.DatabaseAuthMethod;
import com.etlplatform.common.storage.database.DatabaseCertificateAuthRequest;
import com.etlplatform.common.storage.database.DatabaseConnectionTestResult;
import com.etlplatform.common.storage.database.DatabaseConnectionTestSupport;
import com.etlplatform.common.storage.database.DatabaseStorageRegistrationDraft;
import com.etlplatform.common.storage.database.DatabaseStorageRegistrationSupport;
import com.etlplatform.common.storage.database.DatabaseVendor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Map;

@Service
@RequiredArgsConstructor
@Slf4j
public class DatabaseStorageManagementService {

    private final ExternalStorageRepository externalStorageRepository;
    private final ExternalDatabaseStorageRepository externalDatabaseStorageRepository;
    private final ExternalStorageConnectionHistoryRepository historyRepository;
    private final DatabaseCredentialEncryptor credentialEncryptor;
    private final DatabaseCertificateMetaService certificateMetaService;
    private final DatabaseStorageQueryService queryService;

    private final DatabaseStorageRegistrationSupport registrationSupport = new DatabaseStorageRegistrationSupport();
    private final DatabaseConnectionTestSupport connectionTestSupport = new DatabaseConnectionTestSupport();

    @Transactional
    public DatabaseStorageDetailResponse update(Long storageId, DatabaseStorageRegistrationRequest request) {
        Map<String, Object> detailMap = requireDetail(storageId);
        DatabaseStorageRegistrationRequest mergedRequest = mergeWithStoredCredentials(detailMap, request);
        DatabaseStorageRegistrationDraft draft = registrationSupport.buildDraft(mergedRequest);

        externalStorageRepository.updateEditableFields(
                storageId,
                mergedRequest.getName(),
                normalizeBlankToNull(mergedRequest.getDescription()),
                "PENDING",
                null
        );

        ExternalDatabaseStorage databaseStorage = new ExternalDatabaseStorage();
        databaseStorage.setStorageId(storageId);
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
        databaseStorage.setJdbcOptionsJson(asString(detailMap.get("jdbcOptionsJson")));
        databaseStorage.setCredentialMetaJson(draft.credentialMetaJson());
        externalDatabaseStorageRepository.update(databaseStorage);

        historyRepository.insert(storageId, "UPDATE", true, "DB 저장소 수정 완료", "연결 정보가 수정되어 상태를 PENDING으로 변경했어요.");
        return queryService.getDetail(storageId);
    }

    @Transactional
    public DatabaseConnectionTestResponse revalidate(Long storageId) {
        Map<String, Object> detailMap = requireDetail(storageId);
        DatabaseStorageRegistrationRequest request = buildRequestFromStoredDetail(detailMap);

        try {
            DatabaseConnectionTestResult result = connectionTestSupport.test(request);
            String connectionStatus = result.success() ? "VERIFIED" : "FAILED";
            externalStorageRepository.updateConnectionStatus(storageId, connectionStatus, result.success(),
                    result.success() ? null : result.message(), null);
            historyRepository.insert(storageId, "TEST", result.success(),
                    result.success() ? "등록 저장소 연결 재검증 성공" : "등록 저장소 연결 재검증 실패",
                    request.getVendor() + " / " + request.getJdbcUrl());
            return DatabaseConnectionTestResponse.builder()
                    .success(result.success())
                    .message(result.message())
                    .build();
        } catch (RuntimeException e) {
            log.warn("Registered DB storage revalidation failed. storageId={}", storageId, e);
            externalStorageRepository.updateConnectionStatus(storageId, "FAILED", false, e.getMessage(), null);
            historyRepository.insert(storageId, "TEST", false, "등록 저장소 연결 재검증 실패", e.getMessage());
            throw e;
        }
    }

    @Transactional
    public void delete(Long storageId) {
        requireDetail(storageId);
        externalDatabaseStorageRepository.deleteByStorageId(storageId);
        externalStorageRepository.deleteById(storageId);
        log.info("DB storage deleted. storageId={}", storageId);
    }

    private Map<String, Object> requireDetail(Long storageId) {
        Map<String, Object> detailMap = externalDatabaseStorageRepository.findDetail(storageId);
        if (detailMap == null || detailMap.isEmpty()) {
            throw new AppException("요청한 저장소를 찾을 수 없어요.");
        }
        return detailMap;
    }

    private DatabaseStorageRegistrationRequest mergeWithStoredCredentials(Map<String, Object> detailMap,
                                                                          DatabaseStorageRegistrationRequest request) {
        if (request == null) {
            throw new AppException("수정 요청이 비어 있습니다.");
        }

        DatabaseStorageRegistrationRequest merged = new DatabaseStorageRegistrationRequest();
        merged.setName(request.getName());
        merged.setDescription(request.getDescription());
        merged.setVendor(request.getVendor());
        merged.setAuthMethod(request.getAuthMethod());
        merged.setJdbcUrl(request.getJdbcUrl());
        merged.setPort(request.getPort());
        merged.setUsername(normalizeBlankToNull(request.getUsername()));
        merged.setSchemaName(normalizeBlankToNull(request.getSchemaName()));
        merged.setDatabaseName(normalizeBlankToNull(request.getDatabaseName()));

        DatabaseAuthMethod authMethod = request.getAuthMethod();
        if (authMethod == null) {
            throw new AppException("인증 방식을 선택해 주세요.");
        }

        if (authMethod == DatabaseAuthMethod.PASSWORD) {
            String password = normalizeBlankToNull(request.getPassword());
            if (password == null) {
                password = credentialEncryptor.decryptToString(asString(detailMap.get("passwordEncrypted")), null);
            }
            merged.setPassword(password);
            merged.setCertificateAuth(null);
        } else {
            var storedMeta = certificateMetaService.decrypt(asString(detailMap.get("credentialMetaJson")), null, credentialEncryptor);
            DatabaseCertificateAuthRequest certificateAuth = request.getCertificateAuth();
            if (certificateAuth == null) {
                certificateAuth = new DatabaseCertificateAuthRequest();
            }
            certificateAuth.setTrustStorePath(firstNonBlank(certificateAuth.getTrustStorePath(), storedMeta == null ? null : storedMeta.getTrustStorePath()));
            certificateAuth.setKeyStorePath(firstNonBlank(certificateAuth.getKeyStorePath(), storedMeta == null ? null : storedMeta.getKeyStorePath()));
            certificateAuth.setTrustStorePassword(firstNonBlank(certificateAuth.getTrustStorePassword(), storedMeta == null ? null : storedMeta.getTrustStorePassword()));
            certificateAuth.setKeyStorePassword(firstNonBlank(certificateAuth.getKeyStorePassword(), storedMeta == null ? null : storedMeta.getKeyStorePassword()));
            certificateAuth.setKeyAlias(firstNonBlank(certificateAuth.getKeyAlias(), storedMeta == null ? null : storedMeta.getKeyAlias()));
            certificateAuth.setSslMode(firstNonBlank(certificateAuth.getSslMode(), storedMeta == null ? null : storedMeta.getSslMode()));
            merged.setCertificateAuth(certificateAuth);
            merged.setPassword(null);
        }

        enforceDatabaseNameRule(merged);
        return merged;
    }

    private DatabaseStorageRegistrationRequest buildRequestFromStoredDetail(Map<String, Object> detailMap) {
        DatabaseStorageRegistrationRequest request = new DatabaseStorageRegistrationRequest();
        request.setName(asString(detailMap.get("name")));
        request.setDescription(asString(detailMap.get("description")));
        request.setVendor(DatabaseVendor.valueOf(asString(detailMap.get("vendor"))));
        request.setAuthMethod(DatabaseAuthMethod.valueOf(asString(detailMap.get("authMethod"))));
        request.setJdbcUrl(asString(detailMap.get("jdbcUrl")));
        request.setPort(asInteger(detailMap.get("port")));
        request.setUsername(asString(detailMap.get("username")));
        request.setSchemaName(asString(detailMap.get("schemaName")));
        request.setDatabaseName(asString(detailMap.get("databaseName")));

        if (request.getAuthMethod() == DatabaseAuthMethod.PASSWORD) {
            request.setPassword(credentialEncryptor.decryptToString(asString(detailMap.get("passwordEncrypted")), null));
            return request;
        }

        var meta = certificateMetaService.decrypt(asString(detailMap.get("credentialMetaJson")), null, credentialEncryptor);
        DatabaseCertificateAuthRequest certificateAuth = new DatabaseCertificateAuthRequest();
        if (meta != null) {
            certificateAuth.setTrustStorePath(meta.getTrustStorePath());
            certificateAuth.setTrustStorePassword(meta.getTrustStorePassword());
            certificateAuth.setKeyStorePath(meta.getKeyStorePath());
            certificateAuth.setKeyStorePassword(meta.getKeyStorePassword());
            certificateAuth.setKeyAlias(meta.getKeyAlias());
            certificateAuth.setSslMode(meta.getSslMode());
        }
        request.setCertificateAuth(certificateAuth);
        return request;
    }

    private void enforceDatabaseNameRule(DatabaseStorageRegistrationRequest request) {
        if (request.getVendor() == DatabaseVendor.POSTGRESQL
                && normalizeBlankToNull(request.getDatabaseName()) == null
                && !containsDatabaseSegment(request.getJdbcUrl())) {
            throw new AppException("데이터베이스명은 PostgreSQL에서 필수입니다.");
        }
    }

    private boolean containsDatabaseSegment(String jdbcUrl) {
        String normalized = normalizeBlankToNull(jdbcUrl);
        if (normalized == null) {
            return false;
        }
        int slashIndex = normalized.lastIndexOf('/');
        return slashIndex >= 0 && slashIndex + 1 < normalized.length();
    }

    private String firstNonBlank(String preferred, String fallback) {
        String normalizedPreferred = normalizeBlankToNull(preferred);
        return normalizedPreferred != null ? normalizedPreferred : normalizeBlankToNull(fallback);
    }

    private String normalizeBlankToNull(String value) {
        return value == null || value.isBlank() ? null : value;
    }

    private String asString(Object value) {
        return value == null ? null : String.valueOf(value);
    }

    private Integer asInteger(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Number number) {
            return number.intValue();
        }
        return Integer.parseInt(String.valueOf(value));
    }
}
