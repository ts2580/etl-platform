package com.etl.sfdc.storage.service;

import com.etl.sfdc.storage.dto.DatabaseStoredCredentialDecryptRequest;
import com.etl.sfdc.storage.dto.DatabaseStoredCredentialDecryptResponse;
import com.etl.sfdc.storage.dto.DatabaseStorageDetailResponse;
import com.etl.sfdc.storage.dto.DatabaseStorageListPageResponse;
import com.etl.sfdc.storage.dto.DatabaseStorageListResponse;
import com.etl.sfdc.storage.dto.DatabaseStorageOptionResponse;
import com.etlplatform.common.storage.database.DatabaseAuthMethod;
import com.etl.sfdc.storage.model.repository.ExternalDatabaseStorageRepository;
import com.etl.sfdc.storage.model.repository.ExternalStorageConnectionHistoryRepository;
import com.etl.sfdc.storage.support.DatabaseCredentialEncryptor;
import com.etlplatform.common.error.AppException;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Locale;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class DatabaseStorageQueryService {

    private static final int MAX_PAGE_SIZE = 100;
    private static final int DECRYPT_TTL_SECONDS = 60;

    private final ExternalDatabaseStorageRepository externalDatabaseStorageRepository;
    private final ExternalStorageConnectionHistoryRepository historyRepository;
    private final DatabaseCredentialEncryptor credentialEncryptor;
    private final DatabaseCertificateMetaService certificateMetaService;
    private final DatabaseCredentialRevealService credentialRevealService;

    public DatabaseStorageListPageResponse getList(int page, int size, String sortBy, String sortDirection) {
        int normalizedPage = Math.max(page, 0);
        int normalizedSize = normalizeSize(size);
        String orderBy = resolveOrderBy(sortBy, sortDirection);

        int offset = normalizedPage * normalizedSize;
        List<DatabaseStorageListResponse> items = externalDatabaseStorageRepository
                .findAllSummaries(normalizedSize, offset, orderBy)
                .stream()
                .map(this::toListResponse)
                .toList();

        long totalElements = externalDatabaseStorageRepository.countAll();
        int totalPages = normalizedSize == 0 ? 0 : (int) Math.ceil((double) totalElements / normalizedSize);

        return DatabaseStorageListPageResponse.builder()
                .page(normalizedPage)
                .size(normalizedSize)
                .totalElements(totalElements)
                .totalPages(Math.max(totalPages, 0))
                .items(items)
                .build();
    }


    public List<DatabaseStorageOptionResponse> getRoutingTargetOptions() {
        return externalDatabaseStorageRepository.findAllOptions()
                .stream()
                .map(row -> DatabaseStorageOptionResponse.builder()
                        .id(asLong(row.get("id")))
                        .name(asString(row.get("name")))
                        .vendor(com.etlplatform.common.storage.database.DatabaseVendor.valueOf(asString(row.get("vendor"))))
                        .schemaName(asString(row.get("schemaName")))
                        .username(asString(row.get("username")))
                        .serviceName(asString(row.get("serviceName")))
                        .sid(asString(row.get("sid")))
                        .connectionStatus(asString(row.get("connectionStatus")))
                        .build())
                .toList();
    }

    public DatabaseStorageDetailResponse getDetail(Long storageId) {
        Map<String, Object> detailMap = externalDatabaseStorageRepository.findDetail(storageId);
        if (detailMap == null || detailMap.isEmpty()) {
            throw new AppException("요청한 저장소를 찾을 수 없어요.");
        }

        return toDetailResponse(detailMap);
    }

    public DatabaseStoredCredentialDecryptResponse decryptStoredCredentials(Long storageId,
                                                                          DatabaseStoredCredentialDecryptRequest request,
                                                                          String requesterIdentity) {
        Map<String, Object> detailMap = externalDatabaseStorageRepository.findDetail(storageId);
        if (detailMap == null || detailMap.isEmpty()) {
            throw new AppException("요청한 저장소를 찾을 수 없어요.");
        }

        if (request == null) {
            throw new AppException("복호화 요청이 비어 있습니다.");
        }

        String requester = normalizeRequester(requesterIdentity);
        DatabaseAuthMethod authMethod = DatabaseAuthMethod.valueOf(String.valueOf(detailMap.get("authMethod")));
        String username = asString(detailMap.get("username"));
        String encryptedPassword = asString(detailMap.get("passwordEncrypted"));
        String credentialMetaJson = asString(detailMap.get("credentialMetaJson"));

        if (!request.isRevealRaw()) {
            var masked = buildResponse(authMethod, username, encryptedPassword, credentialMetaJson, false);
            historyRepository.insert(storageId, "DECRYPT", true, "복호화 조회(마스킹)", "requester=" + requester);
            return masked;
        }

        String revealToken = request.getRevealToken();
        if (revealToken == null || revealToken.isBlank()) {
            String issuedToken = credentialRevealService.issue(storageId, requester);
            historyRepository.insert(storageId, "DECRYPT", true, "복호화 평문 조회 토큰 발급", "requester=" + requester);

            var masked = buildResponse(authMethod, username, encryptedPassword, credentialMetaJson, false);
            return DatabaseStoredCredentialDecryptResponse.builder()
                    .authMethod(masked.getAuthMethod())
                    .usernameMasked(masked.getUsernameMasked())
                    .passwordMasked(masked.getPasswordMasked())
                    .trustStorePasswordMasked(masked.getTrustStorePasswordMasked())
                    .keyStorePasswordMasked(masked.getKeyStorePasswordMasked())
                    .keyAlias(masked.getKeyAlias())
                    .sslMode(masked.getSslMode())
                    .username(null)
                    .password(null)
                    .trustStorePassword(null)
                    .keyStorePassword(null)
                    .includeRaw(false)
                    .tokenIssued(true)
                    .revealToken(issuedToken)
                    .ttlSeconds(DECRYPT_TTL_SECONDS)
                    .build();
        }

        if (!credentialRevealService.consumeIfValid(storageId, revealToken, requester)) {
            historyRepository.insert(storageId, "DECRYPT", false, "복호화 조회(평문)",
                    "requester=" + requester + ", token=" + credentialRevealService.summarize(revealToken));
            throw new AppException("복호화 토큰이 없거나 만료되었습니다. 재요청 후 다시 시도해 주세요.");
        }

        var raw = buildResponse(authMethod, username, encryptedPassword, credentialMetaJson, true);
        historyRepository.insert(storageId, "DECRYPT", true, "복호화 조회(평문)",
                "requester=" + requester + ", token=" + credentialRevealService.summarize(revealToken));

        return DatabaseStoredCredentialDecryptResponse.builder()
                .authMethod(raw.getAuthMethod())
                .usernameMasked(raw.getUsernameMasked())
                .passwordMasked(raw.getPasswordMasked())
                .trustStorePasswordMasked(raw.getTrustStorePasswordMasked())
                .keyStorePasswordMasked(raw.getKeyStorePasswordMasked())
                .keyAlias(raw.getKeyAlias())
                .sslMode(raw.getSslMode())
                .username(raw.getUsername())
                .password(raw.getPassword())
                .trustStorePassword(raw.getTrustStorePassword())
                .keyStorePassword(raw.getKeyStorePassword())
                .includeRaw(true)
                .tokenIssued(false)
                .revealToken(null)
                .ttlSeconds(0)
                .build();
    }

    private DatabaseStoredCredentialDecryptResponse buildResponse(DatabaseAuthMethod authMethod,
                                                               String username,
                                                               String encryptedPassword,
                                                               String credentialMetaJson,
                                                               boolean rawAllowed) {
        if (authMethod == DatabaseAuthMethod.PASSWORD) {
            String plainPassword = decryptOptional(encryptedPassword);
            return DatabaseStoredCredentialDecryptResponse.builder()
                    .authMethod(authMethod)
                    .usernameMasked(maskValue(normalizeBlankToNull(username)))
                    .passwordMasked(maskValue(plainPassword))
                    .trustStorePasswordMasked(null)
                    .keyStorePasswordMasked(null)
                    .keyAlias(null)
                    .sslMode(null)
                    .username(rawAllowed ? normalizeBlankToNull(username) : null)
                    .password(rawAllowed ? plainPassword : null)
                    .trustStorePassword(null)
                    .keyStorePassword(null)
                    .includeRaw(rawAllowed)
                    .tokenIssued(false)
                    .revealToken(null)
                    .ttlSeconds(0)
                    .build();
        }

        var decryptedMeta = certificateMetaService.decrypt(credentialMetaJson, null, credentialEncryptor);
        String plainPassword = decryptOptional(encryptedPassword);
        String plainTrustStorePassword = rawAllowed && decryptedMeta != null ? decryptedMeta.getTrustStorePassword() : null;
        String plainKeyStorePassword = rawAllowed && decryptedMeta != null ? decryptedMeta.getKeyStorePassword() : null;

        return DatabaseStoredCredentialDecryptResponse.builder()
                .authMethod(authMethod)
                .usernameMasked(maskValue(normalizeBlankToNull(username)))
                .passwordMasked(maskValue(plainPassword))
                .trustStorePasswordMasked(maskValue(decryptedMeta == null ? null : decryptedMeta.getTrustStorePassword()))
                .keyStorePasswordMasked(maskValue(decryptedMeta == null ? null : decryptedMeta.getKeyStorePassword()))
                .keyAlias(decryptedMeta == null ? null : decryptedMeta.getKeyAlias())
                .sslMode(decryptedMeta == null ? null : decryptedMeta.getSslMode())
                .username(rawAllowed ? normalizeBlankToNull(username) : null)
                .password(rawAllowed ? plainPassword : null)
                .trustStorePassword(rawAllowed ? plainTrustStorePassword : null)
                .keyStorePassword(rawAllowed ? plainKeyStorePassword : null)
                .includeRaw(rawAllowed)
                .tokenIssued(false)
                .revealToken(null)
                .ttlSeconds(0)
                .build();
    }

    private String decryptOptional(String encryptedPassword) {
        if (encryptedPassword == null || encryptedPassword.isBlank()) {
            return null;
        }
        return credentialEncryptor.decrypt(encryptedPassword, null).password();
    }

    private DatabaseStorageListResponse toListResponse(Map<String, Object> row) {
        return DatabaseStorageListResponse.builder()
                .id(asLong(row.get("id")))
                .name(asString(row.get("name")))
                .description(asString(row.get("description")))
                .storageType(asString(row.get("storageType")))
                .vendor(com.etlplatform.common.storage.database.DatabaseVendor.valueOf(asString(row.get("vendor"))))
                .authMethod(DatabaseAuthMethod.valueOf(asString(row.get("authMethod"))))
                .jdbcUrl(asString(row.get("jdbcUrl")))
                .username(asString(row.get("username")))
                .connectionStatus(asString(row.get("connectionStatus")))
                .createdAt(asString(row.get("createdAt")))
                .build();
    }

    private DatabaseStorageDetailResponse toDetailResponse(Map<String, Object> row) {
        String vendor = asString(row.get("vendor"));
        String authMethod = asString(row.get("authMethod"));
        String credentialMetaJson = asString(row.get("credentialMetaJson"));
        String trustStorePath = null;
        String keyStorePath = null;
        String keyAlias = null;
        String sslMode = null;
        boolean hasTrustStorePassword = false;
        boolean hasKeyStorePassword = false;

        if (DatabaseAuthMethod.CERTIFICATE.name().equals(authMethod)) {
            var meta = certificateMetaService.parse(credentialMetaJson);
            if (meta != null) {
                trustStorePath = meta.getTrustStorePath();
                keyStorePath = meta.getKeyStorePath();
                keyAlias = meta.getKeyAlias();
                sslMode = meta.getSslMode();
                hasTrustStorePassword = meta.getTrustStorePasswordEncrypted() != null && !meta.getTrustStorePasswordEncrypted().isBlank();
                hasKeyStorePassword = meta.getKeyStorePasswordEncrypted() != null && !meta.getKeyStorePasswordEncrypted().isBlank();
            }
        }

        return DatabaseStorageDetailResponse.builder()
                .id(asLong(row.get("storageId")))
                .name(asString(row.get("name")))
                .description(asString(row.get("description")))
                .storageType(asString(row.get("storageType")))
                .vendor(com.etlplatform.common.storage.database.DatabaseVendor.valueOf(vendor))
                .authMethod(DatabaseAuthMethod.valueOf(authMethod))
                .jdbcUrl(asString(row.get("jdbcUrl")))
                .port(asInteger(row.get("port")))
                .username(asString(row.get("username")))
                .schemaName(asString(row.get("schemaName")))
                .databaseName(asString(row.get("databaseName")))
                .serviceName(asString(row.get("serviceName")))
                .sid(asString(row.get("sid")))
                .connectionStatus(asString(row.get("connectionStatus")))
                .trustStorePath(trustStorePath)
                .keyStorePath(keyStorePath)
                .keyAlias(keyAlias)
                .sslMode(sslMode)
                .hasTrustStorePassword(hasTrustStorePassword)
                .hasKeyStorePassword(hasKeyStorePassword)
                .createdAt(asString(row.get("createdAt")))
                .updatedAt(asString(row.get("updatedAt")))
                .build();
    }

    private String resolveOrderBy(String sortBy, String sortDirection) {
        String direction = "desc".equalsIgnoreCase(sortDirection) ? "DESC" : "ASC";
        String key = normalizeBlankToNull(sortBy);

        return switch (key == null ? "id" : key.toLowerCase(Locale.ROOT)) {
            case "id" -> "s.id " + direction;
            case "name" -> "s.name " + direction;
            case "vendor" -> "d.vendor " + direction;
            case "status", "connectionstatus", "connection_status" -> "s.connection_status " + direction;
            case "authtype", "authmethod" -> "d.auth_method " + direction;
            case "type", "storagetype" -> "s.storage_type " + direction;
            default -> "s.id " + direction;
        };
    }

    private int normalizeSize(int size) {
        if (size <= 0) {
            return 20;
        }
        if (size > MAX_PAGE_SIZE) {
            return MAX_PAGE_SIZE;
        }
        return size;
    }

    private String asString(Object value) {
        return value == null ? null : String.valueOf(value);
    }

    private Long asLong(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Number number) {
            return number.longValue();
        }
        return Long.parseLong(String.valueOf(value));
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

    private String normalizeBlankToNull(String value) {
        return (value == null || value.isBlank()) ? null : value;
    }

    private String normalizeRequester(String value) {
        String normalized = normalizeBlankToNull(value);
        return normalized == null ? "unknown" : normalized;
    }

    private String maskValue(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        int length = value.length();
        if (length <= 2) {
            return "*".repeat(length);
        }
        if (length <= 4) {
            return value.substring(0, 1) + "***";
        }
        return value.substring(0, 2) + "******" + value.substring(length - 2);
    }
}
