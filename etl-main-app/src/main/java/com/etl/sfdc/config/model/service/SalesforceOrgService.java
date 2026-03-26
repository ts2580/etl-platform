package com.etl.sfdc.config.model.service;

import com.etl.sfdc.config.model.dto.SalesforceOrgCredential;
import com.etl.sfdc.config.model.repository.SalesforceOrgCredentialRepository;
import com.etlplatform.common.error.AppException;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.net.URI;
import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.regex.Pattern;

@Service
@RequiredArgsConstructor
public class SalesforceOrgService {

    private static final Pattern SCHEMA_SAFE_ORG_NAME = Pattern.compile("^[A-Za-z][A-Za-z0-9_ ]{1,47}$");

    private final SalesforceOrgCredentialRepository repository;
    private final DataSource dataSource;

    public List<SalesforceOrgCredential> getActiveOrgs() {
        return repository.findAllActiveOrDefault();
    }

    public SalesforceOrgCredential getOrg(String orgKey) {
        return repository.findByOrgKey(orgKey);
    }

    public SalesforceOrgCredential registerOrUpdateOrg(String orgName, String myDomain, boolean isDefault) {
        SalesforceOrgCredential org = new SalesforceOrgCredential();
        org.setOrgKey(UUID.randomUUID().toString());
        org.setOrgName(normalizeOrgName(orgName == null || orgName.isBlank() ? myDomain : orgName));
        org.setMyDomain(normalizeMyDomain(myDomain));
        org.setSchemaName(buildSchemaName(org.getOrgName()));
        org.setIsActive(true);
        org.setIsDefault(isDefault);
        if (isDefault) {
            repository.unsetDefaultOrgs();
        }
        repository.upsertSalesforceOrg(org);
        ensureOrgSchemaExists(org.getSchemaName());
        return repository.findByOrgKey(org.getOrgKey());
    }

    public SalesforceOrgCredential registerOrUpdateClientCredentials(String orgNameInput,
                                                                      String myDomainInput,
                                                                      String clientId,
                                                                      String clientSecret,
                                                                      boolean isDefault) {
        String myDomain = normalizeMyDomain(myDomainInput);
        // orgKey는 Salesforce orgId가 아니라, 현재 시스템에서 org를 식별하기 위한 myDomain 기반 natural key입니다.
        String resolvedOrgKey = normalizeMyDomain(myDomain);

        String sourceOrgName = (orgNameInput == null || orgNameInput.isBlank()) ? resolvedOrgKey : orgNameInput;
        String resolvedOrgName;
        try {
            resolvedOrgName = normalizeOrgName(sourceOrgName);
        } catch (Exception e) {
            resolvedOrgName = normalizeOrgName(coerceOrgNameForSchema(sourceOrgName));
        }
        String schemaName = buildSchemaName(resolvedOrgName);

        SalesforceOrgCredential existing = repository.findByOrgKey(resolvedOrgKey);

        if (existing == null) {
            SalesforceOrgCredential org = new SalesforceOrgCredential();
            org.setOrgKey(resolvedOrgKey);
            org.setOrgName(resolvedOrgName);
            org.setMyDomain(myDomain);
            org.setSchemaName(schemaName);
            org.setClientId(clientId);
            org.setClientSecret(clientSecret);
            org.setIsActive(true);
            org.setIsDefault(isDefault || hasNoDefaultActiveOrg());
            if (Boolean.TRUE.equals(org.getIsDefault())) {
                repository.unsetDefaultOrgs();
            }
            repository.upsertSalesforceOrg(org);
            ensureOrgSchemaExists(schemaName);
            return repository.findByOrgKey(org.getOrgKey());
        }

        existing.setOrgName(existing.getOrgName() == null || existing.getOrgName().isBlank() ? resolvedOrgName : coerceOrgNameForSchema(existing.getOrgName()));
        existing.setMyDomain(myDomain);
        existing.setSchemaName(schemaName);
        existing.setClientId(clientId);
        existing.setClientSecret(clientSecret);
        existing.setIsActive(true);
        existing.setIsDefault(existing.getIsDefault() != null ? existing.getIsDefault() || isDefault : isDefault);
        if (isDefault) {
            repository.unsetDefaultOrgs();
            existing.setIsDefault(true);
        }

        repository.upsertSalesforceOrg(existing);
        ensureOrgSchemaExists(schemaName);
        return repository.findByOrgKey(existing.getOrgKey());
    }

    public SalesforceOrgCredential updateOrg(String orgKey,
                                              String orgNameInput,
                                              String myDomainInput,
                                              String clientId,
                                              String clientSecret,
                                              boolean isDefault) {
        SalesforceOrgCredential existing = repository.findByOrgKey(orgKey);
        if (existing == null) {
            throw new AppException("존재하지 않는 Org입니다.");
        }

        String myDomain = normalizeMyDomain(myDomainInput);
        String sourceOrgName = (orgNameInput == null || orgNameInput.isBlank()) ? existing.getOrgName() : orgNameInput;
        String resolvedOrgName;
        try {
            resolvedOrgName = normalizeOrgName(sourceOrgName);
        } catch (Exception e) {
            resolvedOrgName = normalizeOrgName(coerceOrgNameForSchema(sourceOrgName));
        }
        String schemaName = buildSchemaName(resolvedOrgName);

        existing.setOrgName(resolvedOrgName);
        existing.setMyDomain(myDomain);
        existing.setSchemaName(schemaName);
        existing.setClientId(clientId);
        if (clientSecret != null && !clientSecret.isBlank()) {
            existing.setClientSecret(clientSecret);
        }
        existing.setIsActive(true);
        if (isDefault) {
            repository.unsetDefaultOrgs();
            existing.setIsDefault(true);
        }
        repository.upsertSalesforceOrg(existing);
        ensureOrgSchemaExists(schemaName);
        return repository.findByOrgKey(existing.getOrgKey());
    }

    public SalesforceOrgCredential updateClientCredentials(String orgKey,
                                                       String clientId,
                                                       String clientSecret) {
        return updateOrg(orgKey, null, null, clientId, clientSecret, false);
    }

    public SalesforceOrgCredential setDefaultOrg(String orgKey) {
        repository.unsetDefaultOrgs();
        repository.setDefaultOrg(orgKey);
        return repository.findByOrgKey(orgKey);
    }

    public void deactivateOrg(String orgKey) {
        SalesforceOrgCredential org = repository.findByOrgKey(orgKey);
        if (org == null) {
            return;
        }

        repository.deleteByOrgKey(orgKey);
        dropOrgSchema(org.getSchemaName());

        if (Boolean.TRUE.equals(org.getIsDefault())) {
            SalesforceOrgCredential nextOrg = getDefaultOrg();
            if (nextOrg != null) {
                repository.setDefaultOrg(nextOrg.getOrgKey());
            }
        }
    }

    public SalesforceOrgCredential getDefaultOrg() {
        List<SalesforceOrgCredential> active = repository.findAllActiveOrDefault();
        return active.isEmpty() ? null : active.get(0);
    }

    public boolean hasNoDefaultActiveOrg() {
        return getActiveOrgs().stream().noneMatch(org -> Boolean.TRUE.equals(org.getIsDefault()));
    }

    public void persistTokens(String orgKey, String accessToken) {
        SalesforceOrgCredential existing = repository.findByOrgKey(orgKey);
        if (existing == null) {
            return;
        }
        if (accessToken != null && !accessToken.isBlank()) {
            existing.setAccessToken(accessToken);
        }
        repository.upsertSalesforceOrg(existing);
    }

    public void storeAccessToken(String orgKey, String accessToken) {
        if (orgKey == null || orgKey.isBlank() || accessToken == null || accessToken.isBlank()) {
            return;
        }
        repository.updateAccessToken(orgKey, accessToken);
    }

    public String resolveSchemaName(String orgKey) {
        SalesforceOrgCredential org = repository.findByOrgKey(orgKey);
        return org == null
                ? buildSchemaName(orgKey)
                : (org.getSchemaName() == null || org.getSchemaName().isBlank()
                ? buildSchemaName(org.getOrgName() == null || org.getOrgName().isBlank() ? org.getOrgKey() : org.getOrgName())
                : org.getSchemaName());
    }

    private String coerceOrgNameForSchema(String rawOrgName) {
        String value = rawOrgName == null ? "" : rawOrgName.trim();
        value = value.replaceAll("\\p{Cntrl}", " ");
        value = value.replaceAll("[^A-Za-z0-9_ ]", "_");
        value = value.replaceAll("\\s+", "_");
        value = value.replaceAll("_+", "_");
        value = value.replaceAll("^_+", "");
        value = value.replaceAll("_+$", "");

        if (value.isBlank()) {
            return "org_default";
        }

        if (Character.isDigit(value.charAt(0))) {
            value = "org_" + value;
        }

        if (value.length() < 2) {
            value = value + "_org";
        }

        if (value.length() > 48) {
            value = value.substring(0, 48);
        }

        return value;
    }

    public String buildSchemaName(String orgName) {
        String safeOrgName = orgName == null || orgName.isBlank() ? "default_org" : orgName;
        String normalizedOrgName;
        try {
            normalizedOrgName = normalizeOrgName(safeOrgName);
        } catch (Exception e) {
            normalizedOrgName = coerceOrgNameForSchema(safeOrgName);
        }

        String sanitized = normalizedOrgName.toLowerCase(Locale.ROOT)
                .replace(' ', '_')
                .replaceAll("_+", "_")
                .replaceAll("^_+|_+$", "");
        if (sanitized.isBlank()) {
            sanitized = "default_org";
        }
        if (sanitized.length() > 48) {
            sanitized = sanitized.substring(0, 48);
        }
        return "org_" + sanitized;
    }

    public void ensureOrgSchemaExists(String schemaName) {
        executeSchemaSql("CREATE DATABASE IF NOT EXISTS `" + schemaName + "` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci");
    }

    public void dropOrgSchema(String schemaName) {
        if (schemaName == null || schemaName.isBlank()) {
            return;
        }
        executeSchemaSql("DROP DATABASE IF EXISTS `" + schemaName + "`");
    }

    private void executeSchemaSql(String sql) {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute(sql);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to execute schema SQL: " + sql, e);
        }
    }

    public String normalizeMyDomain(String rawUrl) {
        if (rawUrl == null || rawUrl.isBlank()) {
            return null;
        }
        try {
            String host = URI.create(rawUrl).getHost();
            return host == null || host.isBlank() ? rawUrl : host;
        } catch (Exception e) {
            return rawUrl;
        }
    }

    public String normalizeOrgName(String rawOrgName) {
        if (rawOrgName == null) {
            throw new AppException("Org 이름은 비어 있을 수 없어요.");
        }
        String normalized = rawOrgName.trim().replaceAll("\\s+", " ");
        if (!SCHEMA_SAFE_ORG_NAME.matcher(normalized).matches()) {
            throw new AppException("Org 이름은 영문으로 시작하고, 영문/숫자/공백/_ 만 사용할 수 있어요. 길이는 2~48자여야 해요.");
        }
        return normalized;
    }
}
