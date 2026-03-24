package com.etlplatform.common.storage.database;

import com.etlplatform.common.error.AppException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class DatabaseCertificateMetaCodec {

    private final ObjectMapper objectMapper;

    public DatabaseCertificateMetaCodec() {
        this(new ObjectMapper());
    }

    public DatabaseCertificateMetaCodec(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public String toJson(DatabaseCertificateAuthRequest request, DatabaseCredentialEncryptor encryptor, String userKey) {
        if (request == null) {
            return null;
        }

        StoredCertificateCredentialMeta meta = new StoredCertificateCredentialMeta();
        meta.setTrustStorePath(request.getTrustStorePath());
        meta.setTrustStorePasswordEncrypted(encryptor.encrypt(request.getTrustStorePassword(), userKey));
        meta.setKeyStorePath(request.getKeyStorePath());
        meta.setKeyStorePasswordEncrypted(encryptor.encrypt(request.getKeyStorePassword(), userKey));
        meta.setKeyAlias(request.getKeyAlias());
        meta.setSslMode(request.getSslMode());

        try {
            return objectMapper.writeValueAsString(meta);
        } catch (Exception e) {
            throw new AppException("인증서 메타데이터 직렬화에 실패했어요.", e);
        }
    }

    public StoredCertificateCredentialMeta parse(String rawJson) {
        if (rawJson == null || rawJson.isBlank()) {
            return null;
        }
        try {
            return objectMapper.readValue(rawJson, StoredCertificateCredentialMeta.class);
        } catch (Exception e) {
            throw new AppException("인증서 메타데이터 파싱에 실패했어요.", e);
        }
    }

    public DecryptedStoredCertificateCredentialMeta decrypt(StoredCertificateCredentialMeta storedMeta,
                                                            String userKey,
                                                            DatabaseCredentialEncryptor encryptor) {
        if (storedMeta == null) {
            return null;
        }
        return new DecryptedStoredCertificateCredentialMeta(
                storedMeta.getTrustStorePath(),
                decryptNullable(storedMeta.getTrustStorePasswordEncrypted(), userKey, encryptor),
                storedMeta.getKeyStorePath(),
                decryptNullable(storedMeta.getKeyStorePasswordEncrypted(), userKey, encryptor),
                storedMeta.getKeyAlias(),
                storedMeta.getSslMode());
    }

    private String decryptNullable(String encryptedValue, String userKey, DatabaseCredentialEncryptor encryptor) {
        if (encryptedValue == null || encryptedValue.isBlank()) {
            return null;
        }
        return encryptor.decryptToString(encryptedValue, userKey);
    }
}
