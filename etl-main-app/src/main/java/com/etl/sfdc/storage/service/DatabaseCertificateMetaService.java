package com.etl.sfdc.storage.service;

import com.etl.sfdc.storage.dto.DecryptedStoredCertificateCredentialMeta;
import com.etl.sfdc.storage.dto.StoredCertificateCredentialMeta;
import com.etl.sfdc.storage.support.CertificateCredentialMetaSerializer;
import com.etl.sfdc.storage.support.DatabaseCredentialEncryptor;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class DatabaseCertificateMetaService {

    private final CertificateCredentialMetaSerializer serializer;

    public String buildCredentialMetaJson(com.etlplatform.common.storage.database.DatabaseCertificateAuthRequest certificateAuth,
                                          DatabaseCredentialEncryptor encryptor,
                                          String userKey) {
        return serializer.toJson(certificateAuth, encryptor, userKey);
    }

    public StoredCertificateCredentialMeta parse(String metaJson) {
        com.etlplatform.common.storage.database.StoredCertificateCredentialMeta parsed = serializer.parse(metaJson);
        if (parsed == null) {
            return null;
        }
        return StoredCertificateCredentialMeta.builder()
                .trustStorePath(parsed.getTrustStorePath())
                .trustStorePasswordEncrypted(parsed.getTrustStorePasswordEncrypted())
                .keyStorePath(parsed.getKeyStorePath())
                .keyStorePasswordEncrypted(parsed.getKeyStorePasswordEncrypted())
                .keyAlias(parsed.getKeyAlias())
                .sslMode(parsed.getSslMode())
                .build();
    }

    public DecryptedStoredCertificateCredentialMeta decrypt(String metaJson, String userKey, DatabaseCredentialEncryptor encryptor) {
        com.etlplatform.common.storage.database.DecryptedStoredCertificateCredentialMeta decrypted = serializer.decrypt(serializer.parse(metaJson), userKey, encryptor);
        if (decrypted == null) {
            return null;
        }
        return DecryptedStoredCertificateCredentialMeta.builder()
                .trustStorePath(decrypted.trustStorePath())
                .trustStorePassword(decrypted.trustStorePassword())
                .keyStorePath(decrypted.keyStorePath())
                .keyStorePassword(decrypted.keyStorePassword())
                .keyAlias(decrypted.keyAlias())
                .sslMode(decrypted.sslMode())
                .build();
    }
}
