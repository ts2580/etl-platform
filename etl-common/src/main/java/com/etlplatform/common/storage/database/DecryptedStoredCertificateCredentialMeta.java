package com.etlplatform.common.storage.database;

public record DecryptedStoredCertificateCredentialMeta(
        String trustStorePath,
        String trustStorePassword,
        String keyStorePath,
        String keyStorePassword,
        String keyAlias,
        String sslMode
) {
}
