package com.etl.sfdc.storage.dto;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class StoredCertificateCredentialMeta {
    private final String trustStorePath;
    private final String trustStorePasswordEncrypted;
    private final String keyStorePath;
    private final String keyStorePasswordEncrypted;
    private final String keyAlias;
    private final String sslMode;
}
