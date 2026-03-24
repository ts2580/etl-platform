package com.etl.sfdc.storage.dto;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class DecryptedStoredCertificateCredentialMeta {
    private final String trustStorePath;
    private final String trustStorePassword;
    private final String keyStorePath;
    private final String keyStorePassword;
    private final String keyAlias;
    private final String sslMode;
}
