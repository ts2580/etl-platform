package com.etl.sfdc.storage.dto;

import com.etlplatform.common.storage.database.DatabaseAuthMethod;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class DatabaseStoredCredentialDecryptResponse {
    private final DatabaseAuthMethod authMethod;
    private final String usernameMasked;
    private final String passwordMasked;
    private final String trustStorePasswordMasked;
    private final String keyStorePasswordMasked;
    private final String keyAlias;
    private final String sslMode;
    private final String username;
    private final String password;
    private final String trustStorePassword;
    private final String keyStorePassword;
    private final boolean includeRaw;
    private final boolean tokenIssued;
    private final String revealToken;
    private final int ttlSeconds;
}
