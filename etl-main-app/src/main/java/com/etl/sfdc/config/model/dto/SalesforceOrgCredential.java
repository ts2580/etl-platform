package com.etl.sfdc.config.model.dto;

import lombok.Data;

@Data
public class SalesforceOrgCredential {
    private Long id;
    private String orgKey;
    private String orgName;
    private String myDomain;
    private String schemaName;
    private String clientId;
    private String clientSecret;
    private String accessToken;
    private String accessTokenIssuedAt;
    private Long credentialVersion;
    private Boolean isActive;
    private Boolean isDefault;
    private String createdAt;
    private String updatedAt;
}
