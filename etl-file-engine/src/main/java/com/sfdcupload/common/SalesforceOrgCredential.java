package com.sfdcupload.common;

import lombok.Data;

@Data
public class SalesforceOrgCredential {
    private Long id;
    private String orgKey;
    private String orgName;
    private String myDomain;
    private String clientId;
    private String clientSecret;
    private String accessToken;
    private String accessTokenIssuedAt;
    private Boolean isActive;
    private Boolean isDefault;
    private String createdAt;
    private String updatedAt;
}
