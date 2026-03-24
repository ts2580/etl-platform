package com.etl.sfdc.storage.dto;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class DatabaseStoredCredentialDecryptRequest {
    private String userKey;
    private boolean revealRaw;
    private String revealToken;
}
