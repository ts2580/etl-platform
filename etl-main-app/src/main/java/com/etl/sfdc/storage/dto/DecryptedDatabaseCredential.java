package com.etl.sfdc.storage.dto;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class DecryptedDatabaseCredential {
    private final String password;
}
