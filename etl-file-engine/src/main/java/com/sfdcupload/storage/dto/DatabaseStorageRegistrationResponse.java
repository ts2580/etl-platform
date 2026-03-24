package com.sfdcupload.storage.dto;

import com.etlplatform.common.storage.database.DatabaseAuthMethod;
import com.etlplatform.common.storage.database.DatabaseVendor;
import com.etlplatform.common.storage.database.StorageType;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class DatabaseStorageRegistrationResponse {
    private final Long id;
    private final String name;
    private final StorageType storageType;
    private final DatabaseVendor vendor;
    private final DatabaseAuthMethod authMethod;
    private final String jdbcUrl;
    private final String username;
    private final String connectionStatus;
    private final boolean enabled;
}
