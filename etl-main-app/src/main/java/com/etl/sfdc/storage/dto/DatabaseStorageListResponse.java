package com.etl.sfdc.storage.dto;

import com.etlplatform.common.storage.database.DatabaseAuthMethod;
import com.etlplatform.common.storage.database.DatabaseVendor;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class DatabaseStorageListResponse {
    private final Long id;
    private final String name;
    private final String description;
    private final String storageType;
    private final DatabaseVendor vendor;
    private final DatabaseAuthMethod authMethod;
    private final String jdbcUrl;
    private final String username;
    private final String connectionStatus;
    private final String createdAt;
}
