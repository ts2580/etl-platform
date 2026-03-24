package com.etl.sfdc.storage.dto;

import com.etlplatform.common.storage.database.DatabaseVendor;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class DatabaseStorageOptionResponse {
    private final Long id;
    private final String name;
    private final DatabaseVendor vendor;
    private final String schemaName;
    private final String username;
    private final String serviceName;
    private final String sid;
    private final String connectionStatus;
}
