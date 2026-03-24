package com.etl.sfdc.storage.dto;

import com.etlplatform.common.storage.database.DatabaseAuthMethod;
import com.etlplatform.common.storage.database.DatabaseVendor;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class DatabaseStorageDetailResponse {
    private final Long id;
    private final String name;
    private final String description;
    private final String storageType;
    private final DatabaseVendor vendor;
    private final DatabaseAuthMethod authMethod;
    private final String jdbcUrl;
    private final Integer port;
    private final String username;
    private final String schemaName;
    private final String databaseName;
    private final String serviceName;
    private final String sid;
    private final String connectionStatus;
    private final String sslMode;
    private final String trustStorePath;
    private final String keyStorePath;
    private final String keyAlias;
    private final boolean hasTrustStorePassword;
    private final boolean hasKeyStorePassword;
    private final String createdAt;
    private final String updatedAt;
}
