package com.etl.sfdc.storage.model.dto;

import com.etlplatform.common.storage.database.DatabaseAuthMethod;
import com.etlplatform.common.storage.database.DatabaseVendor;
import lombok.Getter;
import lombok.Setter;

import java.time.LocalDateTime;

@Getter
@Setter
public class ExternalDatabaseStorage {
    private Long storageId;
    private DatabaseVendor vendor;
    private DatabaseAuthMethod authMethod;
    private String jdbcUrl;
    private String host;
    private Integer port;
    private String databaseName;
    private String serviceName;
    private String sid;
    private String username;
    private String passwordEncrypted;
    private String schemaName;
    private String jdbcOptionsJson;
    private String credentialMetaJson;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
}
