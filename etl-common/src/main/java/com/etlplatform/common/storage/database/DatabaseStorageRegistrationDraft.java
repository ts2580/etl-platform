package com.etlplatform.common.storage.database;

public record DatabaseStorageRegistrationDraft(
        DatabaseVendor vendor,
        String jdbcUrl,
        DatabaseAuthMethod authMethod,
        String host,
        Integer port,
        String databaseName,
        String serviceName,
        String sid,
        String username,
        String passwordEncrypted,
        String schemaName,
        String credentialMetaJson
) {
}
