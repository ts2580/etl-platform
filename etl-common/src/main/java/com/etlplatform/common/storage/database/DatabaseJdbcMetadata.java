package com.etlplatform.common.storage.database;

public record DatabaseJdbcMetadata(String host,
                                   Integer port,
                                   String databaseName,
                                   String serviceName,
                                   String sid) {
}
