package com.sfdcupload.storage.model.repository;

import com.sfdcupload.storage.model.dto.ExternalDatabaseStorage;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface ExternalDatabaseStorageRepository {

    @Insert("INSERT INTO config.external_database_storage (storage_id, vendor, jdbc_url, auth_method, host, port, database_name, service_name, sid, username, password_encrypted, schema_name, jdbc_options_json, credential_meta_json) " +
            "VALUES (#{storageId}, #{vendor}, #{jdbcUrl}, #{authMethod}, #{host}, #{port}, #{databaseName}, #{serviceName}, #{sid}, #{username}, #{passwordEncrypted}, #{schemaName}, #{jdbcOptionsJson}, #{credentialMetaJson})")
    int insert(ExternalDatabaseStorage storage);
}
