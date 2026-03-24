package com.apache.sfdc.storage.model.repository;

import com.etlplatform.common.storage.database.ExternalDatabaseStorageDefinition;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

@Mapper
public interface ExternalDatabaseStorageRoutingRepository {

    @Select("SELECT s.id AS storageId, s.name AS name, s.connection_status AS connectionStatus, s.enabled AS enabled, " +
            "d.vendor AS vendor, d.auth_method AS authMethod, d.jdbc_url AS jdbcUrl, d.username AS username, " +
            "d.password_encrypted AS passwordEncrypted, d.schema_name AS schemaName, d.credential_meta_json AS credentialMetaJson " +
            "FROM config.external_storage s " +
            "JOIN config.external_database_storage d ON d.storage_id = s.id " +
            "WHERE s.id = #{storageId}")
    ExternalDatabaseStorageDefinition findByStorageId(@Param("storageId") Long storageId);
}
