package com.etl.sfdc.storage.model.repository;

import org.apache.ibatis.annotations.Update;
import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import com.etl.sfdc.storage.model.dto.ExternalDatabaseStorage;

import java.util.List;
import java.util.Map;

@Mapper
public interface ExternalDatabaseStorageRepository {

    @Insert("INSERT INTO config.external_database_storage (storage_id, vendor, jdbc_url, auth_method, host, port, database_name, service_name, sid, username, password_encrypted, schema_name, jdbc_options_json, credential_meta_json) " +
            "VALUES (#{storageId}, #{vendor}, #{jdbcUrl}, #{authMethod}, #{host}, #{port}, #{databaseName}, #{serviceName}, #{sid}, #{username}, #{passwordEncrypted}, #{schemaName}, #{jdbcOptionsJson}, #{credentialMetaJson})")
    int insert(ExternalDatabaseStorage storage);

    @Update("UPDATE config.external_database_storage " +
            "SET vendor = #{vendor}, jdbc_url = #{jdbcUrl}, auth_method = #{authMethod}, host = #{host}, port = #{port}, " +
            "database_name = #{databaseName}, service_name = #{serviceName}, sid = #{sid}, username = #{username}, " +
            "password_encrypted = #{passwordEncrypted}, schema_name = #{schemaName}, jdbc_options_json = #{jdbcOptionsJson}, " +
            "credential_meta_json = #{credentialMetaJson} " +
            "WHERE storage_id = #{storageId}")
    int update(ExternalDatabaseStorage storage);

    @Select("SELECT s.id AS storageId, s.name AS name, s.description AS description, s.storage_type AS storageType, s.connection_status AS connectionStatus, s.created_at AS createdAt, s.updated_at AS updatedAt, " +
            "d.vendor, d.jdbc_url AS jdbcUrl, d.auth_method AS authMethod, d.host, d.port, d.database_name AS databaseName, d.service_name AS serviceName, d.sid, d.username, d.password_encrypted AS passwordEncrypted, d.schema_name AS schemaName, d.jdbc_options_json AS jdbcOptionsJson, d.credential_meta_json AS credentialMetaJson " +
            "FROM config.external_storage s " +
            "JOIN config.external_database_storage d ON d.storage_id = s.id " +
            "WHERE s.id = #{storageId}")
    Map<String, Object> findDetail(@Param("storageId") Long storageId);

    @Select("SELECT s.id AS id, s.name AS name, s.description AS description, s.storage_type AS storageType, s.connection_status AS connectionStatus, d.vendor, d.auth_method AS authMethod, d.jdbc_url AS jdbcUrl, d.username, d.schema_name AS schemaName, d.service_name AS serviceName, d.sid, d.created_at AS createdAt " +
            "FROM config.external_storage s " +
            "JOIN config.external_database_storage d ON d.storage_id = s.id " +
            "ORDER BY ${orderBy} LIMIT #{size} OFFSET #{offset}")
    List<Map<String, Object>> findAllSummaries(@Param("size") int size,
                                              @Param("offset") int offset,
                                              @Param("orderBy") String orderBy);

    @Select("SELECT COUNT(1) FROM config.external_storage s " +
            "JOIN config.external_database_storage d ON d.storage_id = s.id")
    long countAll();

    @Delete("DELETE FROM config.external_database_storage WHERE storage_id = #{storageId}")
    int deleteByStorageId(@Param("storageId") Long storageId);

    @Select("SELECT s.id AS id, s.name AS name, s.connection_status AS connectionStatus, d.vendor, d.schema_name AS schemaName, " +
            "d.username, d.service_name AS serviceName, d.sid " +
            "FROM config.external_storage s " +
            "JOIN config.external_database_storage d ON d.storage_id = s.id " +
            "WHERE s.enabled = TRUE " +
            "ORDER BY s.name ASC")
    List<Map<String, Object>> findAllOptions();
}
