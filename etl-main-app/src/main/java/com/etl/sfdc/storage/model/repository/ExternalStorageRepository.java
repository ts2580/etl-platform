package com.etl.sfdc.storage.model.repository;

import com.etl.sfdc.storage.model.dto.ExternalStorage;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Options;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Update;

@Mapper
public interface ExternalStorageRepository {

    @Insert("INSERT INTO config.external_storage (org_key, storage_type, name, description, enabled, connection_status, created_by, updated_by) " +
            "VALUES (#{orgKey}, #{storageType}, #{name}, #{description}, #{enabled}, #{connectionStatus}, #{createdBy}, #{updatedBy})")
    @Options(useGeneratedKeys = true, keyProperty = "id")
    int insert(ExternalStorage storage);

    @Update("UPDATE config.external_storage " +
            "SET name = #{name}, description = #{description}, connection_status = #{connectionStatus}, updated_by = #{updatedBy} " +
            "WHERE id = #{id}")
    int updateEditableFields(@Param("id") Long id,
                             @Param("name") String name,
                             @Param("description") String description,
                             @Param("connectionStatus") String connectionStatus,
                             @Param("updatedBy") String updatedBy);

    @Update("UPDATE config.external_storage " +
            "SET connection_status = #{connectionStatus}, last_tested_at = CURRENT_TIMESTAMP, " +
            "last_test_success = #{lastTestSuccess}, last_error_message = #{lastErrorMessage}, updated_by = #{updatedBy} " +
            "WHERE id = #{id}")
    int updateConnectionStatus(@Param("id") Long id,
                               @Param("connectionStatus") String connectionStatus,
                               @Param("lastTestSuccess") Boolean lastTestSuccess,
                               @Param("lastErrorMessage") String lastErrorMessage,
                               @Param("updatedBy") String updatedBy);

    @Delete("DELETE FROM config.external_storage WHERE id = #{id}")
    int deleteById(@Param("id") Long id);
}
