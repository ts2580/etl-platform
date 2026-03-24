package com.sfdcupload.storage.model.repository;

import com.sfdcupload.storage.model.dto.ExternalStorage;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Options;

@Mapper
public interface ExternalStorageRepository {

    @Insert("INSERT INTO config.external_storage (org_key, storage_type, name, description, enabled, connection_status, created_by, updated_by) " +
            "VALUES (#{orgKey}, #{storageType}, #{name}, #{description}, #{enabled}, #{connectionStatus}, #{createdBy}, #{updatedBy})")
    @Options(useGeneratedKeys = true, keyProperty = "id")
    int insert(ExternalStorage storage);
}
