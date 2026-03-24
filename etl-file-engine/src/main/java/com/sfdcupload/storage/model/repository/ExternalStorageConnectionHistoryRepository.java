package com.sfdcupload.storage.model.repository;

import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface ExternalStorageConnectionHistoryRepository {

    @Insert("INSERT INTO config.external_storage_connection_history (storage_id, event_type, success, message, detail_text) " +
            "VALUES (#{storageId}, #{eventType}, #{success}, #{message}, #{detailText})")
    int insert(@Param("storageId") Long storageId,
               @Param("eventType") String eventType,
               @Param("success") boolean success,
               @Param("message") String message,
               @Param("detailText") String detailText);
}
