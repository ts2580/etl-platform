package com.etl.sfdc.storage.model.dto;

import com.etlplatform.common.storage.database.StorageType;
import lombok.Getter;
import lombok.Setter;

import java.time.LocalDateTime;

@Getter
@Setter
public class ExternalStorage {
    private Long id;
    private String orgKey;
    private StorageType storageType;
    private String name;
    private String description;
    private Boolean enabled;
    private String connectionStatus;
    private LocalDateTime lastTestedAt;
    private Boolean lastTestSuccess;
    private String lastErrorMessage;
    private String createdBy;
    private String updatedBy;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
}
