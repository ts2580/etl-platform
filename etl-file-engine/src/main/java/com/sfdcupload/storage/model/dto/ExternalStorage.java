package com.sfdcupload.storage.model.dto;

import com.etlplatform.common.storage.database.StorageType;
import lombok.Getter;
import lombok.Setter;

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
    private String createdBy;
    private String updatedBy;
}
