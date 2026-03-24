package com.sfdcupload.file.target;

import com.sfdcupload.file.domain.FileUploadResult;
import com.sfdcupload.file.domain.FileUploadStrategy;
import com.sfdcupload.file.domain.MigrationFile;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class SalesforceFileGateway {
    private final SalesforceFileApi salesforceFileApi;

    public FileUploadResult upload(MigrationFile file, FileUploadStrategy strategy, String accessToken, String myDomain) throws Exception {
        return switch (strategy) {
            case CONTENT_VERSION_SINGLE, CONNECT_API -> salesforceFileApi.uploadFile(file, accessToken, myDomain);
            case CONTENT_VERSION_BATCH -> throw new IllegalArgumentException("Use batch upload method for CONTENT_VERSION_BATCH");
        };
    }
}
