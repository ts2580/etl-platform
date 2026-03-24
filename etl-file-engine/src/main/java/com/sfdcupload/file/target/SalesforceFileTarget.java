package com.sfdcupload.file.target;

import com.sfdcupload.file.domain.FileUploadResult;
import com.sfdcupload.file.domain.FileUploadStrategy;
import com.sfdcupload.file.domain.MigrationFile;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;

@Component
@RequiredArgsConstructor
public class SalesforceFileTarget implements FileUploadTarget {
    private final SalesforceFileGateway salesforceFileGateway;
    private final SalesforceFileApi salesforceFileApi;

    @Override
    public FileUploadResult upload(MigrationFile file, FileUploadStrategy strategy, String accessToken, String myDomain) throws Exception {
        return salesforceFileGateway.upload(file, strategy, accessToken, myDomain);
    }

    @Override
    public List<FileUploadResult> uploadBatch(List<MigrationFile> files, String accessToken, String myDomain) throws IOException {
        return salesforceFileApi.uploadBatch(files, accessToken, myDomain);
    }
}
