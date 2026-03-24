package com.sfdcupload.file.target;

import com.sfdcupload.file.domain.FileUploadResult;
import com.sfdcupload.file.domain.FileUploadStrategy;
import com.sfdcupload.file.domain.MigrationFile;

import java.io.IOException;
import java.util.List;

public interface FileUploadTarget {
    FileUploadResult upload(MigrationFile file, FileUploadStrategy strategy, String accessToken, String myDomain) throws Exception;

    List<FileUploadResult> uploadBatch(List<MigrationFile> files, String accessToken, String myDomain) throws IOException;
}
