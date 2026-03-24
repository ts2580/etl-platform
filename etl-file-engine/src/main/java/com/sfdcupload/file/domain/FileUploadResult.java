package com.sfdcupload.file.domain;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class FileUploadResult {
    private final boolean success;
    private final FileUploadStrategy strategy;
    private final String contentVersionId;
    private final String contentDocumentId;
    private final String message;
}
