package com.sfdcupload.file.domain.result;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class FileUploadItemResult {
    private final String sourceId;
    private final String fileName;
    private final boolean success;
    private final String contentVersionId;
    private final String contentDocumentId;
    private final String message;
}
