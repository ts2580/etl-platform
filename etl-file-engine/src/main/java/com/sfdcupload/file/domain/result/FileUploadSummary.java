package com.sfdcupload.file.domain.result;

import lombok.Builder;
import lombok.Getter;

import java.util.List;

@Getter
@Builder
public class FileUploadSummary {
    private final String uploadRoot;
    private final int totalCount;
    private final int successCount;
    private final int failureCount;
    private final List<FileUploadItemResult> items;
    private final String historyFile;
}
