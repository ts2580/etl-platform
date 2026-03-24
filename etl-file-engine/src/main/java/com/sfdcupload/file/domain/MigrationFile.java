package com.sfdcupload.file.domain;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder(toBuilder = true)
public class MigrationFile {
    private final String sourceId;
    private final String fileName;
    private final byte[] content;
    private final long size;

    public boolean isEmpty() {
        return content == null || content.length == 0;
    }
}
