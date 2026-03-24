package com.etl.sfdc.storage.dto;

import lombok.Builder;
import lombok.Getter;

import java.util.List;

@Getter
@Builder
public class DatabaseStorageListPageResponse {
    private final int page;
    private final int size;
    private final long totalElements;
    private final int totalPages;
    private final List<DatabaseStorageListResponse> items;
}
