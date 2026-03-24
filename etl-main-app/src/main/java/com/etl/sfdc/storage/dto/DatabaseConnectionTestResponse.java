package com.etl.sfdc.storage.dto;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class DatabaseConnectionTestResponse {
    private final boolean success;
    private final String message;
}
