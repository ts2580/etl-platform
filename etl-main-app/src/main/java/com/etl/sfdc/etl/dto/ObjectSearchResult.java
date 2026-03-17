package com.etl.sfdc.etl.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ObjectSearchResult {
    private List<ObjectDefinition> objects;
    private int totalCount;
    private int currentPage;
    private int pageSize;
    private boolean exactCount;
}
