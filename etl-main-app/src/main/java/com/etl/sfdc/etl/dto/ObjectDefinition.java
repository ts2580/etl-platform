package com.etl.sfdc.etl.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class ObjectDefinition {
    public String label;
    public String labelPlural;
    public String name;
}
