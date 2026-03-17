package com.etl.sfdc.etl.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class ObjectDefinition {
    public String label;
    public String labelPlural;
    public String name;

    public String getLabel() {
        if (label == null || label.isBlank() || label.startsWith("__MISSING LABEL__")) {
            return name;
        }
        return label;
    }
}
