package com.apache.sfdc.streaming.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class FieldDefinition {
    public String  name;             // 필드 API
    public String  type;             // 필드 라벨
    public String  label;            // type
    public Integer length;           // Text 계열 필드일 때 길이
    public String  precision;        // Number일 때 길이
    public String  scale;            // Number일 때 소숫점 자릿수
}
