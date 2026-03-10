package com.etl.sfdc.config.model.dto;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;


@Data
public class ParamVo {

    // Database에 테이블 DDL 하기 위한 공용 VO

    private List<String> listUnderDML = new ArrayList<>();
    private String upperDML;
}
