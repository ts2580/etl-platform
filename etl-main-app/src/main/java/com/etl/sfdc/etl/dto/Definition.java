package com.etl.sfdc.etl.dto;

import lombok.Data;

import java.util.ArrayList;

@Data
public class Definition {

    private ObjDef objDef;
    private FieldDef fieldDef;

    public static class ObjDef{
        public ArrayList<Sobjects> sobjects;
    }

    public static class Sobjects{
        public String label;
        public String labelPlural;
        public String name;
    }

    @Data
    public static class FieldDef{
        public ArrayList<Records> records;
        public Integer size;
    }


    public static class Records{
        public String Id;
        public String EntityDefinitionId;
        public String DeveloperName;
        public String QualifiedApiName;
        public String Label;
        public String Length;
        public String DataType;
        public String ValueTypeId;
        public String IsIndexed;
    }
}
