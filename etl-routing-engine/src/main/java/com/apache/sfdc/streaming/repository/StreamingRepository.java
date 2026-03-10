package com.apache.sfdc.streaming.repository;


import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

@Mapper
public interface StreamingRepository {
    void setFieldDef(String ddl);

    int insertObject(@Param("upperQuery") String upperQuery, @Param("listUnderQuery") List<String> listUnderQuery);

    void setTable(String string);

    int deleteObject(@Param("selectedObject")String selectedObject, @Param("listDeleteIds") List<String> listDeleteIds);

    int updateObject(@Param("strUpdate")StringBuilder strUpdate);
}
