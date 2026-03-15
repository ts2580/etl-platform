package com.apache.sfdc.streaming.repository;


import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

@Mapper
public interface StreamingRepository {
    void setFieldDef(String ddl);

    int insertObject(@Param("upperQuery") String upperQuery, @Param("listUnderQuery") List<String> listUnderQuery, @Param("tailQuery") String tailQuery);

    void setTable(String string);

    int deleteObject(@Param("strDelete") StringBuilder strDelete);

    int updateObject(@Param("strUpdate")StringBuilder strUpdate);
}
