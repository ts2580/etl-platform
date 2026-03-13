package com.apache.sfdc.pubsub.repository;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

@Mapper
public interface PubSubRepository {

    void setTable(String string);

    int insertObject(@Param("upperQuery") String upperQuery, @Param("listUnderQuery") List<String> listUnderQuery);

    int updateObject(@Param("strUpdate") StringBuilder strUpdate);

    int deleteObject(@Param("selectedObject") String selectedObject, @Param("listDeleteIds") List<String> listDeleteIds);

}
