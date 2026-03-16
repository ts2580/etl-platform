package com.apache.sfdc.pubsub.repository;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

@Mapper
public interface PubSubRepository {

    void setTable(String string);

    int insertObject(@Param("upperQuery") String upperQuery, @Param("listUnderQuery") List<String> listUnderQuery, @Param("tailQuery") String tailQuery);

    int updateObject(@Param("strUpdate") StringBuilder strUpdate);

    int deleteObject(@Param("strDelete") StringBuilder strDelete);

    int countActiveSlots(@Param("routingProtocol") String routingProtocol);

    void upsertActiveSlot(@Param("orgKey") String orgKey,
                         @Param("selectedObject") String selectedObject,
                         @Param("routingProtocol") String routingProtocol,
                         @Param("routingRegistryId") Long routingRegistryId,
                         @Param("note") String note);

    void deactivateSlot(@Param("routingProtocol") String routingProtocol, @Param("selectedObject") String selectedObject);

}
