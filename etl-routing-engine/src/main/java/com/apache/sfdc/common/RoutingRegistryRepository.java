package com.apache.sfdc.common;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;
import java.util.Map;

@Mapper
public interface RoutingRegistryRepository {

    void upsertRoutingRegistry(Map<String, Object> params);

    void insertRoutingHistory(Map<String, Object> params);

    List<Map<String, Object>> findActiveRoutes();

    List<Map<String, Object>> findActiveRoutesByOrg(@Param("orgKey") String orgKey);

    List<Map<String, Object>> findActiveOrgs();

    List<Map<String, Object>> findActiveOrgsByOrg(@Param("orgKey") String orgKey);

    void markRoutingReleased(@Param("orgKey") String orgKey,
                             @Param("selectedObject") String selectedObject,
                             @Param("routingProtocol") String routingProtocol,
                             @Param("message") String message,
                             @Param("updatedBy") String updatedBy);

    void markRoutingFailed(@Param("orgKey") String orgKey,
                           @Param("selectedObject") String selectedObject,
                           @Param("routingProtocol") String routingProtocol,
                           @Param("message") String message,
                           @Param("updatedBy") String updatedBy);

    int countActiveSlots(@Param("routingProtocol") String routingProtocol);

    void deactivateSlotsByObject(@Param("orgKey") String orgKey,
                                @Param("routingProtocol") String routingProtocol,
                                @Param("selectedObject") String selectedObject);

    void insertSlot(@Param("orgKey") String orgKey,
                   @Param("selectedObject") String selectedObject,
                   @Param("routingProtocol") String routingProtocol,
                   @Param("routingRegistryId") Long routingRegistryId,
                   @Param("note") String note);

    List<Map<String, Object>> findActiveSlots(@Param("orgKey") String orgKey,
                                              @Param("routingProtocol") String routingProtocol);
}
