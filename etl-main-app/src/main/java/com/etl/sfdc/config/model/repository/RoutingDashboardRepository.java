package com.etl.sfdc.config.model.repository;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;
import java.util.Map;

@Mapper
public interface RoutingDashboardRepository {

    List<Map<String, Object>> findActiveRoutes();

    List<Map<String, Object>> findActiveRoutesByOrg(@Param("orgKey") String orgKey);

    List<Map<String, Object>> findActiveOrgs();

    List<Map<String, Object>> findActiveOrgsByOrg(@Param("orgKey") String orgKey);

    Map<String, Object> findCdcSlotSummary();

    Map<String, Object> findRouteDetail(@Param("orgKey") String orgKey,
                                        @Param("selectedObject") String selectedObject,
                                        @Param("routingProtocol") String routingProtocol);

    Integer countObjectRows(@Param("schemaName") String schemaName, @Param("tableName") String tableName);

    void upsertRoutingRegistry(Map<String, Object> params);

    void deactivateRoutesNotInSync(@Param("orgKey") String orgKey, @Param("myDomain") String myDomain, @Param("syncedAt") String syncedAt);

    void markRoutingReleased(@Param("orgKey") String orgKey,
                             @Param("selectedObject") String selectedObject,
                             @Param("routingProtocol") String routingProtocol,
                             @Param("message") String message,
                             @Param("updatedBy") String updatedBy);

    void deactivateSlotByObject(@Param("orgKey") String orgKey,
                                @Param("selectedObject") String selectedObject,
                                @Param("routingProtocol") String routingProtocol,
                                @Param("note") String note);

    void insertRoutingHistory(Map<String, Object> params);
}
