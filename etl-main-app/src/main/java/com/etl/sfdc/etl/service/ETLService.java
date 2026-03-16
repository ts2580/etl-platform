package com.etl.sfdc.etl.service;

import com.etl.sfdc.etl.dto.ObjectDefinition;

import java.util.List;
import java.util.Map;

public interface ETLService {

    List<ObjectDefinition> getObjects(String accessToken, String myDomain) throws Exception;

    Map<String, Object> getCdcSlotSummary();

    Map<String, String> getIngestionStatusByObject(String accessToken, String myDomain) throws Exception;

    void syncRoutingRegistryFromSalesforce(String accessToken, String actor, String myDomain) throws Exception;

    Map<String, Object> getRoutingDashboard(String orgKey);

    Map<String, Object> getRouteDetail(String accessToken, String myDomain, String orgKey, String selectedObject, String routingProtocol) throws Exception;

    Map<String, Object> setObjects(String selectedObject, String ingestionMode, String accessToken, String refreshToken, String actor, String myDomain, String orgKey, String orgName) throws Exception;

    Map<String, Object> releaseObject(String selectedObject, String ingestionMode, String accessToken, String actor, String myDomain, String orgKey) throws Exception;
}
