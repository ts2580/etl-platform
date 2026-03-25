package com.apache.sfdc.pubsub.service;

import java.util.Map;

public interface PubSubService {
    Map<String, Object> createCdcChannel(Map<String, String> mapProperty, String token) throws Exception;

    Map<String, Object> setTable(Map<String, String> mapProperty, String token);

    void dropTable(Map<String, String> mapProperty);

    void stopRoute(String orgKey, String selectedObject, String reason);

    void subscribeCDC(Map<String, String> mapProperty, Map<String, Object> mapType) throws Exception;

    void markSlotActive(String selectedObject, String ingestionProtocol, String orgKey, Long routingRegistryId);

    void deactivateSlot(String orgKey, String selectedObject, String ingestionProtocol);

    Map<String, Object> getSlotSummary(String ingestionProtocol);

    Map<String, Object> refreshCredentials(Map<String, String> mapProperty) throws Exception;

    Map<String, Object> restartRoutesForOrg(String orgKey, String reason) throws Exception;

    boolean isRouteActive(String orgKey, String selectedObject);
}
