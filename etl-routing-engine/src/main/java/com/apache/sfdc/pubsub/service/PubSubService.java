package com.apache.sfdc.pubsub.service;

import java.util.Map;

public interface PubSubService {
    Map<String, Object> createCdcChannel(Map<String, String> mapProperty, String token) throws Exception;

    Map<String, Object> setTable(Map<String, String> mapProperty, String token);

    void subscribeCDC(Map<String, String> mapProperty, Map<String, Object> mapType) throws Exception;

    void markCdcSlotActive(String selectedObject);

    Map<String, Object> getCdcSlotSummary();
}
