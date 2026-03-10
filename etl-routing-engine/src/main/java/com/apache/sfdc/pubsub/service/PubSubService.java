package com.apache.sfdc.pubsub.service;

import java.util.Map;

public interface PubSubService {
    Map<String, Object> setTable(Map<String, String> mapProperty, String token);

    void subscribeCDC(Map<String, String> mapProperty) throws Exception;
}
