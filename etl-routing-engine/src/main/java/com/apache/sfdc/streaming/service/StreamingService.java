package com.apache.sfdc.streaming.service;

import java.util.Map;

public interface StreamingService {

    Map<String, Object> setTable(Map<String, String> mapProperty, String token);

    String setPushTopic(Map<String, String> mapProperty, Map<String, Object> mapReturn, String token) throws Exception;

    void subscribePushTopic(Map<String, String> mapProperty, String token, Map<String, Object> mapType) throws Exception;
}
