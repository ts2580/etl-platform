package com.etl.sfdc.etl.service;

import com.etl.sfdc.etl.dto.ObjectDefinition;

import java.util.List;
import java.util.Map;

public interface ETLService {

    List<ObjectDefinition> getObjects(String accessToken) throws Exception;

    Map<String, Object> getCdcSlotSummary();

    Map<String, Object> setObjects(String selectedObject, String ingestionMode, String accessToken, String refreshToken) throws Exception;
}