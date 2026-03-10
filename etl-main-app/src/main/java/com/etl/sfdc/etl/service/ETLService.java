package com.etl.sfdc.etl.service;

import com.etl.sfdc.etl.dto.ObjectDefinition;

import java.util.List;

public interface ETLService {

    List<ObjectDefinition> getObjects(String accessToken) throws Exception;

    void setObjects(String selectedObject, String accessToken, String refreshToken) throws Exception;
}