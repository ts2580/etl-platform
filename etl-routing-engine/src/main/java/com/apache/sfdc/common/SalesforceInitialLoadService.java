package com.apache.sfdc.common;

import java.util.Map;

public interface SalesforceInitialLoadService {

    int load(String routingProtocol,
             Map<String, String> mapProperty,
             String accessToken,
             String instanceUrl,
             String apiVersion,
             String selectedObject,
             SalesforceObjectSchemaBuilder.SchemaResult schemaResult,
             Long targetStorageId);
}
