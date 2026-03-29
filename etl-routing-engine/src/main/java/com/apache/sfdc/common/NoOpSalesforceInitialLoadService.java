package com.apache.sfdc.common;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
@ConditionalOnProperty(name = "app.db.enabled", havingValue = "false", matchIfMissing = true)
public class NoOpSalesforceInitialLoadService implements SalesforceInitialLoadService {

    @Override
    public int load(String routingProtocol,
                    Map<String, String> mapProperty,
                    String accessToken,
                    String instanceUrl,
                    String apiVersion,
                    String selectedObject,
                    SalesforceObjectSchemaBuilder.SchemaResult schemaResult,
                    Long targetStorageId) {
        return 0;
    }
}
