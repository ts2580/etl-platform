package com.apache.sfdc.common;

import com.etlplatform.common.error.AppException;
import com.etlplatform.common.salesforce.SalesforceOAuthClient;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class SalesforceOAuth {

    private final SalesforceOAuthClient client;

    public SalesforceOAuth(SalesforceOAuthClient client) {
        this.client = client;
    }

    public String getAccessToken(Map<String, String> mapProperty) throws Exception {
        if (mapProperty == null) {
            throw new AppException("Salesforce OAuth property map is required");
        }

        String loginUrl = mapProperty.get("loginUrl");
        String clientId = mapProperty.get("client_id");
        String clientSecret = mapProperty.get("client_secret");
        String username = mapProperty.get("username");
        String password = mapProperty.get("password");

        if (loginUrl == null || clientId == null || clientSecret == null || username == null || password == null) {
            throw new AppException("Salesforce OAuth property map is missing required keys");
        }

        return client.passwordGrantAccessToken(loginUrl, clientId, clientSecret, username, password);
    }
}
