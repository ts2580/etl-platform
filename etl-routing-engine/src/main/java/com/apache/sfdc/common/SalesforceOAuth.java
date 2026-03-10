package com.apache.sfdc.common;

import com.etlplatform.common.salesforce.SalesforceOAuthClient;

import java.util.Map;

public class SalesforceOAuth {

    // NOTE: legacy no-arg access (kept for backward compatibility)
    private static final String LOGIN_URL = "";
    private static final String CLIENT_ID = "";
    private static final String CLIENT_SECRET = "";
    private static final String USERNAME = "";
    private static final String PASSWORD = "";

    public static String getAccessToken() throws Exception {
        SalesforceOAuthClient client = new SalesforceOAuthClient();
        return client.passwordGrantAccessToken(LOGIN_URL, CLIENT_ID, CLIENT_SECRET, USERNAME, PASSWORD);
    }

    /**
     * Password grant helper.
     *
     * Expected keys in mapProperty:
     * - loginUrl
     * - client_id
     * - client_secret
     * - username
     * - password
     */
    public static String getAccessToken(Map<String, String> mapProperty) throws Exception {
        SalesforceOAuthClient client = new SalesforceOAuthClient();
        return client.passwordGrantAccessToken(
                mapProperty.get("loginUrl"),
                mapProperty.get("client_id"),
                mapProperty.get("client_secret"),
                mapProperty.get("username"),
                mapProperty.get("password")
        );
    }
}
