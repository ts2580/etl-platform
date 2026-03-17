package com.etlplatform.common.salesforce;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.FormBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import java.io.IOException;

/**
 * Shared Salesforce client credentials helper.
 */
public class SalesforceClientCredentialsClient {

    private final OkHttpClient http;
    private final ObjectMapper objectMapper;

    public SalesforceClientCredentialsClient() {
        this(new OkHttpClient(), new ObjectMapper());
    }

    public SalesforceClientCredentialsClient(OkHttpClient http, ObjectMapper objectMapper) {
        this.http = http;
        this.objectMapper = objectMapper;
    }

    public TokenResponse exchangeClientCredentials(String tokenUrl, String clientId, String clientSecret) throws IOException {
        RequestBody requestBody = new FormBody.Builder()
                .add("grant_type", "client_credentials")
                .add("client_id", clientId)
                .add("client_secret", clientSecret)
                .build();

        Request tokenRequest = new Request.Builder()
                .url(tokenUrl)
                .post(requestBody)
                .build();

        try (Response tokenResponse = http.newCall(tokenRequest).execute()) {
            String body = tokenResponse.body() != null ? tokenResponse.body().string() : "";
            if (!tokenResponse.isSuccessful()) {
                String message = body == null || body.isBlank() ? "(empty)" : body;
                throw new IOException("Salesforce client credentials token request failed: HTTP " + tokenResponse.code() + " body=" + message);
            }

            JsonNode rootNode = objectMapper.readTree(body);
            String accessToken = rootNode.path("access_token").asText(null);
            String instanceUrl = rootNode.path("instance_url").asText(null);
            String idUrl = rootNode.path("id").asText(null);

            return new TokenResponse(accessToken, instanceUrl, idUrl, body);
        }
    }

    public record TokenResponse(
            String accessToken,
            String instanceUrl,
            String id,
            String rawJson
    ) {}
}
