package com.etlplatform.common.salesforce;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.*;

import java.io.IOException;
import java.util.Objects;

/**
 * Shared Salesforce OAuth helper.
 *
 * Supports:
 * - Authorization Code Grant (code -> access/refresh token)
 * - Password Grant (username/password -> access token)
 */
public class SalesforceOAuthClient {

    private final OkHttpClient http;
    private final ObjectMapper objectMapper;

    public SalesforceOAuthClient() {
        this(new OkHttpClient(), new ObjectMapper());
    }

    public SalesforceOAuthClient(OkHttpClient http, ObjectMapper objectMapper) {
        this.http = http;
        this.objectMapper = objectMapper;
    }

    public String buildLoginRedirectUrl(String authUrl, String clientId, String redirectUri) {
        return authUrl
                + "?response_type=code"
                + "&client_id=" + clientId
                + "&redirect_uri=" + redirectUri;
    }

    public TokenResponse exchangeAuthorizationCode(
            String tokenUrl,
            String code,
            String clientId,
            String clientSecret,
            String redirectUri
    ) throws IOException {

        RequestBody requestBody = new FormBody.Builder()
                .add("grant_type", "authorization_code")
                .add("code", code)
                .add("client_id", clientId)
                .add("client_secret", clientSecret)
                .add("redirect_uri", redirectUri)
                .build();

        Request tokenRequest = new Request.Builder()
                .url(tokenUrl)
                .post(requestBody)
                .build();

        try (Response tokenResponse = http.newCall(tokenRequest).execute()) {
            if (!tokenResponse.isSuccessful()) {
                throw new IOException("Salesforce token request failed: HTTP " + tokenResponse.code());
            }

            String body = Objects.requireNonNull(tokenResponse.body()).string();
            JsonNode rootNode = objectMapper.readTree(body);

            String accessToken = rootNode.path("access_token").asText(null);
            String refreshToken = rootNode.path("refresh_token").asText(null);
            String instanceUrl = rootNode.path("instance_url").asText(null);

            return new TokenResponse(accessToken, refreshToken, instanceUrl, body);
        }
    }

    public String passwordGrantAccessToken(
            String loginUrl,
            String clientId,
            String clientSecret,
            String username,
            String password
    ) throws IOException {

        RequestBody formBody = new FormBody.Builder()
                .addEncoded("grant_type", "password")
                .addEncoded("client_id", clientId)
                .addEncoded("client_secret", clientSecret)
                .addEncoded("username", username)
                .addEncoded("password", password)
                .build();

        Request request = new Request.Builder()
                .url(loginUrl + "/services/oauth2/token")
                .method("POST", formBody)
                .addHeader("Content-Type", "application/x-www-form-urlencoded")
                .build();

        try (Response response = http.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new IOException("Salesforce password-grant token request failed: HTTP " + response.code());
            }
            JsonNode rootNode = objectMapper.readTree(Objects.requireNonNull(response.body()).string());
            return rootNode.path("access_token").asText(null);
        }
    }

    public record TokenResponse(
            String accessToken,
            String refreshToken,
            String instanceUrl,
            String rawJson
    ) {}
}
