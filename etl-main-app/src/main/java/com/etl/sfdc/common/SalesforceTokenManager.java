package com.etl.sfdc.common;

import com.etlplatform.common.salesforce.SalesforceOAuthClient;
import jakarta.servlet.http.HttpSession;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Component
@RequiredArgsConstructor
@Slf4j
public class SalesforceTokenManager {
    public static final String ACCESS_TOKEN = "SALESFORCE_ACCESS_TOKEN";
    public static final String REFRESH_TOKEN = "SALESFORCE_REFRESH_TOKEN";
    public static final String TOKEN_ISSUED_AT = "SALESFORCE_TOKEN_ISSUED_AT";

    private final SalesforceOAuthClient oauthClient;

    @Value("${salesforce.access-token-ttl-minutes:55}")
    private long accessTokenTtlMinutes;

    @Value("${salesforce.clientId}")
    private String clientId;

    @Value("${salesforce.clientSecret}")
    private String clientSecret;

    @Value("${salesforce.tokenUrl}")
    private String tokenUrl;

    public void setTokenPair(HttpSession session, String accessToken, String refreshToken) {
        session.setAttribute(ACCESS_TOKEN, accessToken);
        session.setAttribute(REFRESH_TOKEN, refreshToken);
        session.setAttribute(TOKEN_ISSUED_AT, Instant.now().toEpochMilli());
    }

    public String getAccessToken(HttpSession session) {
        Long issuedAt = (Long) session.getAttribute(TOKEN_ISSUED_AT);
        if (issuedAt == null) {
            return null;
        }

        if (isExpired(issuedAt)) {
            clear(session);
            return null;
        }

        return (String) session.getAttribute(ACCESS_TOKEN);
    }

    public String getRefreshToken(HttpSession session) {
        return (String) session.getAttribute(REFRESH_TOKEN);
    }

    public String refreshAccessToken(HttpSession session) {
        String refreshToken = getRefreshToken(session);
        if (refreshToken == null || refreshToken.isBlank()) {
            return null;
        }

        try {
            SalesforceOAuthClient.TokenResponse token = oauthClient.exchangeRefreshToken(
                    tokenUrl,
                    refreshToken,
                    clientId,
                    clientSecret
            );

            String nextRefreshToken = token.refreshToken() == null || token.refreshToken().isBlank()
                    ? refreshToken
                    : token.refreshToken();
            setTokenPair(session, token.accessToken(), nextRefreshToken);
            log.info("Salesforce access token refreshed successfully");
            return token.accessToken();
        } catch (Exception e) {
            log.warn("Failed to refresh Salesforce access token: {}", e.getMessage());
            clear(session);
            return null;
        }
    }

    private boolean isExpired(Long issuedAt) {
        long ttlMillis = accessTokenTtlMinutes * 60_000L;
        return System.currentTimeMillis() - issuedAt > ttlMillis;
    }

    public void clear(HttpSession session) {
        session.removeAttribute(ACCESS_TOKEN);
        session.removeAttribute(REFRESH_TOKEN);
        session.removeAttribute(TOKEN_ISSUED_AT);
    }
}
