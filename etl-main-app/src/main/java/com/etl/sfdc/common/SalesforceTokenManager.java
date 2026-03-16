package com.etl.sfdc.common;

import com.etl.sfdc.config.model.dto.SalesforceOrgCredential;
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
    public static final String ACTIVE_ORG_KEY = "ACTIVE_SALESFORCE_ORG_KEY";
    public static final String ACCESS_TOKEN = "SALESFORCE_ACCESS_TOKEN";
    public static final String REFRESH_TOKEN = "SALESFORCE_REFRESH_TOKEN";
    public static final String TOKEN_ISSUED_AT = "SALESFORCE_TOKEN_ISSUED_AT";

    private final SalesforceOAuthClient oauthClient;

    @Value("${salesforce.access-token-ttl-minutes:55}")
    private long accessTokenTtlMinutes;

    @Value("${salesforce.tokenUrl:https://login.salesforce.com/services/oauth2/token}")
    private String defaultTokenUrl;

    public void setTokenPair(HttpSession session, String accessToken, String refreshToken) {
        session.setAttribute(ACCESS_TOKEN, accessToken);
        session.setAttribute(REFRESH_TOKEN, refreshToken);
        session.setAttribute(TOKEN_ISSUED_AT, Instant.now().toEpochMilli());
    }

    public void setAccessToken(HttpSession session, String accessToken) {
        session.setAttribute(ACCESS_TOKEN, accessToken);
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

    public void setActiveOrg(HttpSession session, String orgKey) {
        if (orgKey == null || orgKey.isBlank()) {
            session.removeAttribute(ACTIVE_ORG_KEY);
            return;
        }
        session.setAttribute(ACTIVE_ORG_KEY, orgKey);
    }

    public String getActiveOrgKey(HttpSession session) {
        Object v = session.getAttribute(ACTIVE_ORG_KEY);
        return v == null ? null : String.valueOf(v);
    }

    public String refreshAccessToken(HttpSession session, String refreshToken, String clientId, String clientSecret) {
        return refreshAccessToken(session, refreshToken, clientId, clientSecret, null);
    }

    public String refreshAccessToken(HttpSession session, SalesforceOrgCredential orgCredential) {
        RefreshResult result = refreshTokenPair(session, orgCredential);
        return result == null ? null : result.accessToken();
    }

    public RefreshResult refreshTokenPair(HttpSession session, SalesforceOrgCredential orgCredential) {
        if (orgCredential == null) {
            return null;
        }
        return refreshTokenPair(
                session,
                orgCredential.getRefreshToken(),
                orgCredential.getClientId(),
                orgCredential.getClientSecret(),
                orgCredential.getOrgKey()
        );
    }

    public String refreshAccessToken(HttpSession session, String refreshToken, String clientId, String clientSecret, String orgKey) {
        RefreshResult result = refreshTokenPair(session, refreshToken, clientId, clientSecret, orgKey);
        return result == null ? null : result.accessToken();
    }

    public RefreshResult refreshTokenPair(HttpSession session, String refreshToken, String clientId, String clientSecret, String orgKey) {
        if (refreshToken == null || refreshToken.isBlank() || clientId == null || clientId.isBlank() || clientSecret == null || clientSecret.isBlank()) {
            return null;
        }
        try {
            SalesforceOAuthClient.TokenResponse token = oauthClient.exchangeRefreshToken(
                    defaultTokenUrl,
                    refreshToken,
                    clientId,
                    clientSecret
            );

            String nextRefreshToken = token.refreshToken() == null || token.refreshToken().isBlank()
                    ? refreshToken
                    : token.refreshToken();
            setTokenPair(session, token.accessToken(), nextRefreshToken);
            if (orgKey != null && !orgKey.isBlank()) {
                log.info("Salesforce token pair refreshed for org {}", orgKey);
            }
            return new RefreshResult(token.accessToken(), nextRefreshToken);
        } catch (Exception e) {
            log.warn("Failed to refresh Salesforce access token for org {}: {}", orgKey, e.getMessage());
            clear(session);
            return null;
        }
    }

    public String refreshAccessToken(HttpSession session) {
        return refreshAccessToken(session, getRefreshToken(session), null, null);
    }

    private boolean isExpired(Long issuedAt) {
        long ttlMillis = accessTokenTtlMinutes * 60_000L;
        return System.currentTimeMillis() - issuedAt > ttlMillis;
    }

    public void clear(HttpSession session) {
        session.removeAttribute(ACCESS_TOKEN);
        session.removeAttribute(REFRESH_TOKEN);
        session.removeAttribute(TOKEN_ISSUED_AT);
        session.removeAttribute(ACTIVE_ORG_KEY);
    }

    public record RefreshResult(String accessToken, String refreshToken) {}
}
