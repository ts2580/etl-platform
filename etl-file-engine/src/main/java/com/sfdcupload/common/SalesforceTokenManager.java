package com.sfdcupload.common;

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
    public static final String TOKEN_ISSUED_AT = "SALESFORCE_TOKEN_ISSUED_AT";

    private final SalesforceOAuthClient oauthClient;

    @Value("${salesforce.access-token-ttl-minutes:55}")
    private long accessTokenTtlMinutes;

    @Value("${salesforce.tokenUrl:https://login.salesforce.com/services/oauth2/token}")
    private String defaultTokenUrl;

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

    public void setActiveOrg(HttpSession session, String orgKey) {
        if (orgKey == null || orgKey.isBlank()) {
            session.removeAttribute(ACTIVE_ORG_KEY);
            return;
        }
        session.setAttribute(ACTIVE_ORG_KEY, orgKey);
    }

    public String getActiveOrgKey(HttpSession session) {
        Object value = session.getAttribute(ACTIVE_ORG_KEY);
        return value == null ? null : String.valueOf(value);
    }

    public String refreshAccessToken(HttpSession session, SalesforceOrgCredential orgCredential) {
        if (orgCredential == null
                || orgCredential.getRefreshToken() == null || orgCredential.getRefreshToken().isBlank()
                || orgCredential.getClientId() == null || orgCredential.getClientId().isBlank()
                || orgCredential.getClientSecret() == null || orgCredential.getClientSecret().isBlank()) {
            return null;
        }
        try {
            SalesforceOAuthClient.TokenResponse token = oauthClient.exchangeRefreshToken(
                    defaultTokenUrl,
                    orgCredential.getRefreshToken(),
                    orgCredential.getClientId(),
                    orgCredential.getClientSecret()
            );
            setAccessToken(session, token.accessToken());
            return token.accessToken();
        } catch (Exception e) {
            log.warn("Failed to refresh Salesforce access token for file module org {}: {}", orgCredential.getOrgKey(), e.getMessage());
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
        session.removeAttribute(TOKEN_ISSUED_AT);
        session.removeAttribute(ACTIVE_ORG_KEY);
    }
}
