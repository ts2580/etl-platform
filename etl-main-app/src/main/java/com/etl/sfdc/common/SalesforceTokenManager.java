package com.etl.sfdc.common;

import jakarta.servlet.http.HttpSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Component
public class SalesforceTokenManager {
    public static final String ACCESS_TOKEN = "SALESFORCE_ACCESS_TOKEN";
    public static final String REFRESH_TOKEN = "SALESFORCE_REFRESH_TOKEN";
    public static final String TOKEN_ISSUED_AT = "SALESFORCE_TOKEN_ISSUED_AT";

    @Value("${salesforce.access-token-ttl-minutes:55}")
    private long accessTokenTtlMinutes;

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
