package com.etl.sfdc.common;

import com.etl.sfdc.config.model.dto.SalesforceOrgCredential;
import com.etlplatform.common.error.AppException;
import com.etlplatform.common.salesforce.SalesforceClientCredentialsClient;
import jakarta.servlet.http.HttpSession;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.time.Instant;

@Component
@RequiredArgsConstructor
@Slf4j
public class SalesforceTokenManager {
    public static final String ACTIVE_ORG_KEY = "ACTIVE_SALESFORCE_ORG_KEY";
    public static final String ACCESS_TOKEN = "SALESFORCE_ACCESS_TOKEN";
    public static final String TOKEN_ISSUED_AT = "SALESFORCE_TOKEN_ISSUED_AT";

    private final SalesforceClientCredentialsClient oauthClient;

    @Value("${salesforce.access-token-ttl-minutes:55}")
    private long accessTokenTtlMinutes;

    @Value("${salesforce.tokenUrl:/services/oauth2/token}")
    private String defaultTokenUrl;

    public void setAccessToken(HttpSession session, String accessToken) {
        session.setAttribute(ACCESS_TOKEN, accessToken);
        session.setAttribute(TOKEN_ISSUED_AT, Instant.now().toEpochMilli());
    }

    public String getAccessToken(HttpSession session) {
        Long issuedAt = (Long) session.getAttribute(TOKEN_ISSUED_AT);
        if (issuedAt == null) {
            return (String) session.getAttribute(ACCESS_TOKEN);
        }

        if (isExpired(issuedAt)) {
            clearAccessToken(session);
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
        Object v = session.getAttribute(ACTIVE_ORG_KEY);
        return v == null ? null : String.valueOf(v);
    }

    public String refreshAccessToken(HttpSession session, SalesforceOrgCredential orgCredential) {
        RefreshResult result = refreshClientCredentialsToken(session, orgCredential);
        return result == null ? null : result.accessToken();
    }

    public RefreshResult refreshClientCredentialsToken(HttpSession session, SalesforceOrgCredential orgCredential) {
        if (orgCredential == null) {
            return null;
        }
        return refreshClientCredentialsToken(
                session,
                orgCredential.getClientId(),
                orgCredential.getClientSecret(),
                orgCredential.getMyDomain(),
                orgCredential.getOrgKey()
        );
    }

    public RefreshResult refreshClientCredentialsToken(HttpSession session, String clientId, String clientSecret, String myDomain, String orgKey) {
        if (clientId == null || clientId.isBlank() || clientSecret == null || clientSecret.isBlank()) {
            return null;
        }
        try {
            String tokenUrl = resolveTokenUrl(myDomain);
            SalesforceClientCredentialsClient.TokenResponse token = oauthClient.exchangeClientCredentials(
                    tokenUrl,
                    clientId,
                    clientSecret
            );

            setAccessToken(session, token.accessToken());
            if (orgKey != null && !orgKey.isBlank()) {
                log.info("[토큰 만료] Salesforce 접근 토큰 갱신 완료: org {}", orgKey);
            }
            return new RefreshResult(token.accessToken());
        } catch (Exception e) {
            log.warn("[토큰 만료] 토큰 갱신 실패: org {}, tokenUrl={}, reason={}, body={}",
                    orgKey,
                    safeValue(resolveTokenUrl(myDomain)),
                    safeValue(e.getMessage()),
                    extractFailureBody(e));
            clearAccessToken(session);
            return null;
        }
    }

    private boolean isExpired(Long issuedAt) {
        long ttlMillis = accessTokenTtlMinutes * 60_000L;
        return System.currentTimeMillis() - issuedAt > ttlMillis;
    }

    public void clearAccessToken(HttpSession session) {
        session.removeAttribute(ACCESS_TOKEN);
        session.removeAttribute(TOKEN_ISSUED_AT);
    }

    public void clear(HttpSession session) {
        session.removeAttribute(ACCESS_TOKEN);
        session.removeAttribute(TOKEN_ISSUED_AT);
        session.removeAttribute(ACTIVE_ORG_KEY);
    }

    private String extractFailureBody(Exception e) {
        String message = e == null ? null : e.getMessage();
        if (message == null || message.isBlank()) {
            return "-";
        }

        int bodyIndex = message.indexOf("body=");
        if (bodyIndex >= 0) {
            return truncateForLog(message.substring(bodyIndex + 5).trim());
        }
        return truncateForLog(message);
    }

    private String truncateForLog(String value) {
        if (value == null || value.isBlank()) {
            return "-";
        }
        String normalized = value.replaceAll("\n+", " ").trim();
        return normalized.length() > 500 ? normalized.substring(0, 500) + "..." : normalized;
    }

    private String safeValue(String value) {
        return value == null || value.isBlank() ? "-" : truncateForLog(value);
    }

    private String resolveTokenUrl(String myDomain) {
        String tokenPath = resolveSalesforcePath(defaultTokenUrl);
        String loginBaseUrl = resolveSalesforceBaseUrl(myDomain);

        if (tokenPath.startsWith("http://") || tokenPath.startsWith("https://")) {
            return tokenPath;
        }
        return loginBaseUrl + tokenPath;
    }

    private String resolveSalesforcePath(String value) {
        String raw = value == null ? "" : value.trim();
        if (raw.isBlank()) {
            return "/services/oauth2/token";
        }
        if (raw.startsWith("http://") || raw.startsWith("https://")) {
            return raw;
        }
        return raw.startsWith("/") ? raw : "/" + raw;
    }

    private String resolveSalesforceBaseUrl(String myDomain) {
        String base = myDomain == null || myDomain.isBlank() ? null : myDomain.trim();
        String host;

        if (base != null) {
            try {
                if (base.startsWith("http://")) {
                    throw new AppException("Insecure Salesforce myDomain is not allowed: http:// scheme is rejected");
                }
                if (base.startsWith("https://")) {
                    URI uri = URI.create(base);
                    host = uri.getHost();
                } else {
                    host = base;
                }
            } catch (Exception e) {
                if (e instanceof AppException appException) {
                    throw appException;
                }
                host = base;
            }

            if (host != null && !host.isBlank()) {
                String normalizedHost = host.replaceAll("/+$", "").trim();
                String withoutPath = normalizedHost.split("[/?]")[0];
                String[] hostParts = withoutPath.split(":", 2);
                if (hostParts.length > 0 && !hostParts[0].isBlank()) {
                    return "https://" + withoutPath;
                }
            }
        }
        return "https://login.salesforce.com";
    }

    public record RefreshResult(String accessToken) {}
}
