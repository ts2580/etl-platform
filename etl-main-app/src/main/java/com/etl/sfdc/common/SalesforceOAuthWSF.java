package com.etl.sfdc.common;

import com.etlplatform.common.error.AppException;
import com.etlplatform.common.salesforce.SalesforceOAuthClient;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpSession;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

@Slf4j
@RestController
@RequiredArgsConstructor
public class SalesforceOAuthWSF {

    private final SalesforceOAuthClient oauthClient;
    private final SalesforceTokenManager tokenManager;

    @Value("${salesforce.clientId}")
    private String clientId;

    @Value("${salesforce.clientSecret}")
    private String clientSecret;

    @Value("${salesforce.redirectUri}")
    private String redirectUri;

    @Value("${salesforce.authUrl}")
    private String authUrl;

    @Value("${salesforce.tokenUrl}")
    private String tokenUrl;

    @GetMapping("/login")
    public void login(HttpServletResponse response) throws IOException {
        validateOauthConfiguration();

        String redirect = oauthClient.buildLoginRedirectUrl(authUrl, clientId, redirectUri);
        response.sendRedirect(redirect);
    }

    @GetMapping("/oauth/callback")
    public void callback(@RequestParam(required = false) String code, HttpSession session, HttpServletResponse response) throws IOException {
        if (code == null || code.isEmpty()) {
            log.warn("OAuth callback received without authorization code");
            return;
        }
        log.info("OAuth callback received with authorization code");
        validateOauthConfiguration();

        SalesforceOAuthClient.TokenResponse token = oauthClient.exchangeAuthorizationCode(
                tokenUrl,
                code,
                clientId,
                clientSecret,
                redirectUri
        );

        tokenManager.setTokenPair(session, token.accessToken(), token.refreshToken());

        response.sendRedirect("/?message=token_refreshed");
    }

    private void validateOauthConfiguration() {
        StringBuilder missing = new StringBuilder();

        appendMissing(missing, "SALESFORCE_CLIENT_ID", clientId);
        appendMissing(missing, "SALESFORCE_CLIENT_SECRET", clientSecret);
        appendMissing(missing, "SALESFORCE_REDIRECT_URI", redirectUri);
        appendMissing(missing, "SALESFORCE_AUTH_URL", authUrl);
        appendMissing(missing, "SALESFORCE_TOKEN_URL", tokenUrl);

        if (missing.length() > 0) {
            String missingKeys = missing.toString();
            log.error("Salesforce OAuth configuration missing. missingKeys={}, redirectUri={}, authUrl={}, tokenUrl={}",
                    missingKeys,
                    safeValue(redirectUri),
                    safeValue(authUrl),
                    safeValue(tokenUrl));
            throw new AppException("Salesforce OAuth 설정값이 비어 있습니다. 누락 키: " + missingKeys);
        }
    }

    private void appendMissing(StringBuilder missing, String key, String value) {
        if (isBlank(value)) {
            if (missing.length() > 0) {
                missing.append(", ");
            }
            missing.append(key);
        }
    }

    private String safeValue(String value) {
        return isBlank(value) ? "<blank>" : value;
    }

    private boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
