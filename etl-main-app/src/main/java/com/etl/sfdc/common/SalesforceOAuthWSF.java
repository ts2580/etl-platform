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
        if (isBlank(clientId) || isBlank(clientSecret) || isBlank(redirectUri) || isBlank(authUrl) || isBlank(tokenUrl)) {
            throw new AppException("Salesforce OAuth 설정값이 비어 있습니다. 환경변수를 확인해주세요: SALESFORCE_CLIENT_ID, SALESFORCE_CLIENT_SECRET, SALESFORCE_REDIRECT_URI, SALESFORCE_AUTH_URL, SALESFORCE_TOKEN_URL");
        }
    }

    private boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
