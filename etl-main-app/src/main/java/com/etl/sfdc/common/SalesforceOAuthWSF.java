package com.etl.sfdc.common;

import com.etlplatform.common.salesforce.SalesforceOAuthClient;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpSession;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

@RestController
@RequiredArgsConstructor
public class SalesforceOAuthWSF {

    private final SalesforceOAuthClient oauthClient = new SalesforceOAuthClient();

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
        String redirect = oauthClient.buildLoginRedirectUrl(authUrl, clientId, redirectUri);

        response.sendRedirect(redirect);
    }

    @GetMapping("/oauth/callback")
    public void callback(@RequestParam(required = false) String code, HttpSession session, HttpServletResponse response) throws IOException {
        if (code == null || code.isEmpty()) {
            return;
        }

        SalesforceOAuthClient.TokenResponse token = oauthClient.exchangeAuthorizationCode(
                tokenUrl,
                code,
                clientId,
                clientSecret,
                redirectUri
        );

        // ✅ 세션에 저장
        session.setAttribute("accessToken", token.accessToken());
        session.setAttribute("refreshToken", token.refreshToken());

        response.sendRedirect("/?message=token_refreshed");
    }
}