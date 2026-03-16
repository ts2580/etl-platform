package com.etl.sfdc.common;

import com.etl.sfdc.config.model.dto.SalesforceOrgCredential;
import com.etl.sfdc.config.model.service.SalesforceOrgService;
import com.etlplatform.common.error.AppException;
import com.etlplatform.common.salesforce.SalesforceOAuthClient;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpSession;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.view.RedirectView;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@Slf4j
@RestController
@RequiredArgsConstructor
public class SalesforceOAuthWSF {

    public static final String PENDING_OAUTH_CONTEXTS = "PENDING_OAUTH_CONTEXTS";

    private final SalesforceOAuthClient oauthClient;
    private final SalesforceTokenManager tokenManager;
    private final SalesforceOrgService salesforceOrgService;

    @Value("${salesforce.redirectUri}")
    private String defaultRedirectUri;

    @Value("${salesforce.authUrl:https://login.salesforce.com/services/oauth2/authorize}")
    private String defaultAuthUrl;

    @Value("${salesforce.tokenUrl:https://login.salesforce.com/services/oauth2/token}")
    private String defaultTokenUrl;

    @GetMapping("/oauth/start")
    public RedirectView startOAuth(@RequestParam("state") String state,
                                  HttpSession session) {
        if (state == null || state.isBlank()) {
            throw new AppException("OAuth state 정보가 없습니다.");
        }

        @SuppressWarnings("unchecked")
        Map<String, PendingOAuthContext> map = (Map<String, PendingOAuthContext>) session.getAttribute(PENDING_OAUTH_CONTEXTS);
        if (map == null || !map.containsKey(state)) {
            throw new AppException("OAuth 요청 컨텍스트를 찾을 수 없습니다. 다시 로그인 등록을 시작해 주세요.");
        }

        PendingOAuthContext context = map.get(state);
        String resolvedClientId = Optional.ofNullable(context.clientId).map(String::trim).orElse("");
        String resolvedRedirectUri = Optional.ofNullable(context.redirectUri).map(String::trim).orElse(defaultRedirectUri);
        String resolvedAuthUrl = Optional.ofNullable(context.authUrl).map(String::trim).orElse(defaultAuthUrl);

        if (resolvedClientId.isBlank() || resolvedRedirectUri.isBlank() || resolvedAuthUrl.isBlank()) {
            throw new AppException("OAuth 설정값(clientId/redirectUri/authUrl)이 비어 있습니다.");
        }

        if (context.expiresAtEpochSecond <= Instant.now().getEpochSecond()) {
            map.remove(state);
            session.setAttribute(PENDING_OAUTH_CONTEXTS, map);
            throw new AppException("OAuth 요청이 만료되었습니다. 다시 시작해 주세요.");
        }

        String loginUrl = oauthClient.buildLoginRedirectUrl(resolvedAuthUrl, resolvedClientId, resolvedRedirectUri)
                + "&state=" + URLEncoder.encode(state, StandardCharsets.UTF_8);
        return new RedirectView(loginUrl);
    }

    @GetMapping("/login")
    public void login(HttpServletResponse response, HttpSession session,
                      @RequestParam(value = "orgName", required = false) String orgName,
                      @RequestParam(value = "myDomain", required = false) String myDomain,
                      @RequestParam(value = "clientId", required = false) String clientId,
                      @RequestParam(value = "clientSecret", required = false) String clientSecret,
                      @RequestParam(value = "isDefault", required = false, defaultValue = "false") boolean isDefault,
                      @RequestParam(value = "authUrl", required = false) String authUrl,
                      @RequestParam(value = "tokenUrl", required = false) String tokenUrl,
                      @RequestParam(value = "redirectUri", required = false) String redirectUri) throws IOException {

        if (clientId == null || clientId.isBlank()
                || clientSecret == null || clientSecret.isBlank()
                || myDomain == null || myDomain.isBlank()) {
            response.sendRedirect("/etl/orgs?message=need_org_login_inputs");
            return;
        }

        String state = createPendingOAuthContext(session, orgName, myDomain, clientId, clientSecret, authUrl, tokenUrl, redirectUri, isDefault);
        response.sendRedirect("/oauth/start?state=" + URLEncoder.encode(state, StandardCharsets.UTF_8));
    }

    public String createPendingOAuthContext(HttpSession session,
                                            String orgName,
                                            String myDomain,
                                            String clientId,
                                            String clientSecret,
                                            String authUrl,
                                            String tokenUrl,
                                            String redirectUri,
                                            boolean isDefault) {
        String state = UUID.randomUUID().toString();
        @SuppressWarnings("unchecked")
        Map<String, PendingOAuthContext> map =
                (Map<String, PendingOAuthContext>) Optional.ofNullable(session.getAttribute(PENDING_OAUTH_CONTEXTS))
                        .orElse(new HashMap<>());

        long expiresAt = Instant.now().getEpochSecond() + 600;
        map.put(state, new PendingOAuthContext(orgName, myDomain, clientId, clientSecret,
                authUrl, tokenUrl, redirectUri, isDefault, expiresAt));
        session.setAttribute(PENDING_OAUTH_CONTEXTS, map);
        return state;
    }

    @GetMapping("/oauth/callback")
    public void callback(@RequestParam(value = "code", required = false) String code,
                        @RequestParam(value = "state", required = false) String state,
                        HttpServletResponse response,
                        HttpSession session) throws IOException {
        try {
            if (code == null || code.isEmpty()) {
                throw new AppException("OAuth callback에서 인증코드가 없습니다.");
            }
            if (state == null || state.isBlank()) {
                throw new AppException("OAuth callback state가 없습니다.");
            }

            @SuppressWarnings("unchecked")
            Map<String, PendingOAuthContext> map = (Map<String, PendingOAuthContext>) session.getAttribute(PENDING_OAUTH_CONTEXTS);
            if (map == null || !map.containsKey(state)) {
                throw new AppException("유효하지 않은 OAuth state입니다.");
            }

            PendingOAuthContext context = map.remove(state);
            if (context == null) {
                throw new AppException("OAuth 컨텍스트를 찾지 못했습니다.");
            }
            session.setAttribute(PENDING_OAUTH_CONTEXTS, map);

            String resolvedTokenUrl = Optional.ofNullable(context.tokenUrl).map(String::trim).filter(v -> !v.isBlank()).orElse(defaultTokenUrl);
            String resolvedClientId = context.clientId == null ? "" : context.clientId;
            String resolvedClientSecret = context.clientSecret == null ? "" : context.clientSecret;
            String resolvedRedirectUri = Optional.ofNullable(context.redirectUri).map(String::trim)
                    .filter(v -> !v.isBlank()).orElse(defaultRedirectUri);

            if (resolvedClientId.isBlank() || resolvedClientSecret.isBlank() || resolvedRedirectUri.isBlank()) {
                throw new AppException("OAuth 컨텍스트의 clientId/clientSecret/redirectUri 값이 비어 있습니다.");
            }

            SalesforceOAuthClient.TokenResponse token = oauthClient.exchangeAuthorizationCode(
                    resolvedTokenUrl,
                    code,
                    resolvedClientId,
                    resolvedClientSecret,
                    resolvedRedirectUri
            );

            String orgId = extractOrgId(token.id());
            SalesforceOrgCredential registeredOrg = salesforceOrgService.registerOrUpdateFromOAuth(
                    token.instanceUrl(),
                    orgId,
                    context.orgName,
                    context.myDomain,
                    resolvedClientId,
                    resolvedClientSecret,
                    token.accessToken(),
                    token.refreshToken(),
                    context.isDefault
            );

            tokenManager.setTokenPair(session, token.accessToken(), token.refreshToken());
            tokenManager.setActiveOrg(session, registeredOrg.getOrgKey());

            response.sendRedirect("/etl/orgs?message=org_registered");
        } catch (Exception e) {
            log.warn("OAuth callback failed: {}", e.getMessage(), e);
            String reason = e.getMessage() == null ? "oauth_error" : e.getMessage().replace("\\n", " ");
            if (reason.length() > 180) {
                reason = reason.substring(0, 180);
            }
            response.sendRedirect("/etl/orgs?message=oauth_callback_failed&reason=" + URLEncoder.encode(reason, StandardCharsets.UTF_8));
        }
    }

    private String extractOrgId(String identityUrl) {
        if (identityUrl == null || identityUrl.isBlank()) {
            return null;
        }
        try {
            String path = URI.create(identityUrl).getPath();
            if (path == null) {
                return null;
            }
            String[] segments = path.split("/");
            return segments.length >= 2 ? segments[segments.length - 1] : null;
        } catch (Exception e) {
            log.warn("Failed to extract org id from identityUrl={}", identityUrl, e);
            return null;
        }
    }

    @Data
    private static class PendingOAuthContext {
        private final String orgName;
        private final String myDomain;
        private final String clientId;
        private final String clientSecret;
        private final String authUrl;
        private final String tokenUrl;
        private final String redirectUri;
        private final boolean isDefault;
        private final long expiresAtEpochSecond;

        private PendingOAuthContext(String orgName, String myDomain, String clientId, String clientSecret,
                                   String authUrl, String tokenUrl, String redirectUri,
                                   boolean isDefault, long expiresAtEpochSecond) {
            this.orgName = orgName;
            this.myDomain = myDomain;
            this.clientId = clientId;
            this.clientSecret = clientSecret;
            this.authUrl = authUrl;
            this.tokenUrl = tokenUrl;
            this.redirectUri = redirectUri;
            this.isDefault = isDefault;
            this.expiresAtEpochSecond = expiresAtEpochSecond;
        }
    }
}
