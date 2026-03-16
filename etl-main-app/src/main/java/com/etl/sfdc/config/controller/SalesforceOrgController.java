package com.etl.sfdc.config.controller;

import com.etl.sfdc.config.model.service.SalesforceOrgService;
import com.etl.sfdc.config.model.dto.SalesforceOrgCredential;
import com.etl.sfdc.common.SalesforceTokenManager;
import com.etl.sfdc.common.SalesforceOAuthWSF;
import com.etl.sfdc.common.UserSession;
import jakarta.servlet.http.HttpSession;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

@Controller
@RequiredArgsConstructor
@Slf4j
public class SalesforceOrgController {

    public static final String ACTIVE_ORG_SESSION_KEY = "ACTIVE_SALESFORCE_ORG_KEY";
    private final SalesforceOrgService salesforceOrgService;
    private final SalesforceOAuthWSF salesforceOAuthWSF;
    private final SalesforceTokenManager tokenManager;
    private final UserSession userSession;

    @GetMapping("/etl/orgs")
    public String orgManagement(Model model, HttpSession session) {
        model.addAttribute("orgs", salesforceOrgService.getActiveOrgs());
        model.addAttribute("activeOrgKey", session.getAttribute(ACTIVE_ORG_SESSION_KEY));
        if (userSession.getUserAccount() != null) {
            model.addAttribute(userSession.getUserAccount().getMember());
        }
        return "org_management";
    }

    @PostMapping("/etl/orgs/oauth/register")
    public String registerOrgOauth(@RequestParam("orgName") String orgName,
                                   @RequestParam("myDomain") String myDomain,
                                   @RequestParam("clientId") String clientId,
                                   @RequestParam("clientSecret") String clientSecret,
                                   @RequestParam(value = "isDefault", defaultValue = "false") boolean isDefault,
                                   Model model,
                                   HttpSession session) {
        model.addAttribute("orgNameInput", orgName);
        model.addAttribute("myDomainInput", myDomain);
        model.addAttribute("clientIdInput", clientId);
        model.addAttribute("clientSecretInput", clientSecret);
        model.addAttribute("isDefaultInput", isDefault);
        model.addAttribute("orgs", salesforceOrgService.getActiveOrgs());
        model.addAttribute("activeOrgKey", session.getAttribute(ACTIVE_ORG_SESSION_KEY));

        if (userSession.getUserAccount() != null) {
            model.addAttribute(userSession.getUserAccount().getMember());
        }

        if (orgName == null || orgName.isBlank() || myDomain == null || myDomain.isBlank()
                || clientId == null || clientId.isBlank() || clientSecret == null || clientSecret.isBlank()) {
            model.addAttribute("message", "need_org_login_inputs");
            return "org_management";
        }

        try {
            salesforceOrgService.normalizeOrgName(orgName);
        } catch (Exception e) {
            model.addAttribute("message", "invalid_org_name");
            return "org_management";
        }

        String state = salesforceOAuthWSF.createPendingOAuthContext(
                session,
                orgName,
                myDomain,
                clientId,
                clientSecret,
                null,
                null,
                null,
                isDefault
        );
        return "redirect:/oauth/start?state=" + URLEncoder.encode(state, StandardCharsets.UTF_8);
    }

    @PostMapping(value = "/etl/orgs/select")
    public String selectOrg(@RequestParam("orgKey") String orgKey,
                           @RequestParam(value = "returnTo", required = false) String returnTo,
                           HttpSession session) {
        session.setAttribute(ACTIVE_ORG_SESSION_KEY, orgKey);
        if ("/".equals(returnTo) || "/etl".equals(returnTo) || "/etl/orgs".equals(returnTo)) {
            if ("/".equals(returnTo)) {
                return "redirect:/";
            }
            return "redirect:/etl/orgs?message=org_selected";
        }
        return "redirect:/etl/orgs?message=org_selected";
    }

    @PostMapping(value = "/etl/orgs/activate")
    public String activateOrg(@RequestParam("orgKey") String orgKey, HttpSession session) {
        salesforceOrgService.setDefaultOrg(orgKey);
        return "redirect:/etl/orgs?message=org_activated";
    }


    @PostMapping(value = "/etl/orgs/refresh")
    public String refreshOrgToken(@RequestParam("orgKey") String orgKey, HttpSession session) {
        if (orgKey == null || orgKey.isBlank()) {
            return "redirect:/etl/orgs?message=token_refresh_failed";
        }

        SalesforceOrgCredential org = salesforceOrgService.getOrg(orgKey);
        if (org == null) {
            return "redirect:/etl/orgs?message=token_refresh_failed";
        }

        String clientId = org.getClientId();
        String clientSecret = org.getClientSecret();
        if (clientId == null || clientId.isBlank() || clientSecret == null || clientSecret.isBlank()) {
            return "redirect:/etl/orgs?message=oauth_start_failed&reason=missing_client_info";
        }

        // 우선 refresh_token grant를 먼저 시도
        SalesforceTokenManager.RefreshResult refreshResult = tokenManager.refreshTokenPair(session, org);
        if (refreshResult != null && refreshResult.accessToken() != null && !refreshResult.accessToken().isBlank()) {
            salesforceOrgService.persistTokens(orgKey, refreshResult.accessToken(), refreshResult.refreshToken());
            tokenManager.setActiveOrg(session, orgKey);
            return "redirect:/etl/orgs?message=token_refreshed&orgKey=" + orgKey;
        }

        // refresh token이 깨졌거나 만료된 경우: 저장된 클라이언트 정보로 OAuth를 재시작해 토큰 전체를 새로 받는다
        String state = salesforceOAuthWSF.createPendingOAuthContext(
                session,
                org.getOrgName(),
                org.getMyDomain(),
                clientId,
                clientSecret,
                null,
                null,
                null,
                Boolean.TRUE.equals(org.getIsDefault())
        );
        return "redirect:/oauth/start?state=" + URLEncoder.encode(state, StandardCharsets.UTF_8);
    }

    @PostMapping(value = "/etl/orgs/deactivate")
    public String deactivateOrg(@RequestParam("orgKey") String orgKey, HttpSession session) {
        salesforceOrgService.deactivateOrg(orgKey);
        Object active = session.getAttribute(ACTIVE_ORG_SESSION_KEY);
        if (orgKey.equals(String.valueOf(active))) {
            session.removeAttribute(ACTIVE_ORG_SESSION_KEY);
        }
        return "redirect:/etl/orgs?message=org_deactivated";
    }
}
