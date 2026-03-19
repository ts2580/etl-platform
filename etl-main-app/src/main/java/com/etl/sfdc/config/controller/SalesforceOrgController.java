package com.etl.sfdc.config.controller;

import com.etl.sfdc.config.model.service.SalesforceOrgService;
import com.etl.sfdc.config.model.dto.SalesforceOrgCredential;
import com.etl.sfdc.common.SalesforceTokenManager;
import com.etl.sfdc.common.UserSession;
import com.etl.sfdc.etl.service.ETLService;
import jakarta.servlet.http.HttpSession;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;


@Controller
@RequiredArgsConstructor
@Slf4j
public class SalesforceOrgController {

    public static final String ACTIVE_ORG_SESSION_KEY = "ACTIVE_SALESFORCE_ORG_KEY";
    private final SalesforceOrgService salesforceOrgService;
    private final SalesforceTokenManager tokenManager;
    private final ETLService etlService;
    private final UserSession userSession;

    @GetMapping("/etl/orgs")
    public String orgManagement(@RequestParam(value = "editOrgKey", required = false) String editOrgKey,
                               Model model,
                               HttpSession session) {
        model.addAttribute("orgs", salesforceOrgService.getActiveOrgs());
        model.addAttribute("activeOrgKey", session.getAttribute(ACTIVE_ORG_SESSION_KEY));
        if (editOrgKey != null && !editOrgKey.isBlank()) {
            model.addAttribute("editOrg", salesforceOrgService.getOrg(editOrgKey));
        }
        if (userSession.getUserAccount() != null) {
            model.addAttribute(userSession.getUserAccount().getMember());
        }
        return "org_management";
    }

    @PostMapping("/etl/orgs/register")
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

        SalesforceOrgCredential org = salesforceOrgService.registerOrUpdateClientCredentials(
                orgName,
                myDomain,
                clientId,
                clientSecret,
                isDefault
        );
        session.setAttribute(ACTIVE_ORG_SESSION_KEY, org.getOrgKey());
        tokenManager.setActiveOrg(session, org.getOrgKey());

        SalesforceTokenManager.RefreshResult refreshResult = tokenManager.refreshClientCredentialsToken(session, org);
        if (refreshResult != null && refreshResult.accessToken() != null && !refreshResult.accessToken().isBlank()) {
            salesforceOrgService.persistTokens(org.getOrgKey(), refreshResult.accessToken());
            tokenManager.setAccessToken(session, refreshResult.accessToken());
            return "redirect:/etl/orgs?message=org_registered";
        }

        return "redirect:/etl/orgs?message=org_registered_but_token_failed&orgKey=" + org.getOrgKey();
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

        // CLIENT_CREDENTIALS 기준으로 접근 토큰 갱신
        SalesforceTokenManager.RefreshResult refreshResult = tokenManager.refreshClientCredentialsToken(session, org);
        if (refreshResult != null && refreshResult.accessToken() != null && !refreshResult.accessToken().isBlank()) {
            salesforceOrgService.persistTokens(orgKey, refreshResult.accessToken());
            tokenManager.setAccessToken(session, refreshResult.accessToken());
            tokenManager.setActiveOrg(session, orgKey);
            return "redirect:/etl/orgs?message=token_refreshed&orgKey=" + orgKey;
        }

        // client credentials 기반 access token 발급 실패
        return "redirect:/etl/orgs?message=token_refresh_failed&reason=client_credentials_failed&orgKey=" + orgKey;
    }

    @PostMapping(value = "/etl/orgs/update-credentials")
    public String updateOrgClientCredentials(@RequestParam("orgKey") String orgKey,
                                            @RequestParam("clientId") String clientId,
                                            @RequestParam("clientSecret") String clientSecret,
                                            HttpSession session) {
        if (orgKey == null || orgKey.isBlank() || clientId == null || clientId.isBlank() || clientSecret == null || clientSecret.isBlank()) {
            return "redirect:/etl/orgs?message=need_org_login_inputs&orgKey=" + (orgKey == null ? "" : orgKey);
        }

        SalesforceOrgCredential updatedOrg = salesforceOrgService.updateClientCredentials(orgKey, clientId, clientSecret);
        if (updatedOrg == null) {
            return "redirect:/etl/orgs?message=org_credentials_update_failed&orgKey=" + orgKey;
        }

        SalesforceTokenManager.RefreshResult refreshResult = tokenManager.refreshClientCredentialsToken(session, updatedOrg);
        if (refreshResult != null && refreshResult.accessToken() != null && !refreshResult.accessToken().isBlank()) {
            salesforceOrgService.persistTokens(orgKey, refreshResult.accessToken());
            tokenManager.setActiveOrg(session, orgKey);
            try {
                etlService.refreshRoutingModuleCredentials(refreshResult.accessToken(), updatedOrg.getMyDomain(), orgKey);
            } catch (Exception e) {
                return "redirect:/etl/orgs?message=org_credentials_updated_routing_failed&orgKey=" + orgKey;
            }
            return "redirect:/etl/orgs?message=org_credentials_updated&orgKey=" + orgKey;
        }

        return "redirect:/etl/orgs?message=org_credentials_updated_token_failed&orgKey=" + orgKey;
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
