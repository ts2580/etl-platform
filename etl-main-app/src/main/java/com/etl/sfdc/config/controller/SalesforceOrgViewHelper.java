package com.etl.sfdc.config.controller;

import com.etl.sfdc.common.SalesforceTokenManager;
import com.etl.sfdc.common.UserSession;
import com.etl.sfdc.config.model.dto.SalesforceOrgCredential;
import com.etl.sfdc.config.model.repository.RoutingDashboardRepository;
import com.etl.sfdc.config.model.service.SalesforceOrgService;
import jakarta.servlet.http.HttpSession;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.ui.Model;

import java.util.List;
import java.util.Map;

@Component
@RequiredArgsConstructor
public class SalesforceOrgViewHelper {

    private final UserSession userSession;
    private final SalesforceOrgService salesforceOrgService;
    private final RoutingDashboardRepository routingDashboardRepository;
    private final SalesforceTokenManager tokenManager;

    @Value("${app.db.enabled:false}")
    private boolean dbEnabled;

    @Value("${file.engine.base-url:http://localhost:9443}")
    private String fileEngineBaseUrl;

    public void populateHomeModel(Model model, HttpSession session) {
        model.addAttribute("dbEnabled", dbEnabled);
        model.addAttribute("fileEngineBaseUrl", fileEngineBaseUrl);

        if (userSession.getUserAccount() == null) {
            return;
        }

        var member = userSession.getUserAccount().getMember();
        model.addAttribute("member", member);
        model.addAttribute("displayName", member.getName() != null && !member.getName().isBlank() ? member.getName() : member.getUsername());
        model.addAttribute("displayDescription", member.getDescription() != null && !member.getDescription().isBlank() ? member.getDescription() : "-");

        List<SalesforceOrgCredential> orgs = salesforceOrgService.getActiveOrgs();
        model.addAttribute("orgs", orgs);

        String activeOrgKey = tokenManager.getActiveOrgKey(session);
        model.addAttribute("activeOrgKey", activeOrgKey);

        SalesforceOrgCredential activeOrg = activeOrgKey == null ? salesforceOrgService.getDefaultOrg() : salesforceOrgService.getOrg(activeOrgKey);
        if (activeOrg == null) {
            activeOrg = salesforceOrgService.getDefaultOrg();
        }
        model.addAttribute("activeOrg", activeOrg);

        List<Map<String, Object>> activeRoutes = activeOrg == null
                ? List.of()
                : routingDashboardRepository.findActiveRoutesByOrg(activeOrg.getOrgKey());
        model.addAttribute("activeOrgRoutes", activeRoutes);
    }

    public void populateOrgInputModel(Model model,
                                      HttpSession session,
                                      String orgName,
                                      String myDomain,
                                      String clientId,
                                      String clientSecret,
                                      boolean isDefault) {
        populateHomeModel(model, session);
        model.addAttribute("orgNameInput", orgName);
        model.addAttribute("myDomainInput", myDomain);
        model.addAttribute("clientIdInput", clientId);
        model.addAttribute("clientSecretInput", clientSecret);
        model.addAttribute("isDefaultInput", isDefault);
    }
}
