package com.etl.sfdc.home.controller;

import com.etl.sfdc.common.SalesforceTokenManager;
import com.etl.sfdc.common.UserSession;
import com.etl.sfdc.config.model.dto.SalesforceOrgCredential;
import com.etl.sfdc.config.model.service.SalesforceOrgService;
import jakarta.servlet.http.HttpSession;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;

@Controller
@RequiredArgsConstructor
public class HomeController {

    private final UserSession userSession;
    private final SalesforceOrgService salesforceOrgService;
    private final SalesforceTokenManager tokenManager;

    @Value("${app.db.enabled:false}")
    private boolean dbEnabled;

    @GetMapping("/")
    public String loginPage(Model model, HttpSession session) {
        model.addAttribute("dbEnabled", dbEnabled);

        if (userSession.getUserAccount() != null) {
            var member = userSession.getUserAccount().getMember();
            model.addAttribute("member", member);
            model.addAttribute("displayName", member.getName() != null && !member.getName().isBlank() ? member.getName() : member.getUsername());
            model.addAttribute("displayDescription", member.getDescription() != null && !member.getDescription().isBlank() ? member.getDescription() : "-");
            model.addAttribute("orgs", salesforceOrgService.getActiveOrgs());
            String activeOrgKey = tokenManager.getActiveOrgKey(session);
            SalesforceOrgCredential activeOrg = activeOrgKey == null ? salesforceOrgService.getDefaultOrg() : salesforceOrgService.getOrg(activeOrgKey);
            if (activeOrg == null) {
                activeOrg = salesforceOrgService.getDefaultOrg();
            }
            model.addAttribute("activeOrg", activeOrg);
        }

        return "home_form";
    }
}
