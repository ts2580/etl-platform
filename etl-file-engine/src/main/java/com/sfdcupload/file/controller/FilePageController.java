package com.sfdcupload.file.controller;

import com.sfdcupload.common.SalesforceOrgCredential;
import com.sfdcupload.common.SalesforceOrgService;
import com.sfdcupload.common.SalesforceTokenManager;
import jakarta.servlet.http.HttpSession;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;

@Controller
@RequiredArgsConstructor
public class FilePageController {

    private final SalesforceOrgService salesforceOrgService;
    private final SalesforceTokenManager tokenManager;

    @GetMapping("/file")
    public String fileIndex(@RequestParam(value = "orgKey", required = false) String orgKey,
                            HttpSession session,
                            Model model) {
        if (orgKey != null && !orgKey.isBlank()) {
            tokenManager.setActiveOrg(session, orgKey);
        }

        SalesforceOrgCredential activeOrg = salesforceOrgService.resolveSelectedOrg(tokenManager.getActiveOrgKey(session));
        if (activeOrg != null) {
            tokenManager.setActiveOrg(session, activeOrg.getOrgKey());
        }
        model.addAttribute("activeOrg", activeOrg);
        return "forward:/index.html";
    }
}
