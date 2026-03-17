package com.sfdcupload.file.controller;

import com.sfdcupload.common.SalesforceOrgCredential;
import com.sfdcupload.common.SalesforceOrgService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;

@Controller
@RequiredArgsConstructor
public class FilePageController {

    private final SalesforceOrgService salesforceOrgService;

    @GetMapping("/file")
    public String fileIndex(@RequestParam(value = "orgKey", required = false) String orgKey,
                            Model model) {
        SalesforceOrgCredential activeOrg = salesforceOrgService.resolveSelectedOrg(orgKey);
        model.addAttribute("activeOrg", activeOrg);
        model.addAttribute("activeOrgKey", activeOrg != null ? activeOrg.getOrgKey() : orgKey);
        return "forward:/index.html";
    }
}
