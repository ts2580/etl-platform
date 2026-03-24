package com.sfdcupload.file.controller;

import com.sfdcupload.common.SalesforceOrgCredential;
import com.sfdcupload.common.SalesforceOrgService;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

@Controller
@RequiredArgsConstructor
public class FilePageController {

    private final SalesforceOrgService salesforceOrgService;

    @Value("${app.main.base-url:http://localhost:8080}")
    private String mainAppBaseUrl;

    @GetMapping("/file")
    public String fileIndex(@RequestParam(value = "orgKey", required = false) String orgKey,
                            Model model) {
        // 여기서 전달받는 orgKey는 Salesforce orgId가 아니라 main-app에서 관리하는 myDomain 기반 식별자입니다.
        SalesforceOrgCredential activeOrg = salesforceOrgService.resolveSelectedOrg(orgKey);
        model.addAttribute("activeOrg", activeOrg);
        model.addAttribute("activeOrgKey", activeOrg != null ? activeOrg.getOrgKey() : orgKey);
        return "forward:/index.html";
    }

    @GetMapping("/storages/databases")
    public String redirectStorageList(@RequestParam(value = "orgKey", required = false) String orgKey) {
        return "redirect:" + buildMainAppStorageUrl(orgKey, false);
    }

    @GetMapping("/storages/databases/new")
    public String redirectStorageRegistration(@RequestParam(value = "orgKey", required = false) String orgKey) {
        return "redirect:" + buildMainAppStorageUrl(orgKey, true);
    }

    private String buildMainAppStorageUrl(String orgKey, boolean registrationAnchor) {
        StringBuilder url = new StringBuilder(normalizeMainAppBaseUrl()).append("/storages/databases");
        if (orgKey != null && !orgKey.isBlank()) {
            url.append("?orgKey=")
                    .append(URLEncoder.encode(orgKey, StandardCharsets.UTF_8));
        }
        if (registrationAnchor) {
            url.append("#storage-registration");
        }
        return url.toString();
    }

    private String normalizeMainAppBaseUrl() {
        if (mainAppBaseUrl == null || mainAppBaseUrl.isBlank()) {
            return "http://localhost:8080";
        }
        if (mainAppBaseUrl.endsWith("/")) {
            return mainAppBaseUrl.substring(0, mainAppBaseUrl.length() - 1);
        }
        return mainAppBaseUrl;
    }
}
