package com.etl.sfdc.etl.controller;

import com.etl.sfdc.common.SalesforceTokenManager;
import com.etl.sfdc.common.UserSession;
import com.etl.sfdc.etl.dto.ObjectDefinition;
import com.etl.sfdc.etl.service.ETLService;
import com.etlplatform.common.error.AppException;
import com.etlplatform.common.validation.RequestValidationUtils;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpSession;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.List;

@Controller
@RequiredArgsConstructor
@RequestMapping("etl")
@Slf4j
public class ETLController {

    private final UserSession userSession;
    private final SalesforceTokenManager tokenManager;
    private final ETLService etlService;

    @GetMapping("/objects")
    public String getObjects(Model model, HttpSession session, HttpServletResponse response) throws Exception {
        String accessToken = tokenManager.getAccessToken(session);
        if (accessToken == null) {
            log.info("ETL object list requested without access token. Redirecting to /login");
            response.sendRedirect("/login");
            return null;
        }

        List<ObjectDefinition> objectDefinitions = etlService.getObjects(accessToken);
        model.addAttribute("objectDefinitions", objectDefinitions);

        if (userSession.getUserAccount() != null) {
            model.addAttribute(userSession.getUserAccount().getMember());
        }
        return "object_select_form";
    }

    @PostMapping("/ddl")
    public String setObjects(@RequestParam("selectedObject") String selectedObject, Model model, HttpSession session) throws Exception {
        String accessToken = tokenManager.getAccessToken(session);
        String refreshToken = tokenManager.getRefreshToken(session);
        String sanitizedObject = RequestValidationUtils.requireIdentifier(selectedObject, "selectedObject");

        if (accessToken == null || refreshToken == null) {
            throw new AppException("세션이 만료되었습니다. 다시 Salesforce 로그인 해주세요.");
        }

        if (userSession.getUserAccount() != null) {
            model.addAttribute(userSession.getUserAccount().getMember());
        }

        log.info("Submitting ETL object selection. selectedObject={}", sanitizedObject);
        etlService.setObjects(sanitizedObject, accessToken, refreshToken);
        return "home_form";
    }
}
