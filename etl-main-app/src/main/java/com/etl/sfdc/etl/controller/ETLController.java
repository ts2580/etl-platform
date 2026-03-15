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
import java.util.Map;

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

        List<ObjectDefinition> objectDefinitions;
        try {
            objectDefinitions = etlService.getObjects(accessToken);
        } catch (AppException e) {
            if (e.getMessage() != null && e.getMessage().contains("401")) {
                String refreshedAccessToken = tokenManager.refreshAccessToken(session);
                if (refreshedAccessToken == null) {
                    log.warn("Failed to recover from Salesforce 401 while fetching object list. Redirecting to /login");
                    response.sendRedirect("/login");
                    return null;
                }
                objectDefinitions = etlService.getObjects(refreshedAccessToken);
            } else {
                throw e;
            }
        }
        model.addAttribute("objectDefinitions", objectDefinitions);
        model.addAttribute("cdcSlotSummary", etlService.getCdcSlotSummary());

        if (userSession.getUserAccount() != null) {
            model.addAttribute(userSession.getUserAccount().getMember());
        }
        return "object_select_form";
    }

    @PostMapping("/ddl")
    public String setObjects(@RequestParam("selectedObject") String selectedObject,
                             @RequestParam("ingestionMode") String ingestionMode,
                             Model model,
                             HttpSession session) throws Exception {
        String accessToken = tokenManager.getAccessToken(session);
        String refreshToken = tokenManager.getRefreshToken(session);
        String sanitizedObject = RequestValidationUtils.requireIdentifier(selectedObject, "selectedObject");
        String sanitizedMode = RequestValidationUtils.requireText(ingestionMode, "ingestionMode").trim().toUpperCase();

        if (!"STREAMING".equals(sanitizedMode) && !"CDC".equals(sanitizedMode)) {
            throw new AppException("ingestionMode는 STREAMING 또는 CDC만 허용됩니다.");
        }

        if (accessToken == null || refreshToken == null) {
            throw new AppException("세션이 만료되었습니다. 다시 Salesforce 로그인 해주세요.");
        }

        if (userSession.getUserAccount() != null) {
            model.addAttribute(userSession.getUserAccount().getMember());
        }

        try {
            log.info("Submitting ETL object selection. selectedObject={}, ingestionMode={}", sanitizedObject, sanitizedMode);
            Map<String, Object> resultSummary = etlService.setObjects(sanitizedObject, sanitizedMode, accessToken, refreshToken);
            model.addAttribute("resultSummary", resultSummary);
            return "etl_result_summary";
        } catch (AppException e) {
            log.warn("ETL submission rejected. selectedObject={}, ingestionMode={}, message={}", sanitizedObject, sanitizedMode, e.getMessage());
            Map<String, Object> failureSummary = new java.util.HashMap<>();
            failureSummary.put("selectedObject", sanitizedObject);
            failureSummary.put("ingestionMode", sanitizedMode);
            failureSummary.put("endpoint", "CDC".equals(sanitizedMode) ? "/pubsub" : "/streaming");
            failureSummary.put("status", "FAILED");
            failureSummary.put("message", "설정 중 문제가 발생했어요.");
            failureSummary.put("engineMessage", e.getMessage());
            failureSummary.put("responseBody", e.getMessage());
            failureSummary.put("initialLoadCount", 0);
            failureSummary.put("subscribeStatus", "NOT_STARTED");
            failureSummary.put("pushTopicStatus", "NOT_STARTED");
            failureSummary.put("cdcCreationStatus", "NOT_STARTED");
            failureSummary.put("cdcCreationMessage", "실패로 인해 CDC 생성 단계가 완료되지 않았어요.");
            failureSummary.put("failureStage", "UNKNOWN");
            failureSummary.put("failureDetail", e.getMessage());
            failureSummary.put("slotRegistryStatus", "NOT_STARTED");
            model.addAttribute("resultSummary", failureSummary);
            return "etl_result_summary";
        }
    }
}
