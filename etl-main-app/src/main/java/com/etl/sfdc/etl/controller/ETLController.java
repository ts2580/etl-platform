package com.etl.sfdc.etl.controller;

import com.etl.sfdc.config.model.dto.SalesforceOrgCredential;
import com.etl.sfdc.config.model.service.SalesforceOrgService;
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

import java.util.HashMap;
import java.util.Optional;
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
    private final SalesforceOrgService salesforceOrgService;

    @GetMapping("/objects")
    public String getObjects(Model model, HttpSession session, HttpServletResponse response,
                            @RequestParam(value = "orgKey", required = false) String requestedOrgKey) throws Exception {
        ExecutionContext context = resolveExecutionContext(session, requestedOrgKey);
        if (context.accessToken == null) {
            log.info("ETL object list requested without access token. Redirecting to /etl/orgs");
            response.sendRedirect("/etl/orgs?message=need_org_auth" + (context.org != null ? "&orgKey=" + context.org.getOrgKey() + "&reason=refresh_failed" : ""));
            return null;
        }

        List<ObjectDefinition> objectDefinitions = fetchObjectDefinitions(context, session, response);
        String actor = currentActor();

        Map<String, String> ingestionStatusByObject;
        try {
            ingestionStatusByObject = etlService.getIngestionStatusByObject(context.accessToken, context.myDomain);
        } catch (AppException e) {
            if (e.getMessage() != null && e.getMessage().contains("401")) {
                String refreshedAccessToken = tokenManager.refreshAccessToken(session, context.org);
                if (refreshedAccessToken != null && context.org != null) {
                    SalesforceTokenManager.RefreshResult refreshPair = tokenManager.refreshTokenPair(session, context.org);
                    if (refreshPair != null) {
                        salesforceOrgService.persistTokens(context.org.getOrgKey(), refreshPair.accessToken(), refreshPair.refreshToken());
                        context.accessToken = refreshPair.accessToken();
                        tokenManager.setTokenPair(session, refreshPair.accessToken(), refreshPair.refreshToken());
                    } else {
                        context.accessToken = refreshedAccessToken;
                    }
                    ingestionStatusByObject = etlService.getIngestionStatusByObject(context.accessToken, context.myDomain);
                } else {
                    response.sendRedirect("/etl/orgs?message=need_org_auth&reason=refresh_failed" + (context.org != null ? "&orgKey=" + context.org.getOrgKey() : ""));
                    return null;
                }
            } else {
                throw e;
            }
        }

        try {
            etlService.syncRoutingRegistryFromSalesforce(context.accessToken, actor, context.myDomain);
        } catch (AppException e) {
            if (e.getMessage() != null && e.getMessage().contains("401")) {
                String refreshedAccessToken = tokenManager.refreshAccessToken(session, context.org);
                if (refreshedAccessToken != null && context.org != null) {
                    SalesforceTokenManager.RefreshResult refreshPair = tokenManager.refreshTokenPair(session, context.org);
                    if (refreshPair != null) {
                        salesforceOrgService.persistTokens(context.org.getOrgKey(), refreshPair.accessToken(), refreshPair.refreshToken());
                        context.accessToken = refreshPair.accessToken();
                        tokenManager.setTokenPair(session, refreshPair.accessToken(), refreshPair.refreshToken());
                    } else {
                        context.accessToken = refreshedAccessToken;
                    }
                    etlService.syncRoutingRegistryFromSalesforce(context.accessToken, actor, context.myDomain);
                } else {
                    response.sendRedirect("/etl/orgs?message=need_org_auth&reason=refresh_failed" + (context.org != null ? "&orgKey=" + context.org.getOrgKey() : ""));
                    return null;
                }
            } else {
                throw e;
            }
        }

        String defaultSelectedObject = objectDefinitions.stream()
                .map(ObjectDefinition::getName)
                .filter(ingestionStatusByObject::containsKey)
                .findFirst()
                .orElse(null);

        model.addAttribute("activeOrgs", salesforceOrgService.getActiveOrgs());
        model.addAttribute("activeOrgKey", context.org != null ? context.org.getOrgKey() : null);
        model.addAttribute("objectDefinitions", objectDefinitions);
        model.addAttribute("cdcSlotSummary", etlService.getCdcSlotSummary());
        model.addAttribute("ingestionStatusByObject", ingestionStatusByObject);
        model.addAttribute("selectedObject", defaultSelectedObject);
        model.addAttribute("selectedIngestionMode", defaultSelectedObject != null ? ingestionStatusByObject.get(defaultSelectedObject) : null);
        model.addAttribute("activeOrgKey", context.org == null ? null : context.org.getOrgKey());

        if (userSession.getUserAccount() != null) {
            model.addAttribute(userSession.getUserAccount().getMember());
        }
        return "object_select_form";
    }

    @GetMapping("/dashboard")
    public String getDashboard(Model model, HttpSession session, HttpServletResponse response,
                              @RequestParam(value = "orgKey", required = false) String requestedOrgKey) throws Exception {
        ExecutionContext context = resolveExecutionContext(session, requestedOrgKey);
        if (context.accessToken == null) {
            if (context.requiresOrgLogin) {
                log.info("No active Salesforce org/session. Redirecting to /etl/orgs");
                response.sendRedirect("/etl/orgs?message=need_org_auth" + (context.org != null ? "&orgKey=" + context.org.getOrgKey() + "&reason=refresh_failed" : ""));
                return null;
            }
            log.info("ETL dashboard requested without access token. Redirecting to /etl/orgs");
            response.sendRedirect("/etl/orgs?message=need_org_auth" + (context.org != null ? "&orgKey=" + context.org.getOrgKey() + "&reason=refresh_failed" : ""));
            return null;
        }

        if (context.accessToken != null) {
            try {
                String actor = currentActor();
                etlService.syncRoutingRegistryFromSalesforce(context.accessToken, actor, context.myDomain);
            } catch (AppException e) {
                if (e.getMessage() != null && e.getMessage().contains("401")) {
                    String refreshedAccessToken = tokenManager.refreshAccessToken(session, context.org);
                    if (refreshedAccessToken != null && context.org != null) {
                        SalesforceTokenManager.RefreshResult refreshPair = tokenManager.refreshTokenPair(session, context.org);
                        if (refreshPair != null) {
                            salesforceOrgService.persistTokens(context.org.getOrgKey(), refreshPair.accessToken(), refreshPair.refreshToken());
                            context.accessToken = refreshPair.accessToken();
                            tokenManager.setTokenPair(session, refreshPair.accessToken(), refreshPair.refreshToken());
                        } else {
                            context.accessToken = refreshedAccessToken;
                        }
                        etlService.syncRoutingRegistryFromSalesforce(context.accessToken, currentActor(), context.myDomain);
                    } else {
                        response.sendRedirect("/etl/orgs?message=need_org_auth&reason=refresh_failed" + (context.org != null ? "&orgKey=" + context.org.getOrgKey() : ""));
                        return null;
                    }
                } else {
                    throw e;
                }
            }
        }
        String orgKey = context.org == null ? null : context.org.getOrgKey();
        model.addAttribute("dashboard", etlService.getRoutingDashboard(orgKey));

        if (userSession.getUserAccount() != null) {
            model.addAttribute(userSession.getUserAccount().getMember());
        }
        model.addAttribute("activeOrgs", salesforceOrgService.getActiveOrgs());
        model.addAttribute("activeOrgKey", context.org != null ? context.org.getOrgKey() : null);
        return "etl_dashboard";
    }

    @GetMapping("/routes/detail")
    public String getRouteDetail(@RequestParam("selectedObject") String selectedObject,
                                 @RequestParam("routingProtocol") String routingProtocol,
                                 @RequestParam(value = "orgKey", required = false) String requestedOrgKey,
                                 Model model,
                                 HttpSession session,
                                 HttpServletResponse response) throws Exception {
        ExecutionContext context = resolveExecutionContext(session, requestedOrgKey);
        if (context.accessToken == null) {
            response.sendRedirect("/etl/orgs?message=need_org_auth" + (context.org != null ? "&orgKey=" + context.org.getOrgKey() + "&reason=refresh_failed" : ""));
            return null;
        }

        try {
            try {
                String actor = currentActor();
                etlService.syncRoutingRegistryFromSalesforce(context.accessToken, actor, context.myDomain);
            } catch (AppException e) {
                if (e.getMessage() != null && e.getMessage().contains("401")) {
                    String refreshedAccessToken = tokenManager.refreshAccessToken(session, context.org);
                    if (refreshedAccessToken == null && context.org != null) {
                        response.sendRedirect("/etl/orgs?message=need_org_auth&reason=refresh_failed&orgKey=" + context.org.getOrgKey());
                        return null;
                    }
                    if (refreshedAccessToken != null && context.org != null) {
                        SalesforceTokenManager.RefreshResult refreshPair = tokenManager.refreshTokenPair(session, context.org);
                        if (refreshPair != null) {
                            salesforceOrgService.persistTokens(context.org.getOrgKey(), refreshPair.accessToken(), refreshPair.refreshToken());
                            context.accessToken = refreshPair.accessToken();
                            tokenManager.setTokenPair(session, refreshPair.accessToken(), refreshPair.refreshToken());
                        } else {
                            context.accessToken = refreshedAccessToken;
                        }
                        tokenManager.setActiveOrg(session, context.org.getOrgKey());
                    }
                } else {
                    throw e;
                }
            }

            if (context.accessToken == null) {
                if (context.org == null) {
                    response.sendRedirect("/etl/orgs?message=need_org_auth&reason=refresh_failed");
                    return null;
                }
                String refreshedAccessToken = tokenManager.refreshAccessToken(session, context.org);
                if (refreshedAccessToken == null) {
                    response.sendRedirect("/etl/orgs?message=need_org_auth&reason=refresh_failed&orgKey=" + context.org.getOrgKey());
                    return null;
                }
                SalesforceTokenManager.RefreshResult refreshPair = tokenManager.refreshTokenPair(session, context.org);
                if (refreshPair != null) {
                    salesforceOrgService.persistTokens(context.org.getOrgKey(), refreshPair.accessToken(), refreshPair.refreshToken());
                    context.accessToken = refreshPair.accessToken();
                    tokenManager.setTokenPair(session, refreshPair.accessToken(), refreshPair.refreshToken());
                } else {
                    context.accessToken = refreshedAccessToken;
                }
            }

            model.addAttribute("routeDetail", etlService.getRouteDetail(
                    context.accessToken,
                    context.myDomain,
                    context.org == null ? null : context.org.getOrgKey(),
                    selectedObject,
                    routingProtocol
            ));
            model.addAttribute("activeOrgs", salesforceOrgService.getActiveOrgs());
            model.addAttribute("activeOrgKey", context.org != null ? context.org.getOrgKey() : null);
            if (userSession.getUserAccount() != null) {
                model.addAttribute(userSession.getUserAccount().getMember());
            }
            return "etl_route_detail";
        } catch (AppException e) {
            log.warn("Failed to open route detail. selectedObject={}, routingProtocol={}, message={}", selectedObject, routingProtocol, e.getMessage());
            model.addAttribute("dashboard", etlService.getRoutingDashboard(context.org == null ? null : context.org.getOrgKey()));
            model.addAttribute("dashboardError", e.getMessage());
            model.addAttribute("activeOrgs", salesforceOrgService.getActiveOrgs());
            model.addAttribute("activeOrgKey", context.org != null ? context.org.getOrgKey() : null);
            if (userSession.getUserAccount() != null) {
                model.addAttribute(userSession.getUserAccount().getMember());
            }
            return "etl_dashboard";
        }
    }

    @PostMapping("/reprocess")
    public String reprocessObject(@RequestParam("selectedObject") String selectedObject,
                                  @RequestParam("ingestionMode") String ingestionMode,
                                  @RequestParam(value = "orgKey", required = false) String requestedOrgKey,
                                  Model model,
                                  HttpSession session,
                                  HttpServletResponse response) throws Exception {
        return setObjects(selectedObject, ingestionMode, requestedOrgKey, model, session, response);
    }

    @PostMapping("/release")
    public String releaseObject(@RequestParam("selectedObject") String selectedObject,
                                @RequestParam("ingestionMode") String ingestionMode,
                                @RequestParam(value = "orgKey", required = false) String requestedOrgKey,
                                Model model,
                                HttpSession session,
                                HttpServletResponse response) throws Exception {
        return doRelease(selectedObject, ingestionMode, requestedOrgKey, null, model, session, response);
    }

    @PostMapping("/dashboard/release")
    public String releaseFromDashboard(@RequestParam("selectedObject") String selectedObject,
                                      @RequestParam("ingestionMode") String ingestionMode,
                                      @RequestParam(value = "orgKey", required = false) String requestedOrgKey,
                                      Model model,
                                      HttpSession session,
                                      HttpServletResponse response) throws Exception {
        return doRelease(selectedObject, ingestionMode, requestedOrgKey, "/etl/dashboard", model, session, response);
    }

    private String doRelease(String selectedObject,
                            String ingestionMode,
                            String requestedOrgKey,
                            String redirectToDashboard,
                            Model model,
                            HttpSession session,
                            HttpServletResponse response) throws Exception {
        ExecutionContext context = resolveExecutionContext(session, requestedOrgKey);
        String accessToken = getValidAccessTokenForOperation(context, session, response);
        if (accessToken == null) {
            return null;
        }

        String sanitizedObject = RequestValidationUtils.requireIdentifier(selectedObject, "selectedObject");
        String sanitizedMode = RequestValidationUtils.requireText(ingestionMode, "ingestionMode").trim().toUpperCase();

        if (userSession.getUserAccount() != null) {
            model.addAttribute(userSession.getUserAccount().getMember());
        }

        String actor = currentActor();
        try {
            Map<String, Object> resultSummary = etlService.releaseObject(
                    sanitizedObject,
                    sanitizedMode,
                    accessToken,
                    actor,
                    context.myDomain,
                    context.org == null ? null : context.org.getOrgKey()
            );

            model.addAttribute("activeOrgKey", context.org == null ? null : context.org.getOrgKey());
            if (redirectToDashboard != null) {
                return "redirect:/etl/dashboard" + (context.org != null ? "?orgKey=" + context.org.getOrgKey() : "");
            }

            model.addAttribute("resultSummary", resultSummary);
            return "etl_result_summary";
        } catch (AppException e) {
            if (e.getMessage() != null && e.getMessage().contains("401") && context.org != null) {
                String refreshedAccessToken = tokenManager.refreshAccessToken(session, context.org);
                if (refreshedAccessToken != null) {
                    SalesforceTokenManager.RefreshResult refreshPair = tokenManager.refreshTokenPair(session, context.org);
                    if (refreshPair != null) {
                        salesforceOrgService.persistTokens(context.org.getOrgKey(), refreshPair.accessToken(), refreshPair.refreshToken());
                        tokenManager.setActiveOrg(session, context.org.getOrgKey());
                        tokenManager.setTokenPair(session, refreshPair.accessToken(), refreshPair.refreshToken());
                        context.accessToken = refreshPair.accessToken();
                    }
                    Map<String, Object> retrySummary = etlService.releaseObject(
                            sanitizedObject,
                            sanitizedMode,
                            (refreshPair == null ? refreshedAccessToken : refreshPair.accessToken()),
                            actor,
                            context.myDomain,
                            context.org.getOrgKey()
                    );

                    model.addAttribute("activeOrgKey", context.org == null ? null : context.org.getOrgKey());
                    if (redirectToDashboard != null) {
                        return "redirect:/etl/dashboard" + (context.org != null ? "?orgKey=" + context.org.getOrgKey() : "");
                    }

                    model.addAttribute("resultSummary", retrySummary);
                    return "etl_result_summary";
                }
            }
            throw e;
        }
    }

    @PostMapping("/ddl")
    public String setObjects(@RequestParam("selectedObject") String selectedObject,
                             @RequestParam("ingestionMode") String ingestionMode,
                             @RequestParam(value = "orgKey", required = false) String requestedOrgKey,
                             Model model,
                             HttpSession session,
                             HttpServletResponse response) throws Exception {
        ExecutionContext context = resolveExecutionContext(session, requestedOrgKey);
        String accessToken = getValidAccessTokenForOperation(context, session, response);
        if (accessToken == null) {
            return null;
        }

        String sanitizedObject = RequestValidationUtils.requireIdentifier(selectedObject, "selectedObject");
        String sanitizedMode = RequestValidationUtils.requireText(ingestionMode, "ingestionMode").trim().toUpperCase();

        String refreshToken = Optional.ofNullable(context.org == null ? tokenManager.getRefreshToken(session) : context.org.getRefreshToken())
                .filter(v -> v != null && !v.isBlank())
                .orElse(tokenManager.getRefreshToken(session));
        if (refreshToken == null || refreshToken.isBlank()) {
            log.debug("refresh token is empty while configuring object. continue with existing access token only: selectedObject={}, ingestionMode={}", selectedObject, sanitizedMode);
            refreshToken = "";
        }

        if (!"STREAMING".equals(sanitizedMode) && !"CDC".equals(sanitizedMode)) {
            throw new AppException("ingestionMode는 STREAMING 또는 CDC만 허용됩니다.");
        }

        if (userSession.getUserAccount() != null) {
            model.addAttribute(userSession.getUserAccount().getMember());
        }

        try {
            log.info("Submitting ETL object selection. selectedObject={}, ingestionMode={}", sanitizedObject, sanitizedMode);
            String actor = currentActor();
            model.addAttribute("activeOrgKey", context.org == null ? null : context.org.getOrgKey());
            Map<String, Object> resultSummary = etlService.setObjects(
                    sanitizedObject,
                    sanitizedMode,
                    accessToken,
                    refreshToken,
                    actor,
                    context.myDomain,
                    context.org == null ? null : context.org.getOrgKey(),
                    context.org == null ? "default" : context.org.getOrgName()
            );
            model.addAttribute("resultSummary", resultSummary);
            return "etl_result_summary";
        } catch (AppException e) {
            if (e.getMessage() != null && e.getMessage().contains("401") && context.org != null) {
                String refreshedAccessToken = tokenManager.refreshAccessToken(session, context.org);
                if (refreshedAccessToken != null) {
                    SalesforceTokenManager.RefreshResult refreshPair = tokenManager.refreshTokenPair(session, context.org);
                    if (refreshPair != null) {
                        salesforceOrgService.persistTokens(context.org.getOrgKey(), refreshPair.accessToken(), refreshPair.refreshToken());
                        tokenManager.setActiveOrg(session, context.org.getOrgKey());
                        tokenManager.setTokenPair(session, refreshPair.accessToken(), refreshPair.refreshToken());
                        context.accessToken = refreshPair.accessToken();
                    }
                    String resolvedRefreshToken = refreshPair == null ? refreshToken : refreshPair.refreshToken();
                    if (resolvedRefreshToken == null || resolvedRefreshToken.isBlank()) {
                        resolvedRefreshToken = tokenManager.getRefreshToken(session);
                    }
                    if (resolvedRefreshToken == null) {
                        resolvedRefreshToken = "";
                    }
                    String accessForRetry = refreshPair == null ? refreshedAccessToken : refreshPair.accessToken();
                    String actor = currentActor();
                    Map<String, Object> resultSummary = etlService.setObjects(
                            sanitizedObject,
                            sanitizedMode,
                            accessForRetry,
                            resolvedRefreshToken,
                            actor,
                            context.myDomain,
                            context.org.getOrgKey(),
                            context.org.getOrgName()
                    );
                    model.addAttribute("resultSummary", resultSummary);
                    return "etl_result_summary";
                }
                response.sendRedirect("/etl/orgs?message=need_org_auth&reason=refresh_failed&orgKey=" + context.org.getOrgKey());
                return null;
            }

            log.warn("ETL submission rejected. selectedObject={}, ingestionMode={}, message={}", sanitizedObject, sanitizedMode, e.getMessage());
            Map<String, Object> failureSummary = new HashMap<>();
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

    private ExecutionContext resolveExecutionContext(HttpSession session, String requestedOrgKey) throws Exception {
        ExecutionContext context = new ExecutionContext();
        context.myDomain = null;
        context.requiresOrgLogin = false;

        SalesforceOrgCredential selectedOrg = null;
        if (requestedOrgKey != null && !requestedOrgKey.isBlank()) {
            selectedOrg = salesforceOrgService.getOrg(requestedOrgKey);
            if (selectedOrg != null && Boolean.TRUE.equals(selectedOrg.getIsActive())) {
                context.org = selectedOrg;
                tokenManager.setActiveOrg(session, selectedOrg.getOrgKey());
            }
        }

        if (context.org == null) {
            String sessionOrgKey = tokenManager.getActiveOrgKey(session);
            if (sessionOrgKey != null && !sessionOrgKey.isBlank()) {
                selectedOrg = salesforceOrgService.getOrg(sessionOrgKey);
                if (selectedOrg != null && Boolean.TRUE.equals(selectedOrg.getIsActive())) {
                    context.org = selectedOrg;
                }
            }
        }

        if (context.org == null) {
            context.org = salesforceOrgService.getDefaultOrg();
            if (context.org != null) {
                tokenManager.setActiveOrg(session, context.org.getOrgKey());
            }
        }

        if (context.org != null) {
            String currentOrgKey = context.org.getOrgKey();
            context.myDomain = context.org.getMyDomain();
            String sessionActiveOrg = tokenManager.getActiveOrgKey(session);
            String sessionAccessToken = tokenManager.getAccessToken(session);

            // 1) 우선 세션 토큰 사용(현재 active org와 일치할 때)
            if (currentOrgKey.equals(sessionActiveOrg) && sessionAccessToken != null) {
                context.accessToken = sessionAccessToken;
            }

            // 2) 세션 토큰이 없거나 org가 바뀐 경우, DB에 저장된 최신 토큰 사용
            if (context.accessToken == null) {
                context.accessToken = context.org.getAccessToken();
                if (context.accessToken != null) {
                    String orgRefreshToken = context.org.getRefreshToken();
                    String sessionRefreshToken = tokenManager.getRefreshToken(session);
                    tokenManager.setAccessToken(session, context.accessToken);
                    if ((orgRefreshToken != null && !orgRefreshToken.isBlank()) || (sessionRefreshToken != null && !sessionRefreshToken.isBlank())) {
                        tokenManager.setActiveOrg(session, currentOrgKey);
                    }
                }
            }

            // 3) 그래도 없으면 refresh token으로 재발급 시도
            if (context.accessToken == null) {
                String refreshed = tokenManager.refreshAccessToken(session, context.org);
                if (refreshed != null) {
                    SalesforceTokenManager.RefreshResult refreshPair = tokenManager.refreshTokenPair(session, context.org);
                    if (refreshPair != null) {
                        salesforceOrgService.persistTokens(currentOrgKey, refreshPair.accessToken(), refreshPair.refreshToken());
                        context.accessToken = refreshPair.accessToken();
                        tokenManager.setActiveOrg(session, currentOrgKey);
                        tokenManager.setTokenPair(session, refreshPair.accessToken(), refreshPair.refreshToken());
                    } else {
                        context.accessToken = refreshed;
                    }
                } else {
                    context.requiresOrgLogin = true;
                }
            }

            if (context.accessToken == null) {
                tokenManager.setActiveOrg(session, currentOrgKey);
                log.warn("Active org token unavailable for org {}. Redirecting to org re-login flow.", currentOrgKey);
            } else if (currentOrgKey.equals(tokenManager.getActiveOrgKey(session)) == false) {
                tokenManager.setActiveOrg(session, currentOrgKey);
            }
        } else {
            context.accessToken = tokenManager.getAccessToken(session);
            if (context.accessToken == null) {
                context.requiresOrgLogin = true;
            }
        }

        return context;
    }

    private List<ObjectDefinition> fetchObjectDefinitions(ExecutionContext context, HttpSession session, HttpServletResponse response) throws Exception {
        try {
            return etlService.getObjects(context.accessToken, context.myDomain);
        } catch (AppException e) {
            boolean unauthorized = e.getMessage() != null && e.getMessage().contains("401");
            if (!unauthorized) {
                throw e;
            }

            SalesforceTokenManager.RefreshResult refreshPair = tokenManager.refreshTokenPair(session, context.org);
            if (refreshPair == null) {
                response.sendRedirect("/etl/orgs?message=need_org_auth&reason=refresh_failed" + (context.org != null ? "&orgKey=" + context.org.getOrgKey() : ""));
                return List.of();
            }

            if (context.org != null) {
                salesforceOrgService.persistTokens(context.org.getOrgKey(), refreshPair.accessToken(), refreshPair.refreshToken());
                context.accessToken = refreshPair.accessToken();
                tokenManager.setTokenPair(session, refreshPair.accessToken(), refreshPair.refreshToken());
            }
            
            
            tokenManager.setActiveOrg(session, context.org != null ? context.org.getOrgKey() : null);
            try {
                return etlService.getObjects(context.accessToken, context.myDomain);
            } catch (AppException retryFailure) {
                if (retryFailure.getMessage() != null && retryFailure.getMessage().contains("401")) {
                    response.sendRedirect("/etl/orgs?message=need_org_auth&reason=refresh_failed" + (context.org != null ? "&orgKey=" + context.org.getOrgKey() : ""));
                    return List.of();
                }
                throw retryFailure;
            }
        }
    }

    private String getValidAccessTokenForOperation(ExecutionContext context, HttpSession session, HttpServletResponse response) throws Exception {
        if (context.org != null) {
            String orgKey = context.org.getOrgKey();
            String currentAccess = context.accessToken;
            if (currentAccess != null) {
                return currentAccess;
            }

            String refreshed = tokenManager.refreshAccessToken(session, context.org);
            if (refreshed != null) {
                SalesforceTokenManager.RefreshResult refreshPair = tokenManager.refreshTokenPair(session, context.org);
                if (refreshPair != null) {
                    salesforceOrgService.persistTokens(orgKey, refreshPair.accessToken(), refreshPair.refreshToken());
                    tokenManager.setActiveOrg(session, orgKey);
                    tokenManager.setTokenPair(session, refreshPair.accessToken(), refreshPair.refreshToken());
                    context.accessToken = refreshPair.accessToken();
                    return refreshPair.accessToken();
                }
                tokenManager.setAccessToken(session, refreshed);
                context.accessToken = refreshed;
                return refreshed;
            }

            response.sendRedirect("/etl/orgs?message=need_org_auth&reason=refresh_failed" + "&orgKey=" + orgKey);
            return null;
        }

        response.sendRedirect("/etl/orgs?message=need_org_auth");
        return null;
    }


    private String currentActor() {
        return userSession.getUserAccount() != null ? userSession.getUserAccount().getMember().getUsername() : "system";
    }

    private String requireAccessToken(ExecutionContext context) {
        if (context.accessToken == null) {
            throw new AppException("세션이 만료되었습니다. 다시 Salesforce 로그인 해주세요.");
        }
        return context.accessToken;
    }

    private static class ExecutionContext {
        SalesforceOrgCredential org;
        String accessToken;
        String myDomain;
        boolean requiresOrgLogin;
    }
}
