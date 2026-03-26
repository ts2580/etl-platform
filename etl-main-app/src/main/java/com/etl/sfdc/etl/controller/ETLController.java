package com.etl.sfdc.etl.controller;

import com.etl.sfdc.config.model.dto.SalesforceOrgCredential;
import com.etl.sfdc.config.model.service.SalesforceOrgService;
import com.etl.sfdc.common.SalesforceTokenManager;
import com.etl.sfdc.common.UserSession;
import com.etl.sfdc.etl.dto.ObjectDefinition;
import com.etl.sfdc.etl.dto.ObjectSearchResult;
import com.etl.sfdc.etl.service.ETLService;
import com.etl.sfdc.storage.service.DatabaseStorageQueryService;
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

import java.util.Comparator;
import java.util.HashMap;
import java.util.Optional;
import java.util.List;
import java.util.Locale;
import java.util.Map;

@Controller
@RequiredArgsConstructor
@RequestMapping("etl")
@Slf4j
public class ETLController {



    private final UserSession userSession;
    private final SalesforceTokenManager tokenManager;
    private final ETLService etlService;
    private final DatabaseStorageQueryService databaseStorageQueryService;
    private final SalesforceOrgService salesforceOrgService;

    @GetMapping("/objects")
    public String getObjects(Model model, HttpSession session, HttpServletResponse response,
                            @RequestParam(value = "orgKey", required = false) String requestedOrgKey,
                            @RequestParam(value = "targetStorageId", required = false) Long requestedTargetStorageId,
                            @RequestParam(value = "q", required = false) String searchQuery,
                            @RequestParam(value = "selectedObject", required = false) String selectedObject,
                            @RequestParam(value = "status", required = false, defaultValue = "ALL") String statusFilter,
                            @RequestParam(value = "sort", required = false, defaultValue = "LABEL_ASC") String sort,
                            @RequestParam(value = "page", required = false, defaultValue = "1") Integer page,
                            @RequestParam(value = "size", required = false, defaultValue = "20") Integer size) throws Exception {
        ExecutionContext context = resolveExecutionContext(session, requestedOrgKey);
        if (context.accessToken == null) {
            log.info("ETL object list requested without access token. Redirecting to /etl/orgs");
            response.sendRedirect("/etl/orgs?message=need_org_auth" + (context.org != null ? "&orgKey=" + context.org.getOrgKey() + "&reason=refresh_failed" : ""));
            return null;
        }

        String actor = currentActor();
        String accessToken = ensureAccessToken(context, session, response);
        if (accessToken == null) {
            return null;
        }

        Map<String, String> ingestionStatusByObject = with401Retry(
                context,
                session,
                response,
                token -> etlService.getIngestionStatusByObject(token, context.myDomain)
        );
        if (ingestionStatusByObject == null) {
            return null;
        }

        Boolean syncResult = with401Retry(
                context,
                session,
                response,
                token -> {
                    etlService.syncRoutingRegistryFromSalesforce(token, actor, context.myDomain);
                    return Boolean.TRUE;
                }
        );
        if (syncResult == null) {
            return null;
        }

        String normalizedQuery = normalizeQuery(searchQuery);
        String normalizedSelectedObject = normalizeSelectedObject(selectedObject);
        String normalizedStatusFilter = normalizeStatusFilter(statusFilter);
        String normalizedSort = normalizeSort(sort);
        int pageSize = normalizePageSize(size);
        int requestedPage = page == null ? 1 : page;
        final Map<String, String> statusMap = ingestionStatusByObject;

        List<ObjectDefinition> allObjects = with401Retry(
                context,
                session,
                response,
                token -> etlService.getObjects(token, context.myDomain)
        );
        if (allObjects == null) {
            return null;
        }
        allObjects = new java.util.ArrayList<>(allObjects);

        List<ObjectDefinition> filteredObjects = allObjects.stream()
                .filter(object -> matchesSearch(object, normalizedQuery))
                .filter(object -> matchesStatusFilter(object, statusMap, normalizedStatusFilter))
                .sorted(resolveSortComparator(normalizedSort, statusMap))
                .toList();

        int totalObjectCount = filteredObjects.size();
        int totalPages = Math.max(1, (int) Math.ceil((double) Math.max(totalObjectCount, 1) / pageSize));
        int currentPage = Math.max(1, Math.min(requestedPage, totalPages));
        int fromIndex = totalObjectCount == 0 ? 0 : Math.min((currentPage - 1) * pageSize, totalObjectCount);
        int toIndex = totalObjectCount == 0 ? 0 : Math.min(fromIndex + pageSize, totalObjectCount);
        List<ObjectDefinition> pagedObjects = new java.util.ArrayList<>(filteredObjects);

        boolean selectedObjectExists = normalizedSelectedObject != null
                && filteredObjects.stream().anyMatch(object -> normalizedSelectedObject.equals(object.getName()));

        String defaultSelectedObject = selectedObjectExists
                ? normalizedSelectedObject
                : null;

        var routingTargetStorages = databaseStorageQueryService.getRoutingTargetOptions();
        Long selectedTargetStorageId = requestedTargetStorageId;
        if (selectedTargetStorageId == null && !routingTargetStorages.isEmpty()) {
            selectedTargetStorageId = routingTargetStorages.get(0).getId();
        }

        model.addAttribute("activeOrgs", salesforceOrgService.getActiveOrgs());
        model.addAttribute("activeOrgKey", context.org != null ? context.org.getOrgKey() : null);
        model.addAttribute("routingTargetStorages", routingTargetStorages);
        model.addAttribute("selectedTargetStorageId", selectedTargetStorageId);
        model.addAttribute("objectDefinitions", pagedObjects);
        model.addAttribute("cdcSlotSummary", etlService.getCdcSlotSummary());
        model.addAttribute("ingestionStatusByObject", ingestionStatusByObject);
        model.addAttribute("selectedObject", defaultSelectedObject);
        model.addAttribute("selectedIngestionMode", defaultSelectedObject != null ? ingestionStatusByObject.get(defaultSelectedObject) : null);
        model.addAttribute("activeOrgKey", context.org == null ? null : context.org.getOrgKey());
        model.addAttribute("searchQuery", normalizedQuery);
        model.addAttribute("selectedObjectQuery", defaultSelectedObject);
        model.addAttribute("selectedObjectVisible", filteredObjects.stream().anyMatch(object -> object.getName().equals(defaultSelectedObject)));
        model.addAttribute("selectedObjectStatus", defaultSelectedObject == null ? null : ingestionStatusByObject.get(defaultSelectedObject));
        model.addAttribute("statusFilter", normalizedStatusFilter);
        model.addAttribute("sort", normalizedSort);
        model.addAttribute("pageSize", pageSize);
        model.addAttribute("currentPage", currentPage);
        model.addAttribute("totalPages", totalPages);
        model.addAttribute("totalObjectCount", totalObjectCount);
        model.addAttribute("pageStartIndex", totalObjectCount == 0 ? 0 : fromIndex + 1);
        model.addAttribute("pageEndIndex", toIndex);
        model.addAttribute("pageNumbers", buildPageNumbers(currentPage, totalPages));

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
                    String refreshedAccessToken = refreshAccessTokenIfNeeded(context, session);
                    if (refreshedAccessToken != null) {
                        etlService.syncRoutingRegistryFromSalesforce(refreshedAccessToken, currentActor(), context.myDomain);
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
    public String getRouteDetail(@RequestParam(value = "selectedObject", required = false) String selectedObject,
                                 @RequestParam(value = "routingProtocol", required = false) String routingProtocol,
                                 @RequestParam(value = "orgKey", required = false) String requestedOrgKey,
                                 Model model,
                                 HttpSession session,
                                 HttpServletResponse response) throws Exception {
        if (selectedObject == null || selectedObject.isBlank() || routingProtocol == null || routingProtocol.isBlank()) {
            return redirectToDashboardWithMessage("route_detail_direct_access", requestedOrgKey);
        }
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
                    String refreshedAccessToken = refreshAccessTokenIfNeeded(context, session);
                    if (refreshedAccessToken == null && context.org != null) {
                        response.sendRedirect("/etl/orgs?message=need_org_auth&reason=refresh_failed&orgKey=" + context.org.getOrgKey());
                        return null;
                    }
                    if (refreshedAccessToken != null && context.org != null) {
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
                String refreshedAccessToken = refreshAccessTokenIfNeeded(context, session);
                if (refreshedAccessToken == null) {
                    response.sendRedirect("/etl/orgs?message=need_org_auth&reason=refresh_failed&orgKey=" + context.org.getOrgKey());
                    return null;
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
                                  @RequestParam(value = "targetStorageId", required = false) Long targetStorageId,
                                  @RequestParam(value = "orgKey", required = false) String requestedOrgKey,
                                  Model model,
                                  HttpSession session,
                                  HttpServletResponse response) throws Exception {
        return setObjects(selectedObject, ingestionMode, targetStorageId, requestedOrgKey, model, session, response);
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

            model.addAttribute("resultSummary", ensureResultSummary(
                    resultSummary,
                    sanitizedObject,
                    sanitizedMode,
                    null,
                    context.org == null ? null : context.org.getOrgKey(),
                    context.org == null ? null : context.org.getOrgName(),
                    "라우팅 엔진 응답이 비어있어요."
            ));
            return "etl_result_summary";
        } catch (AppException e) {
            if (e.getMessage() != null && e.getMessage().contains("401") && context.org != null) {
                String refreshedAccessToken = refreshAccessTokenIfNeeded(context, session);
                if (refreshedAccessToken != null) {
                    Map<String, Object> retrySummary = etlService.releaseObject(
                            sanitizedObject,
                            sanitizedMode,
                            refreshedAccessToken,
                            actor,
                            context.myDomain,
                            context.org.getOrgKey()
                    );

                    model.addAttribute("activeOrgKey", context.org == null ? null : context.org.getOrgKey());
                    if (redirectToDashboard != null) {
                        return "redirect:/etl/dashboard" + (context.org != null ? "?orgKey=" + context.org.getOrgKey() : "");
                    }

                    model.addAttribute("resultSummary", ensureResultSummary(
                            retrySummary,
                            sanitizedObject,
                            sanitizedMode,
                            null,
                            context.org.getOrgKey(),
                            context.org == null ? null : context.org.getOrgName(),
                            "재시도 결과가 비어있어요."
                    ));
                    return "etl_result_summary";
                }
            }
            throw e;
        }
    }

    @PostMapping("/ddl")
    public String setObjects(@RequestParam("selectedObject") String selectedObject,
                             @RequestParam("ingestionMode") String ingestionMode,
                             @RequestParam(value = "targetStorageId", required = false) Long targetStorageId,
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
                    targetStorageId,
                    accessToken,
                    actor,
                    context.myDomain,
                    context.org == null ? null : context.org.getOrgKey(),
                    context.org == null ? "default" : context.org.getOrgName()
            );
            model.addAttribute("resultSummary", ensureResultSummary(
                    resultSummary,
                    sanitizedObject,
                    sanitizedMode,
                    targetStorageId,
                    context.org == null ? null : context.org.getOrgKey(),
                    context.org == null ? null : context.org.getOrgName(),
                    "라우팅 엔진 응답이 비어있어요."
            ));
            return "etl_result_summary";
        } catch (AppException e) {
            if (e.getMessage() != null && e.getMessage().contains("401") && context.org != null) {
                String refreshedAccessToken = refreshAccessTokenIfNeeded(context, session);
                if (refreshedAccessToken != null) {
                    String actor = currentActor();
                    Map<String, Object> resultSummary = etlService.setObjects(
                            sanitizedObject,
                            sanitizedMode,
                            targetStorageId,
                            refreshedAccessToken,
                            actor,
                            context.myDomain,
                            context.org.getOrgKey(),
                            context.org.getOrgName()
                    );
                    model.addAttribute("resultSummary", ensureResultSummary(
                            resultSummary,
                            sanitizedObject,
                            sanitizedMode,
                            targetStorageId,
                            context.org.getOrgKey(),
                            context.org == null ? null : context.org.getOrgName(),
                            "재시도 결과가 비어있어요."
                    ));
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
            failureSummary.put("sourceOrgKey", context.org == null ? null : context.org.getOrgKey());
            failureSummary.put("sourceOrgName", context.org == null ? null : context.org.getOrgName());
            failureSummary.put("targetStorageId", targetStorageId);
            model.addAttribute("resultSummary", failureSummary);
            return "etl_result_summary";
        }
    }

    @GetMapping("/ddl")
    public String redirectDirectDdlAccess(@RequestParam(value = "orgKey", required = false) String requestedOrgKey) {
        return redirectToObjectsWithMessage("ddl_direct_access", requestedOrgKey);
    }

    private String redirectToObjectsWithMessage(String messageKey, String requestedOrgKey) {
        StringBuilder redirect = new StringBuilder("redirect:/etl/objects?message=").append(messageKey);
        if (requestedOrgKey != null && !requestedOrgKey.isBlank()) {
            redirect.append("&orgKey=").append(requestedOrgKey);
        }
        return redirect.toString();
    }

    private String redirectToDashboardWithMessage(String messageKey, String requestedOrgKey) {
        StringBuilder redirect = new StringBuilder("redirect:/etl/dashboard?message=").append(messageKey);
        if (requestedOrgKey != null && !requestedOrgKey.isBlank()) {
            redirect.append("&orgKey=").append(requestedOrgKey);
        }
        return redirect.toString();
    }

    private Map<String, Object> ensureResultSummary(Map<String, Object> resultSummary,
                                                   String selectedObject,
                                                   String ingestionMode,
                                                   Long targetStorageId,
                                                   String sourceOrgKey,
                                                   String sourceOrgName,
                                                   String fallbackMessage) {
        if (resultSummary != null) {
            return resultSummary;
        }

        Map<String, Object> fallback = new HashMap<>();
        fallback.put("selectedObject", selectedObject);
        fallback.put("ingestionMode", ingestionMode);
        fallback.put("endpoint", "CDC".equalsIgnoreCase(ingestionMode) ? "/pubsub" : "/streaming");
        fallback.put("status", "FAILED");
        fallback.put("message", "라우팅 응답이 비어 있었어요.");
        fallback.put("engineMessage", fallbackMessage);
        fallback.put("responseBody", fallbackMessage);
        fallback.put("initialLoadCount", 0);
        fallback.put("subscribeStatus", "NOT_STARTED");
        fallback.put("pushTopicStatus", "NOT_STARTED");
        fallback.put("cdcCreationStatus", "NOT_STARTED");
        fallback.put("cdcCreationMessage", "실패로 인해 CDC 생성 단계가 완료되지 않았어요.");
        fallback.put("failureStage", "UNKNOWN");
        fallback.put("failureDetail", fallbackMessage);
        fallback.put("slotRegistryStatus", "NOT_STARTED");
        fallback.put("sourceOrgKey", sourceOrgKey);
        fallback.put("sourceOrgName", sourceOrgName);
        fallback.put("targetStorageId", targetStorageId);

        return fallback;
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
                    tokenManager.setAccessToken(session, context.accessToken);
                    tokenManager.setActiveOrg(session, currentOrgKey);
                }
            }

            // 3) 그래도 없으면 client credentials으로 접근 토큰 갱신 시도
            if (context.accessToken == null) {
                String refreshed = refreshAccessTokenIfNeeded(context, session);
                if (refreshed == null) {
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

    private ObjectSearchResult fetchObjectSearchResult(ExecutionContext context,
                                                       HttpSession session,
                                                       HttpServletResponse response,
                                                       String query,
                                                       String sort,
                                                       int page,
                                                       int size) throws Exception {
        try {
            return etlService.searchObjects(context.accessToken, context.myDomain, query, sort, page, size);
        } catch (AppException e) {
            boolean unauthorized = isSalesforceAuthFailure(e);
            if (!unauthorized) {
                throw e;
            }

            String refreshedAccessToken = refreshAccessTokenIfNeeded(context, session);
            if (refreshedAccessToken == null) {
                response.sendRedirect("/etl/orgs?message=need_org_auth&reason=refresh_failed" + (context.org != null ? "&orgKey=" + context.org.getOrgKey() : ""));
                return null;
            }

            tokenManager.setActiveOrg(session, context.org != null ? context.org.getOrgKey() : null);
            try {
                return etlService.searchObjects(refreshedAccessToken, context.myDomain, query, sort, page, size);
            } catch (AppException retryFailure) {
                if (isSalesforceAuthFailure(retryFailure)) {
                    response.sendRedirect("/etl/orgs?message=need_org_auth&reason=refresh_failed" + (context.org != null ? "&orgKey=" + context.org.getOrgKey() : ""));
                    return null;
                }
                throw retryFailure;
            }
        }
    }

    private ObjectSearchResult applyStatusAwarePaging(ExecutionContext context,
                                                      HttpSession session,
                                                      HttpServletResponse response,
                                                      String query,
                                                      String sort,
                                                      String statusFilter,
                                                      int pageSize,
                                                      int requestedPage,
                                                      Map<String, String> ingestionStatusByObject) throws Exception {
        int resolvedPage = Math.max(1, requestedPage);
        int skip = (resolvedPage - 1) * pageSize;
        int fetchPage = 1;
        int sourcePageSize = Math.max(pageSize * 3, 100);
        int matchedTotal = 0;
        List<ObjectDefinition> pageObjects = new java.util.ArrayList<>();

        while (true) {
            ObjectSearchResult batch = fetchObjectSearchResult(context, session, response, query, sort, fetchPage, sourcePageSize);
            if (batch == null) {
                return null;
            }
            List<ObjectDefinition> sourceObjects = batch.getObjects() == null ? List.of() : batch.getObjects();
            if (sourceObjects.isEmpty()) {
                break;
            }

            for (ObjectDefinition object : sourceObjects) {
                if (!matchesStatusFilter(object, ingestionStatusByObject, statusFilter)) {
                    continue;
                }
                if (matchedTotal >= skip && pageObjects.size() < pageSize) {
                    pageObjects.add(object);
                }
                matchedTotal++;
            }

            if (sourceObjects.size() < sourcePageSize) {
                break;
            }
            fetchPage++;
        }

        return new ObjectSearchResult(pageObjects, matchedTotal, resolvedPage, pageSize, true);
    }

    private boolean isSalesforceAuthFailure(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            String message = current.getMessage();
            if (message != null) {
                String upper = message.toUpperCase(Locale.ROOT);
                if (upper.contains("401")
                        || upper.contains("INVALID_SESSION_ID")
                        || upper.contains("SESSION EXPIRED")
                        || upper.contains("EXPIRED ACCESS TOKEN")
                        || upper.contains("AUTHENTICATION FAILURE")) {
                    return true;
                }
            }
            current = current.getCause();
        }
        return false;
    }

    private String getValidAccessTokenForOperation(ExecutionContext context, HttpSession session, HttpServletResponse response) throws Exception {
        if (context.org != null) {
            String orgKey = context.org.getOrgKey();
            String currentAccess = context.accessToken;
            if (currentAccess != null) {
                return currentAccess;
            }

            String refreshed = refreshAccessTokenIfNeeded(context, session);
            if (refreshed != null) {
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

    @FunctionalInterface
    private interface TokenOperation<T> {
        T apply(String accessToken) throws Exception;
    }

    private <T> T with401Retry(ExecutionContext context,
                               HttpSession session,
                               HttpServletResponse response,
                               TokenOperation<T> operation) throws Exception {
        String accessToken = ensureAccessToken(context, session, response);
        if (accessToken == null) {
            return null;
        }

        try {
            return operation.apply(accessToken);
        } catch (AppException e) {
            if (isSalesforceAuthFailure(e)) {
                String refreshedAccessToken = refreshAccessTokenIfNeeded(context, session);
                if (refreshedAccessToken != null) {
                    return operation.apply(refreshedAccessToken);
                }
                response.sendRedirect("/etl/orgs?message=need_org_auth&reason=refresh_failed" + (context.org != null ? "&orgKey=" + context.org.getOrgKey() : ""));
                return null;
            }
            throw e;
        }
    }

    private String ensureAccessToken(ExecutionContext context, HttpSession session, HttpServletResponse response) throws Exception {
        if (context.accessToken != null && !context.accessToken.isBlank()) {
            return context.accessToken;
        }
        String refreshedAccessToken = refreshAccessTokenIfNeeded(context, session);
        if (refreshedAccessToken != null) {
            return refreshedAccessToken;
        }
        response.sendRedirect("/etl/orgs?message=need_org_auth&reason=refresh_failed" + (context.org != null ? "&orgKey=" + context.org.getOrgKey() : ""));
        return null;
    }

    private String normalizeQuery(String searchQuery) {
        return searchQuery == null ? "" : searchQuery.trim();
    }

    private String normalizeSelectedObject(String selectedObject) {
        if (selectedObject == null || selectedObject.isBlank()) {
            return null;
        }
        return selectedObject.trim();
    }

    private String normalizeStatusFilter(String statusFilter) {
        if (statusFilter == null || statusFilter.isBlank()) {
            return "ALL";
        }
        String normalized = statusFilter.trim().toUpperCase(Locale.ROOT);
        return switch (normalized) {
            case "NONE", "STREAMING", "CDC" -> normalized;
            default -> "ALL";
        };
    }

    private String normalizeSort(String sort) {
        if (sort == null || sort.isBlank()) {
            return "LABEL_ASC";
        }
        String normalized = sort.trim().toUpperCase(Locale.ROOT);
        return switch (normalized) {
            case "LABEL_ASC", "LABEL_DESC", "API_ASC", "API_DESC", "STATUS_ASC", "STATUS_DESC" -> normalized;
            default -> "LABEL_ASC";
        };
    }

    private int normalizePageSize(Integer size) {
        if (size == null) {
            return 20;
        }
        return switch (size) {
            case 10, 20, 50, 100 -> size;
            default -> 20;
        };
    }

    private Comparator<ObjectDefinition> resolveSortComparator(String sort, Map<String, String> ingestionStatusByObject) {
        Comparator<ObjectDefinition> labelComparator = Comparator.comparing(
                (ObjectDefinition object) -> Optional.ofNullable(object.getLabel()).orElse(""),
                String.CASE_INSENSITIVE_ORDER
        );
        Comparator<ObjectDefinition> apiComparator = Comparator.comparing(
                (ObjectDefinition object) -> Optional.ofNullable(object.getName()).orElse(""),
                String.CASE_INSENSITIVE_ORDER
        );
        Comparator<ObjectDefinition> statusComparator = Comparator.comparing(
                (ObjectDefinition object) -> Optional.ofNullable(ingestionStatusByObject.get(object.getName())).orElse("NONE"),
                String.CASE_INSENSITIVE_ORDER
        ).thenComparing(labelComparator);

        return switch (sort) {
            case "LABEL_DESC" -> labelComparator.reversed().thenComparing(apiComparator);
            case "API_ASC" -> apiComparator.thenComparing(labelComparator);
            case "API_DESC" -> apiComparator.reversed().thenComparing(labelComparator);
            case "STATUS_ASC" -> statusComparator;
            case "STATUS_DESC" -> statusComparator.reversed();
            default -> labelComparator.thenComparing(apiComparator);
        };
    }

    private List<Integer> buildPageNumbers(int currentPage, int totalPages) {
        int window = 2;
        int start = Math.max(1, currentPage - window);
        int end = Math.min(totalPages, currentPage + window);
        if (end - start < window * 2) {
            if (start == 1) {
                end = Math.min(totalPages, start + window * 2);
            } else if (end == totalPages) {
                start = Math.max(1, end - window * 2);
            }
        }
        return java.util.stream.IntStream.rangeClosed(start, end).boxed().toList();
    }

    private boolean matchesSearch(ObjectDefinition object, String normalizedQuery) {
        if (normalizedQuery == null || normalizedQuery.isBlank()) {
            return true;
        }
        String query = normalizedQuery.toLowerCase(Locale.ROOT);
        return Optional.ofNullable(object.getLabel()).orElse("").toLowerCase(Locale.ROOT).contains(query)
                || Optional.ofNullable(object.getName()).orElse("").toLowerCase(Locale.ROOT).contains(query);
    }

    private boolean matchesStatusFilter(ObjectDefinition object, Map<String, String> ingestionStatusByObject, String statusFilter) {
        String status = ingestionStatusByObject.get(object.getName());
        return switch (statusFilter) {
            case "NONE" -> status == null || status.isBlank();
            case "STREAMING", "CDC" -> statusFilter.equalsIgnoreCase(status);
            default -> true;
        };
    }

    private String refreshAccessTokenIfNeeded(ExecutionContext context, HttpSession session) {
        String actor = currentActor();
        String caller = detectCallerMethod();
        if (context == null || context.org == null) {
            log.warn("[TOKEN-REFRESH] Skip refresh request. reason=missing_context, caller={}, actor={}, org=unknown", caller, actor);
            return null;
        }

        String orgKey = context.org.getOrgKey();
        log.info("[TOKEN-REFRESH] Start. caller={}, actor={}, orgKey={}, myDomain={}", caller, actor, orgKey, context.myDomain);

        SalesforceTokenManager.RefreshResult refreshedTokens = tokenManager.refreshClientCredentialsToken(session, context.org);
        if (refreshedTokens == null || refreshedTokens.accessToken() == null || refreshedTokens.accessToken().isBlank()) {
            log.warn("[TOKEN-REFRESH] Failed. caller={}, actor={}, orgKey={} (token refresh returned empty)", caller, actor, orgKey);
            return null;
        }

        salesforceOrgService.storeAccessToken(context.org.getOrgKey(), refreshedTokens.accessToken());
        tokenManager.setActiveOrg(session, context.org.getOrgKey());
        tokenManager.setAccessToken(session, refreshedTokens.accessToken());
        context.accessToken = refreshedTokens.accessToken();
        log.info("[TOKEN-REFRESH] SUCCESS. caller={}, actor={}, orgKey={}, cause=token_expired", caller, actor, orgKey);

        try {
            etlService.refreshRoutingModuleCredentials(
                    refreshedTokens.accessToken(),
                    context.myDomain,
                    context.org.getOrgKey()
            );
            log.info("[TOKEN-REFRESH] Routing module credentials refreshed. caller={}, actor={}, orgKey={}", caller, actor, orgKey);
        } catch (Exception e) {
            log.warn("[TOKEN-REFRESH] Failed to refresh routing module credentials. caller={}, actor={}, orgKey={}", caller, actor, context.org.getOrgKey());
        }

        return refreshedTokens.accessToken();
    }

    private String detectCallerMethod() {
        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        for (StackTraceElement element : stackTrace) {
            String className = element.getClassName();
            String methodName = element.getMethodName();
            if (className.endsWith("ETLController")
                    && !"refreshAccessTokenIfNeeded".equals(methodName)
                    && !"detectCallerMethod".equals(methodName)
                    && !"getClass".equals(methodName)) {
                return className.substring(className.lastIndexOf('.') + 1) + "." + methodName + "(" + element.getLineNumber() + ")";
            }
        }
        return "Unknown";
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
