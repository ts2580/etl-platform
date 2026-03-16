package com.apache.sfdc.common;

import com.apache.sfdc.common.RoutingRegistryRepository;
import com.apache.sfdc.pubsub.service.PubSubService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequiredArgsConstructor
public class RoutingDashboardController {

    private final RoutingRegistryRepository routingRegistryRepository;
    private final PubSubService pubSubService;

    @GetMapping("/routing/dashboard")
    public Map<String, Object> getDashboard(@RequestParam(value = "orgKey", required = false) String orgKey) {
        Map<String, Object> result = new HashMap<>();

        List<Map<String, Object>> activeRoutes = (orgKey == null || orgKey.isBlank())
                ? routingRegistryRepository.findActiveRoutes()
                : routingRegistryRepository.findActiveRoutesByOrg(orgKey);

        List<Map<String, Object>> activeOrgs = (orgKey == null || orgKey.isBlank())
                ? routingRegistryRepository.findActiveOrgs()
                : routingRegistryRepository.findActiveOrgsByOrg(orgKey);

        Map<String, Object> cdcSlotSummary = pubSubService.getSlotSummary("CDC");
        int used = asInt(cdcSlotSummary.get("used"));
        int limit = asInt(cdcSlotSummary.get("limit"));
        cdcSlotSummary.put("available", Math.max(limit - used, 0));
        cdcSlotSummary.put("message", "CDC는 최대 5개까지 운영할 수 있어요.");

        long streamingCount = activeRoutes.stream().filter(r -> "STREAMING".equals(String.valueOf(r.get("protocol")))).count();
        long cdcCount = activeRoutes.stream().filter(r -> "CDC".equals(String.valueOf(r.get("protocol")))).count();

        result.put("orgSummary", activeOrgs.isEmpty() ? defaultOrgSummary(orgKey == null || orgKey.isBlank() ? "-" : orgKey) : activeOrgs.get(0));
        result.put("activeOrgs", activeOrgs);
        result.put("activeOrgCount", activeOrgs.size());
        result.put("activeRoutes", activeRoutes);
        result.put("activeRouteCount", activeRoutes.size());
        result.put("streamingCount", streamingCount);
        result.put("cdcCount", cdcCount);
        result.put("cdcSlotSummary", cdcSlotSummary);
        result.put("refreshedAt", LocalDateTime.now().toString());
        return result;
    }

    private Map<String, Object> defaultOrgSummary(String fallbackOrgKey) {
        Map<String, Object> fallback = new HashMap<>();
        fallback.put("orgKey", fallbackOrgKey);
        fallback.put("orgName", "-");
        fallback.put("instanceName", "-");
        fallback.put("orgType", "-");
        fallback.put("isSandbox", false);
        fallback.put("myDomain", "-");
        return fallback;
    }

    private int asInt(Object value) {
        if (value == null) {
            return 0;
        }
        if (value instanceof Number n) {
            return n.intValue();
        }
        try {
            return Integer.parseInt(String.valueOf(value));
        } catch (NumberFormatException e) {
            return 0;
        }
    }
}
