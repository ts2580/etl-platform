package com.etl.sfdc.etl.service;

import com.etl.sfdc.config.model.repository.RoutingDashboardRepository;
import com.etl.sfdc.config.model.service.SalesforceOrgService;
import com.etl.sfdc.etl.dto.ObjectDefinition;
import com.etlplatform.common.error.AppException;
import com.etlplatform.common.validation.RequestValidationUtils;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import okhttp3.*;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class ETLServiceImpl implements ETLService {

    private final RoutingDashboardRepository routingDashboardRepository;
    private final SalesforceOrgService salesforceOrgService;

    @org.springframework.beans.factory.annotation.Value("${routing.engine.base-url:https://localhost:${ROUTING_APP_PORT:3931}}")
    private String routingEngineBaseUrl;

    @Override
    public List<ObjectDefinition> getObjects(String accessToken, String myDomain) throws Exception {
        List<ObjectDefinition> listDef = new ArrayList<>();
        String resolvedMyDomain = resolveSalesforceDomain(myDomain);
        OkHttpClient client = new OkHttpClient();

        Request request = new Request.Builder()
                .url(resolvedMyDomain + "/services/data/v63.0/sobjects")
                .addHeader("Authorization", "Bearer " + accessToken)
                .addHeader("Content-Type", "application/json")
                .build();

        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new AppException("오브젝트 목록 조회 실패: " + response.code() + " " + response.message());
            }

            String responseBody = Objects.requireNonNull(response.body()).string();
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode rootNode = objectMapper.readTree(responseBody);
            JsonNode sobjects = rootNode.get("sobjects");
            listDef = objectMapper.convertValue(sobjects, new TypeReference<List<ObjectDefinition>>() {});
            log.info("Fetched Salesforce objects successfully. count={}", listDef.size());
        } catch (IOException e) {
            throw new AppException("Salesforce object 조회 중 오류 발생", e);
        }

        return listDef;
    }

    @Override
    public Map<String, Object> getCdcSlotSummary() {
        OkHttpClient client = new OkHttpClient.Builder()
                .connectTimeout(5, TimeUnit.SECONDS)
                .readTimeout(5, TimeUnit.SECONDS)
                .writeTimeout(5, TimeUnit.SECONDS)
                .build();

        String resolvedRoutingBaseUrl = resolveRoutingBaseUrl();
        Request request = new Request.Builder()
                .url(resolvedRoutingBaseUrl + "/pubsub/slots/summary")
                .get()
                .addHeader("Content-Type", "application/json")
                .build();

        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                log.warn("Failed to fetch CDC slot summary. status={}, message={}", response.code(), response.message());
                return defaultCdcSlotSummary("CDC 슬롯 정보를 지금은 불러오지 못했어요.");
            }

            String responseBody = response.body() != null ? response.body().string() : "{}";
            ObjectMapper objectMapper = new ObjectMapper();
            Map<String, Object> result = objectMapper.readValue(responseBody, new TypeReference<Map<String, Object>>() {});
            result.putIfAbsent("limit", 5);
            result.putIfAbsent("message", "CDC는 최대 5개까지 운영할 수 있어요.");
            result.put("available", Math.max(asInt(result.get("limit")) - asInt(result.get("used")), 0));
            return result;
        } catch (Exception e) {
            log.warn("Error while fetching CDC slot summary", e);
            return defaultCdcSlotSummary("CDC 슬롯 정보를 지금은 불러오지 못했어요.");
        }
    }

    @Override
    public Map<String, String> getIngestionStatusByObject(String accessToken, String myDomain) throws Exception {
        Map<String, String> statusByObject = new HashMap<>();
        String resolvedMyDomain = resolveSalesforceDomain(myDomain);

        OkHttpClient client = new OkHttpClient.Builder()
                .connectTimeout(10, TimeUnit.SECONDS)
                .readTimeout(10, TimeUnit.SECONDS)
                .writeTimeout(10, TimeUnit.SECONDS)
                .build();
        ObjectMapper objectMapper = new ObjectMapper();

        Request pushTopicRequest = new Request.Builder()
                .url(resolvedMyDomain + "/services/data/v63.0/query/?q=" + URLEncoder.encode("SELECT Name FROM PushTopic", StandardCharsets.UTF_8))
                .addHeader("Authorization", "Bearer " + accessToken)
                .addHeader("Content-Type", "application/json")
                .build();

        try (Response response = client.newCall(pushTopicRequest).execute()) {
            if (response.isSuccessful() && response.body() != null) {
                JsonNode records = objectMapper.readTree(response.body().string()).path("records");
                if (records.isArray()) {
                    for (JsonNode record : records) {
                        String objectName = record.path("Name").asText("");
                        if (!objectName.isBlank()) {
                            statusByObject.put(objectName, "STREAMING");
                        }
                    }
                }
            }
        }

        String cdcQuery = "SELECT SelectedEntity FROM PlatformEventChannelMember WHERE SelectedEntity LIKE '%ChangeEvent'";
        Request cdcRequest = new Request.Builder()
                .url(resolvedMyDomain + "/services/data/v63.0/tooling/query/?q=" + URLEncoder.encode(cdcQuery, StandardCharsets.UTF_8))
                .addHeader("Authorization", "Bearer " + accessToken)
                .addHeader("Content-Type", "application/json")
                .build();

        try (Response response = client.newCall(cdcRequest).execute()) {
            if (response.isSuccessful() && response.body() != null) {
                JsonNode records = objectMapper.readTree(response.body().string()).path("records");
                if (records.isArray()) {
                    for (JsonNode record : records) {
                        String selectedEntity = record.path("SelectedEntity").asText("");
                        String objectName = fromChangeEventEntity(selectedEntity);
                        if (!objectName.isBlank()) {
                            statusByObject.put(objectName, "CDC");
                        }
                    }
                }
            }
        }

        return statusByObject;
    }

    @Override
    public void syncRoutingRegistryFromSalesforce(String accessToken, String actor, String myDomain) throws Exception {
        String resolvedMyDomain = resolveSalesforceDomain(myDomain);
        String orgKey = extractHost(resolvedMyDomain);
        String targetSchema = salesforceOrgService.resolveSchemaName(orgKey);
        Map<String, String> ingestionStatusByObject = getIngestionStatusByObject(accessToken, resolvedMyDomain);
        if (ingestionStatusByObject.isEmpty()) {
            return;
        }

        List<ObjectDefinition> objectDefinitions = getObjects(accessToken, resolvedMyDomain);
        Map<String, String> labelByObject = objectDefinitions.stream()
                .filter(Objects::nonNull)
                .collect(Collectors.toMap(ObjectDefinition::getName, ObjectDefinition::getLabel, (left, right) -> left));

        Map<String, Object> orgSummary = getOrgSummary(accessToken, resolvedMyDomain);
        String resolvedOrgKey = orgKey != null ? orgKey : extractHost(myDomain);
        String orgName = String.valueOf(orgSummary.getOrDefault("orgName", orgKey));
        String instanceName = String.valueOf(orgSummary.getOrDefault("instanceName", extractHost(resolvedMyDomain)));
        String orgType = String.valueOf(orgSummary.getOrDefault("orgType", "-"));
        boolean sandbox = Boolean.parseBoolean(String.valueOf(orgSummary.getOrDefault("sandbox", false)));

        java.sql.Timestamp syncTime = new java.sql.Timestamp(System.currentTimeMillis());
        LocalDateTime syncAt = syncTime.toLocalDateTime();
        for (Map.Entry<String, String> entry : ingestionStatusByObject.entrySet()) {
            Map<String, Object> registry = new HashMap<>();
            registry.put("orgKey", resolvedOrgKey);
            registry.put("orgName", orgName);
            registry.put("myDomain", resolvedMyDomain);
            registry.put("targetSchema", targetSchema);
            registry.put("targetTable", entry.getKey());
            registry.put("instanceName", instanceName);
            registry.put("orgType", orgType);
            registry.put("sandbox", sandbox);
            registry.put("selectedObject", entry.getKey());
            registry.put("objectLabel", labelByObject.getOrDefault(entry.getKey(), entry.getKey()));
            registry.put("routingProtocol", entry.getValue());
            registry.put("routingEndpoint", "CDC".equals(entry.getValue()) ? "/pubsub" : "/streaming");
            registry.put("routingStatus", "ACTIVE");
            registry.put("sourceStatus", "ACTIVE");
            registry.put("initialLoadCount", 0);
            registry.put("lastErrorMessage", null);
            registry.put("activatedAt", null);
            registry.put("releasedAt", null);
            registry.put("lastSyncedAt", syncAt);
            registry.put("createdBy", actor);
            registry.put("updatedBy", actor);
            routingDashboardRepository.upsertRoutingRegistry(registry);
        }
        routingDashboardRepository.deactivateRoutesNotInSync(resolvedOrgKey, resolvedMyDomain, java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(syncAt));
    }

    @Override
    public Map<String, Object> getRouteDetail(String accessToken, String myDomain, String orgKey, String selectedObject, String routingProtocol) throws Exception {
        String sanitizedObject = RequestValidationUtils.requireIdentifier(selectedObject, "selectedObject");
        String sanitizedMode = RequestValidationUtils.requireText(routingProtocol, "routingProtocol").trim().toUpperCase(Locale.ROOT);
        String resolvedOrgKey = orgKey != null && !orgKey.isBlank() ? orgKey : extractHost(resolveSalesforceDomain(myDomain));

        Map<String, Object> routeDetail = routingDashboardRepository.findRouteDetail(resolvedOrgKey, sanitizedObject, sanitizedMode);
        if (routeDetail == null || routeDetail.isEmpty()) {
            throw new AppException("요청한 라우팅 상세를 찾지 못했어요. 이미 해지되었거나 동기화 전일 수 있어요.");
        }

        int syncedRecordCount = 0;
        try {
            String targetSchema = String.valueOf(routeDetail.getOrDefault("targetSchema", salesforceOrgService.resolveSchemaName(resolvedOrgKey)));
            String targetTable = String.valueOf(routeDetail.getOrDefault("targetTable", sanitizedObject));
            Integer count = routingDashboardRepository.countObjectRows(targetSchema, targetTable);
            syncedRecordCount = count == null ? 0 : count;
        } catch (Exception e) {
            log.warn("Failed to count synced object rows. object={}, message={}", sanitizedObject, e.getMessage());
        }

        List<Map<String, Object>> fieldDefinitions = new ArrayList<>();
        try {
            fieldDefinitions = getObjectFieldDefinitions(accessToken, resolveSalesforceDomain(myDomain), sanitizedObject);
        } catch (Exception e) {
            log.warn("Failed to fetch route field definitions. selectedObject={}, message={}", sanitizedObject, e.getMessage());
        }

        routeDetail.put("syncedRecordCount", syncedRecordCount);
        routeDetail.put("fieldDefinitions", fieldDefinitions);
        routeDetail.put("fieldLoadWarning", fieldDefinitions.isEmpty() ? "필드 메타데이터를 지금은 불러오지 못했어요." : null);
        routeDetail.put("recordCountWarning", syncedRecordCount == 0 ? "동기화 레코드 수가 0이거나 아직 집계되지 않았어요." : null);
        return routeDetail;
    }

    @Override
    public Map<String, Object> getRoutingDashboard(String orgKey) {
        OkHttpClient client = new OkHttpClient.Builder()
                .connectTimeout(5, TimeUnit.SECONDS)
                .readTimeout(5, TimeUnit.SECONDS)
                .writeTimeout(5, TimeUnit.SECONDS)
                .build();

        StringBuilder url = new StringBuilder(resolveRoutingBaseUrl()).append("/routing/dashboard");
        if (orgKey != null && !orgKey.isBlank()) {
            url.append("?orgKey=").append(java.net.URLEncoder.encode(orgKey, StandardCharsets.UTF_8));
        }

        Request request = new Request.Builder()
                .url(url.toString())
                .get()
                .addHeader("Content-Type", "application/json")
                .build();

        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new AppException("라우팅 대시보드 조회 실패: " + response.code() + " " + response.message());
            }

            String responseBody = response.body() != null ? response.body().string() : "{}";
            ObjectMapper objectMapper = new ObjectMapper();
            Map<String, Object> result = objectMapper.readValue(responseBody, new TypeReference<Map<String, Object>>() {});

            Object rawOrgSummary = result.get("orgSummary");
            if (rawOrgSummary instanceof Map<?, ?>) {
                @SuppressWarnings("unchecked")
                Map<String, Object> orgSummary = (Map<String, Object>) rawOrgSummary;
                if (orgSummary.get("sandbox") == null && orgSummary.get("isSandbox") != null) {
                    orgSummary.put("sandbox", orgSummary.get("isSandbox"));
                }
                if (orgSummary.isEmpty()) {
                    result.put("orgSummary", defaultOrgSummary(orgKey == null || orgKey.isBlank() ? "-" : orgKey));
                }
            }
            if (result.get("orgSummary") == null || !(result.get("orgSummary") instanceof Map)) {
                result.put("orgSummary", defaultOrgSummary(orgKey == null || orgKey.isBlank() ? "-" : orgKey));
            }
            if (!(result.get("activeRoutes") instanceof List)) {
                result.put("activeRoutes", new ArrayList<>());
            }
            if (!(result.get("activeOrgs") instanceof List)) {
                result.put("activeOrgs", new ArrayList<>());
            }

            List<Map<String, Object>> activeRoutes = ((List<?>) result.get("activeRoutes")).stream()
                    .filter(Map.class::isInstance)
                    .map(it -> (Map<String, Object>) it)
                    .collect(java.util.stream.Collectors.toList());
            List<Map<String, Object>> activeOrgs = ((List<?>) result.get("activeOrgs")).stream()
                    .filter(Map.class::isInstance)
                    .map(it -> (Map<String, Object>) it)
                    .collect(java.util.stream.Collectors.toList());

            result.put("activeRouteCount", activeRoutes.size());
            result.put("activeOrgCount", activeOrgs.size());
            result.put("streamingCount", activeRoutes.stream().filter(route -> "STREAMING".equals(String.valueOf(route.get("protocol")))).count());
            result.put("cdcCount", activeRoutes.stream().filter(route -> "CDC".equals(String.valueOf(route.get("protocol")))).count());

            if (result.get("cdcSlotSummary") == null || !(result.get("cdcSlotSummary") instanceof Map)) {
                result.put("cdcSlotSummary", defaultCdcSlotSummary("CDC 슬롯 정보를 조회하지 못해 기본값으로 보여줘요."));
            }

            return result;
        } catch (Exception e) {
            log.warn("Failed to fetch routing dashboard from routing engine. fallback to local db. orgKey={}", orgKey, e);
            return getRoutingDashboardFromDb(orgKey);
        }
    }

    private Map<String, Object> getRoutingDashboardFromDb(String orgKey) {
        List<Map<String, Object>> activeRoutes = orgKey == null ? routingDashboardRepository.findActiveRoutes() : routingDashboardRepository.findActiveRoutesByOrg(orgKey);
        List<Map<String, Object>> activeOrgs = orgKey == null ? routingDashboardRepository.findActiveOrgs() : routingDashboardRepository.findActiveOrgsByOrg(orgKey);
        Map<String, Object> cdcSlotSummary = getCdcSlotSummary();

        Map<String, Object> result = new HashMap<>();
        result.put("orgSummary", orgKey == null ? (activeOrgs.isEmpty() ? defaultOrgSummary("-") : activeOrgs.get(0)) : (activeOrgs.isEmpty() ? defaultOrgSummary("-") : activeOrgs.get(0)));
        result.put("activeOrgs", activeOrgs);
        result.put("activeOrgCount", activeOrgs.size());
        result.put("cdcSlotSummary", cdcSlotSummary);
        result.put("activeRoutes", activeRoutes);
        result.put("activeRouteCount", activeRoutes.size());
        result.put("streamingCount", activeRoutes.stream().filter(route -> "STREAMING".equals(String.valueOf(route.get("protocol")))).count());
        result.put("cdcCount", activeRoutes.stream().filter(route -> "CDC".equals(String.valueOf(route.get("protocol")))).count());
        return result;
    }

    @Override
    public Map<String, Object> setObjects(String selectedObject, String ingestionMode, String accessToken, String refreshToken, String actor, String myDomain, String orgKey, String orgName) throws Exception {
        String sanitizedObject = RequestValidationUtils.requireIdentifier(selectedObject, "selectedObject");
        String sanitizedMode = RequestValidationUtils.requireText(ingestionMode, "ingestionMode").trim().toUpperCase(Locale.ROOT);
        ObjectMapper objectMapper = new ObjectMapper();

        if (!"STREAMING".equals(sanitizedMode) && !"CDC".equals(sanitizedMode)) {
            throw new AppException("지원하지 않는 적재 모드입니다: " + sanitizedMode);
        }

        if ("CDC".equals(sanitizedMode)) {
            Map<String, Object> cdcSlotSummary = getCdcSlotSummary();
            int available = asInt(cdcSlotSummary.get("available"));
            if (available <= 0) {
                throw new AppException("현재 남은 CDC 슬롯이 없어서 CDC로는 등록할 수 없습니다. Streaming을 사용하거나 슬롯을 먼저 비워주세요.");
            }
        }

        OkHttpClient client = new OkHttpClient.Builder()
                .connectTimeout(30, TimeUnit.SECONDS)
                .readTimeout(30, TimeUnit.SECONDS)
                .writeTimeout(30, TimeUnit.SECONDS)
                .build();

        String resolvedMyDomain = resolveSalesforceDomain(myDomain);
        String resolvedOrgKey = orgKey == null || orgKey.isBlank() ? extractHost(resolvedMyDomain) : orgKey;
        String resolvedOrgName = orgName == null || orgName.isBlank() ? extractHost(resolvedMyDomain) : orgName;
        String targetSchema = salesforceOrgService.resolveSchemaName(resolvedOrgKey);
        String json = objectMapper.writeValueAsString(getPropertyMap(sanitizedObject, accessToken, refreshToken, actor, resolvedMyDomain, resolvedOrgName, resolvedOrgKey, targetSchema));
        String endpoint = "CDC".equals(sanitizedMode) ? "/pubsub" : "/streaming";
        String modeLabel = "CDC".equals(sanitizedMode) ? "CDC" : "Streaming";
        String routingBase = resolveRoutingBaseUrl();
        log.info("Calling routing engine. selectedObject={}, ingestionMode={}, endpoint={}", sanitizedObject, sanitizedMode, endpoint);

        RequestBody formBody = RequestBody.create(json, MediaType.get("application/json; charset=utf-8"));
        Request request = new Request.Builder()
                .url(routingBase + endpoint)
                .post(formBody)
                .addHeader("Content-Type", "application/json")
                .build();

        try (Response response = client.newCall(request).execute()) {
            String responseBody = response.body() != null ? response.body().string() : "";
            if (!response.isSuccessful()) {
                log.warn("Routing engine returned failure. selectedObject={}, ingestionMode={}, status={}, body={}", sanitizedObject, sanitizedMode, response.code(), responseBody);
                throw new AppException(modeLabel + " API 호출 실패: " + response.code() + " " + response.message() + " / " + responseBody);
            }
            log.info("Routing engine call completed. selectedObject={}, ingestionMode={}, response={}", sanitizedObject, sanitizedMode, responseBody);

            Map<String, Object> result = new HashMap<>();
            result.put("selectedObject", sanitizedObject);
            result.put("ingestionMode", sanitizedMode);
            result.put("endpoint", endpoint);
            result.put("status", "SUCCESS");
            result.put("message", modeLabel + " 설정이 완료되었어요.");
            result.put("responseBody", responseBody == null || responseBody.isBlank() ? "(empty)" : responseBody);

            Map<String, Object> engineResult = parseEngineResponse(responseBody);
            result.put("engineResult", engineResult);
            result.put("initialLoadCount", asInt(engineResult.get("initialLoadCount")));
            result.put("subscribeStatus", String.valueOf(engineResult.getOrDefault("subscribeStatus", "STARTED")));
            result.put("pushTopicStatus", String.valueOf(engineResult.getOrDefault("pushTopicStatus", "-")));
            result.put("cdcCreationStatus", String.valueOf(engineResult.getOrDefault("cdcCreationStatus", "-"))
            );
            result.put("cdcCreationMessage", String.valueOf(engineResult.getOrDefault("cdcCreationMessage", "-")));
            result.put("engineMessage", String.valueOf(engineResult.getOrDefault("message", modeLabel + " 설정이 완료되었어요.")));
            return result;
        } catch (AppException e) {
            throw e;
        } catch (Exception e) {
            throw new AppException(modeLabel + " API 호출 중 오류 발생", e);
        }
    }

    @Override
    public Map<String, Object> releaseObject(String selectedObject, String ingestionMode, String accessToken, String actor, String myDomain, String orgKey) throws Exception {
        String sanitizedObject = RequestValidationUtils.requireIdentifier(selectedObject, "selectedObject");
        String sanitizedMode = RequestValidationUtils.requireText(ingestionMode, "ingestionMode").trim().toUpperCase(Locale.ROOT);
        if (!"STREAMING".equals(sanitizedMode) && !"CDC".equals(sanitizedMode)) {
            throw new AppException("지원하지 않는 적재 모드입니다: " + sanitizedMode);
        }

        OkHttpClient client = new OkHttpClient.Builder()
                .connectTimeout(15, TimeUnit.SECONDS)
                .readTimeout(15, TimeUnit.SECONDS)
                .writeTimeout(15, TimeUnit.SECONDS)
                .build();
        ObjectMapper objectMapper = new ObjectMapper();

        int releasedCount = "STREAMING".equals(sanitizedMode)
                ? releaseStreaming(client, objectMapper, sanitizedObject, accessToken, myDomain)
                : releaseCdc(client, objectMapper, sanitizedObject, accessToken, myDomain);

        String resolvedOrgKey = orgKey != null ? orgKey : extractHost(resolveSalesforceDomain(myDomain));
        String targetSchema = salesforceOrgService.resolveSchemaName(resolvedOrgKey);
        boolean tableDropped = dropRoutingTableFromEngine(sanitizedMode, targetSchema, sanitizedObject);

        Map<String, Object> result = new HashMap<>();
        result.put("selectedObject", sanitizedObject);
        result.put("ingestionMode", sanitizedMode);
        result.put("endpoint", "CDC".equals(sanitizedMode) ? "/pubsub/release" : "/streaming/release");
        result.put("status", "SUCCESS");
        result.put("message", sanitizedMode + " 해지가 완료되었어요." + (tableDropped ? " (로컬 테이블 drop 완료)" : ""));
        result.put("engineMessage", sanitizedMode + " 해지가 완료되었어요." + (tableDropped ? " (로컬 테이블 drop 완료)" : ""));
        result.put("responseBody", "releasedCount=" + releasedCount + ", tableDropped=" + tableDropped);
        result.put("initialLoadCount", 0);
        result.put("subscribeStatus", "RELEASED");
        result.put("pushTopicStatus", "STREAMING".equals(sanitizedMode) ? "RELEASED" : "-");
        result.put("cdcCreationStatus", "CDC".equals(sanitizedMode) ? "RELEASED" : "-");
        result.put("cdcCreationMessage", "CDC".equals(sanitizedMode) ? (releasedCount > 0 ? "CDC 채널 연결을 해지했어요." : "해지할 CDC 채널이 없었어요.") : "-");
        result.put("releasedCount", releasedCount);

                routingDashboardRepository.markRoutingReleased(resolvedOrgKey, sanitizedObject, sanitizedMode, sanitizedMode + " 해지가 완료되었어요.", actor);
        routingDashboardRepository.deactivateSlotByObject(
                resolvedOrgKey,
                sanitizedObject,
                sanitizedMode,
                sanitizedMode + " 해지로 슬롯 비활성화"
        );
        Map<String, Object> history = new HashMap<>();
        history.put("routingRegistryId", null);
        history.put("orgKey", resolvedOrgKey);
        history.put("selectedObject", sanitizedObject);
        history.put("routingProtocol", sanitizedMode);
        history.put("eventType", "RELEASE");
        history.put("eventStatus", "SUCCESS");
        history.put("eventStage", "COMPLETE");
        history.put("endpoint", "CDC".equals(sanitizedMode) ? "/pubsub/release" : "/streaming/release");
        history.put("message", sanitizedMode + " 해지가 완료되었어요.");
        history.put("detailText", "releasedCount=" + releasedCount);
        history.put("initialLoadCount", 0);
        history.put("actor", actor);
        routingDashboardRepository.insertRoutingHistory(history);
        return result;
    }

    private List<Map<String, Object>> getObjectFieldDefinitions(String accessToken, String myDomain, String selectedObject) throws Exception {
        OkHttpClient client = new OkHttpClient.Builder()
                .connectTimeout(10, TimeUnit.SECONDS)
                .readTimeout(10, TimeUnit.SECONDS)
                .writeTimeout(10, TimeUnit.SECONDS)
                .build();

        String resolvedMyDomain = resolveSalesforceDomain(myDomain);

        Request request = new Request.Builder()
                .url(resolvedMyDomain + "/services/data/v63.0/sobjects/" + selectedObject + "/describe")
                .addHeader("Authorization", "Bearer " + accessToken)
                .addHeader("Content-Type", "application/json")
                .build();

        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful() || response.body() == null) {
                throw new AppException("필드 메타데이터 조회 실패: " + response.code() + " " + response.message());
            }
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode fields = objectMapper.readTree(response.body().string()).path("fields");
            List<Map<String, Object>> result = new ArrayList<>();
            if (fields.isArray()) {
                for (JsonNode field : fields) {
                    Map<String, Object> item = new HashMap<>();
                    item.put("name", field.path("name").asText(""));
                    item.put("label", field.path("label").asText(""));
                    item.put("type", field.path("type").asText(""));
                    result.add(item);
                }
            }
            return result;
        } catch (IOException e) {
            throw new AppException("필드 메타데이터 조회 중 오류 발생", e);
        }
    }

    private Map<String, String> getPropertyMap(String selectedObject, String accessToken, String refreshToken, String actor, String myDomain, String orgName, String orgKey, String targetSchema) {
        String resolvedMyDomain = resolveSalesforceDomain(myDomain);
        Map<String, String> mapProperty = new HashMap<>();
        mapProperty.put("selectedObject", selectedObject);
        mapProperty.put("accessToken", accessToken);
        mapProperty.put("refreshToken", refreshToken);
        mapProperty.put("instanceUrl", resolvedMyDomain);
        mapProperty.put("orgKey", orgKey == null || orgKey.isBlank() ? extractHost(resolvedMyDomain) : orgKey);
        mapProperty.put("orgName", orgName == null || orgName.isBlank() ? extractHost(resolvedMyDomain) : orgName);
        mapProperty.put("targetSchema", targetSchema);
        mapProperty.put("targetTable", selectedObject);
        mapProperty.put("instanceName", extractHost(resolvedMyDomain));
        mapProperty.put("orgType", "-");
        mapProperty.put("isSandbox", String.valueOf(extractHost(resolvedMyDomain).contains("sandbox") || extractHost(resolvedMyDomain).contains("test")));
        mapProperty.put("objectLabel", selectedObject);
        mapProperty.put("actor", actor);
        return mapProperty;
    }

    private Map<String, Object> parseEngineResponse(String responseBody) {
        if (responseBody == null || responseBody.isBlank()) {
            return new HashMap<>();
        }

        try {
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.readValue(responseBody, new TypeReference<Map<String, Object>>() {});
        } catch (Exception e) {
            log.warn("Failed to parse routing-engine response body", e);
            Map<String, Object> fallback = new HashMap<>();
            fallback.put("raw", responseBody);
            return fallback;
        }
    }

    private static Map<String, Object> defaultCdcSlotSummary(String message) {
        Map<String, Object> result = new HashMap<>();

        result.put("used", 0);
        result.put("limit", 5);
        result.put("available", 5);
        result.put("message", message);
        return result;
    }

    private int releaseStreaming(OkHttpClient client, ObjectMapper objectMapper, String selectedObject, String accessToken, String myDomain) throws Exception {
        String resolvedMyDomain = resolveSalesforceDomain(myDomain);
        String query = "SELECT Id FROM PushTopic WHERE Name='" + selectedObject + "'";
        List<String> ids = queryIds(client, objectMapper,
                resolvedMyDomain + "/services/data/v63.0/query/?q=" + URLEncoder.encode(query, StandardCharsets.UTF_8),
                accessToken,
                "Id");
        for (String id : ids) {
            deleteSObject(client, resolvedMyDomain + "/services/data/v63.0/sobjects/PushTopic/" + id, accessToken);
        }
        return ids.size();
    }

    private int releaseCdc(OkHttpClient client, ObjectMapper objectMapper, String selectedObject, String accessToken, String myDomain) throws Exception {
        String resolvedMyDomain = resolveSalesforceDomain(myDomain);
        String selectedEntity = toChangeEventEntity(selectedObject);
        String query = "SELECT Id FROM PlatformEventChannelMember WHERE SelectedEntity='" + selectedEntity + "'";
        List<String> ids = queryIds(client, objectMapper,
                resolvedMyDomain + "/services/data/v63.0/tooling/query/?q=" + URLEncoder.encode(query, StandardCharsets.UTF_8),
                accessToken,
                "Id");
        for (String id : ids) {
            deleteSObject(client, resolvedMyDomain + "/services/data/v63.0/tooling/sobjects/PlatformEventChannelMember/" + id, accessToken);
        }

        return ids.size();
    }

    private List<String> queryIds(OkHttpClient client, ObjectMapper objectMapper, String url, String accessToken, String fieldName) throws Exception {
        Request request = new Request.Builder()
                .url(url)
                .addHeader("Authorization", "Bearer " + accessToken)
                .addHeader("Content-Type", "application/json")
                .build();
        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful() || response.body() == null) {
                throw new AppException("상태 조회/해지용 Salesforce 쿼리에 실패했어요: " + response.code() + " " + response.message());
            }
            JsonNode records = objectMapper.readTree(response.body().string()).path("records");
            List<String> ids = new ArrayList<>();
            if (records.isArray()) {
                for (JsonNode record : records) {
                    String id = record.path(fieldName).asText("");
                    if (!id.isBlank()) {
                        ids.add(id);
                    }
                }
            }
            return ids;
        }
    }

    private void deleteSObject(OkHttpClient client, String url, String accessToken) throws Exception {
        Request request = new Request.Builder()
                .url(url)
                .addHeader("Authorization", "Bearer " + accessToken)
                .delete()
                .build();
        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful() && response.code() != 404) {
                throw new AppException("Salesforce 해지 호출 실패: " + response.code() + " " + response.message());
            }
        }
    }

    private String fromChangeEventEntity(String selectedEntity) {
        if (selectedEntity == null || selectedEntity.isBlank()) {
            return "";
        }
        if (selectedEntity.endsWith("__ChangeEvent")) {
            return selectedEntity.substring(0, selectedEntity.length() - "__ChangeEvent".length()) + "__c";
        }
        if (selectedEntity.endsWith("ChangeEvent")) {
            return selectedEntity.substring(0, selectedEntity.length() - "ChangeEvent".length());
        }
        return selectedEntity;
    }

    private String toChangeEventEntity(String selectedObject) {
        if (selectedObject.endsWith("__c")) {
            return selectedObject.substring(0, selectedObject.length() - 3) + "__ChangeEvent";
        }
        if (selectedObject.endsWith("__ChangeEvent") || selectedObject.endsWith("ChangeEvent")) {
            return selectedObject;
        }
        return selectedObject + "ChangeEvent";
    }

    private Map<String, Object> getOrgSummary(String accessToken, String myDomain) {
        String resolvedMyDomain = resolveSalesforceDomain(myDomain);
        Map<String, Object> summary = defaultOrgSummary(myDomain);
        OkHttpClient client = new OkHttpClient.Builder()
                .connectTimeout(10, TimeUnit.SECONDS)
                .readTimeout(10, TimeUnit.SECONDS)
                .writeTimeout(10, TimeUnit.SECONDS)
                .build();
        ObjectMapper objectMapper = new ObjectMapper();

        String query = "SELECT Id, Name, OrganizationType, InstanceName, IsSandbox FROM Organization LIMIT 1";
        Request request = new Request.Builder()
                .url(resolvedMyDomain + "/services/data/v63.0/query/?q=" + URLEncoder.encode(query, StandardCharsets.UTF_8))
                .addHeader("Authorization", "Bearer " + accessToken)
                .addHeader("Content-Type", "application/json")
                .build();

        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful() || response.body() == null) {
                log.warn("Failed to fetch Salesforce org summary. status={}, message={}", response.code(), response.message());
                return summary;
            }

            JsonNode records = objectMapper.readTree(response.body().string()).path("records");
            if (records.isArray() && !records.isEmpty()) {
                JsonNode org = records.get(0);
                summary.put("orgId", org.path("Id").asText(summary.get("orgId").toString()));
                summary.put("orgName", org.path("Name").asText(summary.get("orgName").toString()));
                summary.put("orgType", org.path("OrganizationType").asText(summary.get("orgType").toString()));
                summary.put("instanceName", org.path("InstanceName").asText(summary.get("instanceName").toString()));
                summary.put("sandbox", org.path("IsSandbox").asBoolean(false));
            }
        } catch (Exception e) {
            log.warn("Error while fetching Salesforce org summary", e);
        }
        return summary;
    }

    private Map<String, Object> defaultOrgSummary(String myDomain) {
        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("orgName", myDomain == null ? "-" : extractHost(myDomain));
        summary.put("orgId", "-");
        summary.put("orgType", "-");
        summary.put("instanceName", extractHost(myDomain));
        summary.put("sandbox", extractHost(myDomain).contains("sandbox") || extractHost(myDomain).contains("test"));
        summary.put("myDomain", resolveSalesforceDomain(myDomain));
        return summary;
    }


    private String resolveSalesforceDomain(String myDomain) {
        if (myDomain == null || myDomain.isBlank()) {
            throw new AppException("Salesforce 도메인이 비어 있습니다.");
        }

        String trimmed = myDomain.strip();
        if (trimmed.startsWith("http://") || trimmed.startsWith("https://")) {
            int idx = trimmed.indexOf("://");
            String scheme = trimmed.substring(0, idx + 3);
            return scheme + trimmed.substring(idx + 3).replaceAll("/+$", "");
        }

        try {
            URI uri = URI.create(trimmed);
            String host = uri.getHost();
            if (host != null && !host.isBlank()) {
                return "https://" + host;
            }
        } catch (Exception ignore) {
            // fallback below
        }

        return "https://" + trimmed.replaceAll("/+$", "");
    }


    private String resolveRoutingBaseUrl() {
        if (routingEngineBaseUrl == null || routingEngineBaseUrl.isBlank()) {
            throw new AppException("라우팅 엔진 주소가 비어 있습니다. 환경변수로 routing.engine.base-url 또는 ROUTING_APP_PORT를 확인해 주세요.");
        }
        String trimmed = routingEngineBaseUrl.trim().replaceAll("/+$", "");
        if (trimmed.startsWith("http://") || trimmed.startsWith("https://")) {
            return trimmed;
        }
        return "http://" + trimmed;
    }

    private String extractHost(String url) {
        try {
            String normalized = resolveSalesforceDomain(url);
            URI uri = URI.create(normalized);
            String host = uri.getHost();
            return host != null ? host : normalized;
        } catch (Exception e) {
            return url;
        }
    }

    private static int asInt(Object value) {
        if (value instanceof Number number) {
            return number.intValue();
        }
        if (value == null) {
            return 0;
        }
        try {
            return Integer.parseInt(String.valueOf(value));
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    private boolean dropRoutingTableFromEngine(String ingestionMode, String targetSchema, String selectedObject) {
        String endpoint = "CDC".equalsIgnoreCase(ingestionMode) ? "/pubsub/drop" : "/streaming/drop";
        OkHttpClient client = new OkHttpClient.Builder()
                .connectTimeout(5, TimeUnit.SECONDS)
                .readTimeout(5, TimeUnit.SECONDS)
                .writeTimeout(5, TimeUnit.SECONDS)
                .build();

        RequestBody formBody = new FormBody.Builder()
                .add("selectedObject", selectedObject)
                .add("targetSchema", targetSchema == null ? "" : targetSchema)
                .build();

        String url = resolveRoutingBaseUrl() + endpoint;
        Request request = new Request.Builder()
                .url(url)
                .post(formBody)
                .build();

        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                log.warn("Failed to drop routing table on engine. mode={}, object={}, status={}, message={}", ingestionMode, selectedObject, response.code(), response.message());
                return false;
            }
            return true;
        } catch (Exception e) {
            log.warn("Error while dropping routing table on engine. mode={}, object={}, schema={}", ingestionMode, selectedObject, targetSchema, e);
            return false;
        }
    }

}
