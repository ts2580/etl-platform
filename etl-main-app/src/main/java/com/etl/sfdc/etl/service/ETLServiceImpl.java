package com.etl.sfdc.etl.service;

import com.etl.sfdc.etl.dto.ObjectDefinition;
import com.etlplatform.common.error.AppException;
import com.etlplatform.common.validation.RequestValidationUtils;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import okhttp3.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Service
@RequiredArgsConstructor
@Slf4j
public class ETLServiceImpl implements ETLService {

    @Value("${salesforce.myDomain}")
    private String myDomain;

    @Value("${aws.albUri}")
    private String albUri;

    @Override
    public List<ObjectDefinition> getObjects(String accessToken) throws Exception {
        List<ObjectDefinition> listDef = new ArrayList<>();
        OkHttpClient client = new OkHttpClient();

        Request request = new Request.Builder()
                .url(myDomain + "/services/data/v63.0/sobjects")
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

        Request request = new Request.Builder()
                .url(albUri + "/pubsub/slots/summary")
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
    public Map<String, Object> setObjects(String selectedObject, String ingestionMode, String accessToken, String refreshToken) throws Exception {
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

        String json = objectMapper.writeValueAsString(getPropertyMap(sanitizedObject, accessToken, refreshToken));
        String endpoint = "CDC".equals(sanitizedMode) ? "/pubsub" : "/streaming";
        String modeLabel = "CDC".equals(sanitizedMode) ? "CDC" : "Streaming";
        log.info("Calling routing engine. selectedObject={}, ingestionMode={}, endpoint={}", sanitizedObject, sanitizedMode, endpoint);

        RequestBody formBody = RequestBody.create(json, MediaType.get("application/json; charset=utf-8"));
        Request request = new Request.Builder()
                .url(albUri + endpoint)
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
            result.put("cdcCreationStatus", String.valueOf(engineResult.getOrDefault("cdcCreationStatus", "-")));
            result.put("cdcCreationMessage", String.valueOf(engineResult.getOrDefault("cdcCreationMessage", "-")));
            result.put("engineMessage", String.valueOf(engineResult.getOrDefault("message", modeLabel + " 설정이 완료되었어요.")));
            return result;
        } catch (AppException e) {
            throw e;
        } catch (Exception e) {
            throw new AppException(modeLabel + " API 호출 중 오류 발생", e);
        }
    }

    private Map<String, String> getPropertyMap(String selectedObject, String accessToken, String refreshToken) {
        Map<String, String> mapProperty = new HashMap<>();
        mapProperty.put("selectedObject", selectedObject);
        mapProperty.put("accessToken", accessToken);
        mapProperty.put("refreshToken", refreshToken);
        mapProperty.put("instanceUrl", myDomain);
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
}
