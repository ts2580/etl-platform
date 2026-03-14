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
    public void setObjects(String selectedObject, String accessToken, String refreshToken) throws Exception {
        String sanitizedObject = RequestValidationUtils.requireIdentifier(selectedObject, "selectedObject");
        ObjectMapper objectMapper = new ObjectMapper();

        OkHttpClient client = new OkHttpClient.Builder()
                .connectTimeout(30, TimeUnit.SECONDS)
                .readTimeout(30, TimeUnit.SECONDS)
                .writeTimeout(30, TimeUnit.SECONDS)
                .build();

        String json = objectMapper.writeValueAsString(getPropertyMap(sanitizedObject, accessToken, refreshToken));
        log.info("Calling routing engine for selectedObject={}", sanitizedObject);

        RequestBody formBody = RequestBody.create(json, MediaType.get("application/json; charset=utf-8"));
        Request request = new Request.Builder()
                .url(albUri + ":3931/streaming")
                .post(formBody)
                .addHeader("Content-Type", "application/json")
                .build();

        try (Response response = client.newCall(request).execute()) {
            String responseBody = response.body() != null ? response.body().string() : "";
            if (!response.isSuccessful()) {
                log.warn("Routing engine returned failure. selectedObject={}, status={}, body={}", sanitizedObject, response.code(), responseBody);
                throw new AppException("Streaming API 호출 실패: " + response.code() + " " + response.message());
            }
            log.info("Routing engine call completed. selectedObject={}, response={}", sanitizedObject, responseBody);
        } catch (Exception e) {
            throw new AppException("Streaming API 호출 중 오류 발생", e);
        }
    }

    private static Map<String, String> getPropertyMap(String selectedObject, String accessToken, String refreshToken) {
        Map<String, String> mapProperty = new HashMap<>();
        mapProperty.put("selectedObject", selectedObject);
        mapProperty.put("accessToken", accessToken);
        mapProperty.put("refreshToken", refreshToken);
        return mapProperty;
    }
}
