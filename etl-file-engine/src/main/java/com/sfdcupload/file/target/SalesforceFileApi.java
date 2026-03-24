package com.sfdcupload.file.target;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sfdcupload.file.domain.FileUploadResult;
import com.sfdcupload.file.domain.FileUploadStrategy;
import com.sfdcupload.file.domain.MigrationFile;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

@Service
@RequiredArgsConstructor
@Slf4j
public class SalesforceFileApi {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final MediaType JSON = MediaType.get("application/json");
    private static final OkHttpClient CLIENT = new OkHttpClient.Builder()
            .callTimeout(600, TimeUnit.SECONDS)
            .connectTimeout(15, TimeUnit.SECONDS)
            .writeTimeout(560, TimeUnit.SECONDS)
            .readTimeout(30, TimeUnit.SECONDS)
            .build();

    private RequestBody generateRequestBody(Object object) throws JsonProcessingException {
        String jsonBody = MAPPER.writeValueAsString(object);
        return RequestBody.create(jsonBody, JSON);
    }

    public FileUploadResult uploadFile(MigrationFile file, String accessToken, String myDomain) throws Exception {
        String contentVersionUrl = normalizeDomain(myDomain) + "/services/data/v65.0/sobjects/ContentVersion";
        String base64Encoded = Base64.getEncoder().encodeToString(file.getContent());

        ObjectNode payload = MAPPER.createObjectNode();
        payload.put("Title", file.getFileName());
        payload.put("PathOnClient", file.getFileName());
        payload.put("VersionData", base64Encoded);

        RequestBody requestBody = generateRequestBody(payload);

        Request request = new Request.Builder()
                .url(contentVersionUrl)
                .addHeader("Authorization", "Bearer " + accessToken)
                .addHeader("Content-Type", "application/json")
                .post(requestBody)
                .build();

        try (Response response = CLIENT.newCall(request).execute()) {
            int statusCode = response.code();
            String responseBody = Objects.requireNonNull(response.body()).string();

            if (statusCode != 201 && statusCode != 200) {
                log.error("ContentVersion 업로드 실패: {}", responseBody);
                throw new Exception("ContentVersion 업로드 실패 ! ==> " + responseBody);
            }

            JsonNode result = MAPPER.readTree(responseBody);
            String contentVersionId = result.path("id").asText(null);
            String contentDocumentId = fetchContentDocumentId(contentVersionId, accessToken, myDomain);

            return FileUploadResult.builder()
                    .success(true)
                    .strategy(FileUploadStrategy.CONTENT_VERSION_SINGLE)
                    .contentVersionId(contentVersionId)
                    .contentDocumentId(contentDocumentId)
                    .message("UPLOAD_SUCCESS")
                    .build();
        }
    }

    public List<FileUploadResult> uploadBatch(List<MigrationFile> files, String accessToken, String myDomain) throws IOException {
        String connectBatchUrl = normalizeDomain(myDomain) + "/services/data/v65.0/connect/batch";
        List<FileUploadResult> results = new ArrayList<>();

        ObjectNode root = MAPPER.createObjectNode();
        root.put("haltOnError", false);

        ArrayNode batchRequests = root.putArray("batchRequests");
        for (MigrationFile file : files) {
            String base64Blob = Base64.getEncoder().encodeToString(file.getContent());

            ObjectNode requestPayload = MAPPER.createObjectNode();
            requestPayload.put("Title", file.getFileName());
            requestPayload.put("PathOnClient", file.getFileName());
            requestPayload.put("VersionData", base64Blob);

            ObjectNode batchRequest = MAPPER.createObjectNode();
            batchRequest.put("url", "/services/data/v65.0/sobjects/ContentVersion");
            batchRequest.put("method", "POST");
            batchRequest.put("richInput", requestPayload);
            batchRequests.add(batchRequest);
        }

        RequestBody requestBody = generateRequestBody(root);

        Request batchRequest = new Request.Builder()
                .url(connectBatchUrl)
                .addHeader("Authorization", "Bearer " + accessToken)
                .addHeader("Content-Type", "application/json")
                .post(requestBody)
                .build();

        try (Response batchResponse = CLIENT.newCall(batchRequest).execute()) {
            String responseBody = Objects.requireNonNull(batchResponse.body()).string();
            int batchStatus = batchResponse.code();
            if (batchStatus != 201 && batchStatus != 200) {
                log.error("batch 업로드 실패: {}", responseBody);
                throw new RuntimeException("batch 업로드 실패: " + responseBody);
            }

            JsonNode batchResults = MAPPER.readTree(responseBody).path("results");
            for (int i = 0; i < batchResults.size(); i++) {
                JsonNode batchResult = batchResults.get(i).path("result");
                boolean success = batchResult.path("success").asBoolean(false);
                String contentVersionId = batchResult.path("id").asText(null);
                String contentDocumentId = success
                        ? fetchContentDocumentId(contentVersionId, accessToken, myDomain)
                        : null;

                results.add(FileUploadResult.builder()
                        .success(success)
                        .strategy(FileUploadStrategy.CONTENT_VERSION_BATCH)
                        .contentVersionId(contentVersionId)
                        .contentDocumentId(contentDocumentId)
                        .message(success ? "UPLOAD_SUCCESS" : batchResult.toString())
                        .build());
            }
        }

        return results;
    }

    private String fetchContentDocumentId(String contentVersionId, String accessToken, String myDomain) throws IOException {
        if (contentVersionId == null || contentVersionId.isBlank()) {
            return null;
        }

        HttpUrl queryUrl = Objects.requireNonNull(HttpUrl.parse(normalizeDomain(myDomain) + "/services/data/v65.0/query"))
                .newBuilder()
                .addQueryParameter("q", "SELECT ContentDocumentId FROM ContentVersion WHERE Id = '" + contentVersionId + "'")
                .build();

        Request request = new Request.Builder()
                .url(queryUrl)
                .addHeader("Authorization", "Bearer " + accessToken)
                .get()
                .build();

        try (Response response = CLIENT.newCall(request).execute()) {
            String responseBody = Objects.requireNonNull(response.body()).string();
            if (!response.isSuccessful()) {
                log.warn("ContentDocumentId 조회 실패. contentVersionId={}, body={}", contentVersionId, responseBody);
                return null;
            }

            JsonNode records = MAPPER.readTree(responseBody).path("records");
            if (!records.isArray() || records.isEmpty()) {
                return null;
            }
            return records.get(0).path("ContentDocumentId").asText(null);
        }
    }

    private String normalizeDomain(String myDomain) {
        if (myDomain == null || myDomain.isBlank()) {
            throw new IllegalArgumentException("Salesforce myDomain is blank");
        }
        return myDomain.startsWith("http://") || myDomain.startsWith("https://") ? myDomain : "https://" + myDomain;
    }
}
