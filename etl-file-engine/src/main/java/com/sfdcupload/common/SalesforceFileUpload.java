package com.sfdcupload.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sfdcupload.file.dto.ExcelFile;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import okhttp3.Headers;
import okhttp3.MediaType;
import okhttp3.MultipartBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okio.ByteString;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

@Service
@RequiredArgsConstructor
@Slf4j
public class SalesforceFileUpload {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final OkHttpClient CLIENT = new OkHttpClient.Builder()
            .callTimeout(600, TimeUnit.SECONDS)
            .connectTimeout(15, TimeUnit.SECONDS)
            .writeTimeout(560, TimeUnit.SECONDS)
            .readTimeout(30, TimeUnit.SECONDS)
            .build();

    private RequestBody generateRequestBody(Object object) throws JsonProcessingException {
        String jsonBody = MAPPER.writeValueAsString(object);
        return RequestBody.create(jsonBody, MediaType.parse("application/json"));
    }

    public boolean uploadFileViaConnectAPI(byte[] fileByte, String fileName, String recordId, String accessToken, String myDomain) throws Exception {
        RequestBody fileBody = RequestBody.create(
                ByteString.of(fileByte),
                MediaType.parse("application/octet-stream")
        );

        Map<String, String> contentVersionMap = new HashMap<>();
        contentVersionMap.put("firstPublishLocationId", recordId);

        RequestBody jsonPart = generateRequestBody(contentVersionMap);

        MultipartBody requestBody = new MultipartBody.Builder()
                .setType(MultipartBody.FORM)
                .addFormDataPart("fileData", fileName, fileBody)
                .addPart(Headers.of("Content-Disposition", "form-data; name=\"json\""), jsonPart)
                .build();

        Request uploadRequest = new Request.Builder()
                .url(normalizeDomain(myDomain) + "/services/data/v65.0/connect/files/users/me")
                .addHeader("Authorization", "Bearer " + accessToken)
                .post(requestBody)
                .build();

        try (Response response = CLIENT.newCall(uploadRequest).execute()) {
            int statusCode = response.code();
            String responseBody = Objects.requireNonNull(response.body()).string();

            if (statusCode != 201 && statusCode != 200) {
                log.error("업로드 에러 :: {}", responseBody);
                throw new Exception("파일 업로드 실패 ! ==> " + responseBody);
            }
            return true;
        }
    }

    public boolean uploadFileViaContentVersionAPI(byte[] fileBytes, String fileName, String recordId, String accessToken, String myDomain) throws Exception {
        String contentVersionUrl = normalizeDomain(myDomain) + "/services/data/v65.0/sobjects/ContentVersion";
        String base64Encoded = Base64.getEncoder().encodeToString(fileBytes);

        Map<String, String> contentVersionMap = new HashMap<>();
        contentVersionMap.put("Title", fileName);
        contentVersionMap.put("PathOnClient", fileName);
        contentVersionMap.put("VersionData", base64Encoded);
        contentVersionMap.put("firstPublishLocationId", recordId);

        RequestBody requestBody = generateRequestBody(contentVersionMap);

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
            return true;
        }
    }

    public List<ExcelFile> uploadFileBatch(List<ExcelFile> listExcel, String accessToken, String myDomain) throws IOException {
        String connectBatchUrl = normalizeDomain(myDomain) + "/services/data/v65.0/connect/batch";
        List<ExcelFile> listSuccess = new ArrayList<>();

        ObjectNode root = MAPPER.createObjectNode();
        root.put("haltOnError", false);

        ArrayNode batchRequests = root.putArray("batchRequests");
        for (ExcelFile excelFile : listExcel) {
            String base64Blob = Base64.getEncoder().encodeToString(excelFile.getAppendFile());

            ObjectNode cvRequest = MAPPER.createObjectNode();
            cvRequest.put("Title", excelFile.getBbsAttachFileName());
            cvRequest.put("PathOnClient", excelFile.getBbsAttachFileName());
            cvRequest.put("VersionData", base64Blob);
            cvRequest.put("firstPublishLocationId", excelFile.getSfid());

            ObjectNode batchRequest = MAPPER.createObjectNode();
            batchRequest.put("url", "/services/data/v65.0/sobjects/ContentVersion");
            batchRequest.put("method", "POST");
            batchRequest.put("richInput", cvRequest);
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

            JsonNode results = MAPPER.readTree(responseBody).get("results");
            for (int i = 0; i < results.size(); i++) {
                JsonNode result = results.get(i).get("result");
                if (result.get("success").asBoolean()) {
                    listExcel.get(i).setIsMig(1);
                    listSuccess.add(listExcel.get(i));
                }
            }
        }

        return listSuccess;
    }

    private String normalizeDomain(String myDomain) {
        if (myDomain == null || myDomain.isBlank()) {
            throw new IllegalArgumentException("Salesforce myDomain is blank");
        }
        return myDomain.startsWith("http://") || myDomain.startsWith("https://") ? myDomain : "https://" + myDomain;
    }
}
