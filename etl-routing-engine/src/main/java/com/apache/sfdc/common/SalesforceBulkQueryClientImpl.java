package com.apache.sfdc.common;

import com.etlplatform.common.error.AppException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Map;

@Slf4j
@Component("salesforceBulkQueryClientImpl")
public class SalesforceBulkQueryClientImpl implements SalesforceBulkQueryClient {

    private static final MediaType JSON = MediaType.get("application/json; charset=utf-8");

    private final OkHttpClient okHttpClient;
    private final ObjectMapper objectMapper;

    public SalesforceBulkQueryClientImpl() {
        this(new OkHttpClient(), new ObjectMapper());
    }

    SalesforceBulkQueryClientImpl(OkHttpClient okHttpClient, ObjectMapper objectMapper) {
        this.okHttpClient = okHttpClient;
        this.objectMapper = objectMapper;
    }

    @Override
    public String createQueryJob(String instanceUrl, String apiVersion, String accessToken, String soql) throws IOException {
        String baseUrl = normalizeBaseUrl(instanceUrl);
        String requestBody = objectMapper.writeValueAsString(Map.of(
                "operation", "query",
                "query", soql,
                "contentType", "CSV"
        ));
        Map<String, Object> errorContext = SalesforceHttpErrorHelper.with(
                SalesforceHttpErrorHelper.with(
                        baseContext(baseUrl, apiVersion, null),
                        "requestPath",
                        "/jobs/query"
                ),
                "queryLength",
                soql != null ? soql.length() : null
        );

        Request request = new Request.Builder()
                .url(baseUrl + "/services/data/v" + apiVersion + "/jobs/query")
                .addHeader("Authorization", "Bearer " + accessToken)
                .addHeader("Content-Type", "application/json")
                .post(RequestBody.create(requestBody, JSON))
                .build();

        try (Response response = okHttpClient.newCall(request).execute()) {
            String body = readBody(response);
            ensureSuccess(response, body, "Failed to create Salesforce Bulk query job", errorContext, "create Bulk query job");
            JsonNode node = objectMapper.readTree(body);
            String jobId = node.path("id").asText(null);
            if (jobId == null || jobId.isBlank()) {
                throw new AppException("Salesforce Bulk query job id is missing: context={" + SalesforceHttpErrorHelper.formatContext(errorContext) + "}");
            }
            return jobId;
        } catch (AppException e) {
            throw e;
        } catch (IOException | RuntimeException e) {
            throw wrapFailure("Failed to create Salesforce Bulk query job", errorContext, e);
        }
    }

    @Override
    public void waitForJobCompletion(String instanceUrl,
                                     String apiVersion,
                                     String accessToken,
                                     String jobId,
                                     long pollIntervalMillis,
                                     long timeoutMillis) throws IOException {
        String baseUrl = normalizeBaseUrl(instanceUrl);
        long deadline = System.currentTimeMillis() + timeoutMillis;
        Map<String, Object> errorContext = SalesforceHttpErrorHelper.with(
                SalesforceHttpErrorHelper.with(
                        baseContext(baseUrl, apiVersion, jobId),
                        "requestPath",
                        "/jobs/query/{jobId}"
                ),
                "timeoutMillis",
                timeoutMillis
        );

        while (true) {
            Request request = new Request.Builder()
                    .url(baseUrl + "/services/data/v" + apiVersion + "/jobs/query/" + jobId)
                    .addHeader("Authorization", "Bearer " + accessToken)
                    .addHeader("Content-Type", "application/json")
                    .build();

            try (Response response = okHttpClient.newCall(request).execute()) {
                String body = readBody(response);
                ensureSuccess(response, body, "Failed to poll Salesforce Bulk query job", errorContext, "poll Bulk query job");
                JsonNode node = objectMapper.readTree(body);
                String state = node.path("state").asText("");
                if ("JobComplete".equalsIgnoreCase(state)) {
                    return;
                }
                if ("Aborted".equalsIgnoreCase(state) || "Failed".equalsIgnoreCase(state)) {
                    Map<String, Object> failureContext = SalesforceHttpErrorHelper.with(errorContext, "jobState", state);
                    String message = node.path("errorMessage").asText(body);
                    log.error("Salesforce Bulk query job failed. context={}, errorMessage={}",
                            SalesforceHttpErrorHelper.formatContext(failureContext),
                            SalesforceHttpErrorHelper.truncateBody(message));
                    throw new AppException("Salesforce Bulk query job failed: context={"
                            + SalesforceHttpErrorHelper.formatContext(failureContext)
                            + "}, errorMessage=" + SalesforceHttpErrorHelper.truncateBody(message));
                }
            } catch (AppException e) {
                throw e;
            } catch (IOException | RuntimeException e) {
                throw wrapFailure("Failed to poll Salesforce Bulk query job", errorContext, e);
            }

            if (System.currentTimeMillis() >= deadline) {
                throw new AppException("Salesforce Bulk query job timed out: context={"
                        + SalesforceHttpErrorHelper.formatContext(errorContext) + "}");
            }
            try {
                Thread.sleep(Math.max(pollIntervalMillis, 200L));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw SalesforceHttpErrorHelper.failure("Interrupted while waiting for Salesforce Bulk query job", errorContext, e);
            }
        }
    }

    @Override
    public BulkQueryPage fetchResults(String instanceUrl,
                                      String apiVersion,
                                      String accessToken,
                                      String jobId,
                                      Integer maxRecords,
                                      String locator) throws IOException {
        HttpUrl.Builder builder = HttpUrl.parse(normalizeBaseUrl(instanceUrl) + "/services/data/v" + apiVersion + "/jobs/query/" + jobId + "/results")
                .newBuilder();
        if (maxRecords != null && maxRecords > 0) {
            builder.addQueryParameter("maxRecords", String.valueOf(maxRecords));
        }
        if (locator != null && !locator.isBlank()) {
            builder.addQueryParameter("locator", locator);
        }

        Map<String, Object> errorContext = SalesforceHttpErrorHelper.with(
                SalesforceHttpErrorHelper.with(
                        SalesforceHttpErrorHelper.with(
                                baseContext(instanceUrl, apiVersion, jobId),
                                "requestPath",
                                "/jobs/query/{jobId}/results"
                        ),
                        "maxRecords",
                        maxRecords
                ),
                "locator",
                locator
        );

        Request request = new Request.Builder()
                .url(builder.build())
                .addHeader("Authorization", "Bearer " + accessToken)
                .addHeader("Accept", "text/csv")
                .build();

        try (Response response = okHttpClient.newCall(request).execute()) {
            String body = readBody(response);
            ensureSuccess(response, body, "Failed to fetch Salesforce Bulk query results", errorContext, "fetch Bulk query results");
            String nextLocator = response.header("Sforce-Locator");
            boolean done = nextLocator == null || nextLocator.isBlank() || "null".equalsIgnoreCase(nextLocator);
            return new BulkQueryPage(body, done ? null : nextLocator, done);
        } catch (AppException e) {
            throw e;
        } catch (IOException | RuntimeException e) {
            throw wrapFailure("Failed to fetch Salesforce Bulk query results", errorContext, e);
        }
    }

    private void ensureSuccess(Response response,
                               String body,
                               String message,
                               Map<String, Object> errorContext,
                               String action) {
        if (!response.isSuccessful()) {
            SalesforceHttpErrorHelper.logHttpFailure(log, action, errorContext, response.code(), body);
            throw SalesforceHttpErrorHelper.httpFailure(message, errorContext, response.code(), body);
        }
    }

    private IOException wrapFailure(String message, Map<String, Object> errorContext, Exception cause) throws IOException {
        if (cause instanceof IOException ioException) {
            throw ioException;
        }
        throw SalesforceHttpErrorHelper.failure(message, errorContext, cause);
    }

    private Map<String, Object> baseContext(String instanceUrl, String apiVersion, String jobId) {
        Map<String, Object> context = SalesforceHttpErrorHelper.context("BULK_API", null, null, instanceUrl, null);
        context = SalesforceHttpErrorHelper.with(context, "apiVersion", apiVersion);
        return SalesforceHttpErrorHelper.with(context, "jobId", jobId);
    }

    private String readBody(Response response) throws IOException {
        return response.body() != null ? response.body().string() : "";
    }

    private String normalizeBaseUrl(String instanceUrl) {
        if (instanceUrl == null || instanceUrl.isBlank()) {
            throw new AppException("Salesforce instanceUrl is required");
        }
        return instanceUrl.contains("/services/data") ? instanceUrl.substring(0, instanceUrl.indexOf("/services/data")) : instanceUrl;
    }
}
