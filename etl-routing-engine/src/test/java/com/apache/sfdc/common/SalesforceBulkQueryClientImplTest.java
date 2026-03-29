package com.apache.sfdc.common;

import com.etlplatform.common.error.AppException;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.Interceptor;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SalesforceBulkQueryClientImplTest {

    @Test
    void createQueryJobIncludesSanitizedContextOnHttpFailure() {
        SalesforceBulkQueryClientImpl client = new SalesforceBulkQueryClientImpl(
                clientResponding(request -> response(request, 500, "{\"error\":\"boom\"}")),
                new ObjectMapper()
        );

        AppException exception = assertThrows(AppException.class, () ->
                client.createQueryJob(
                        "https://example.my.salesforce.com/services/data?v=1&access_token=secret",
                        "61.0",
                        "token",
                        "SELECT Id FROM Account"
                )
        );

        assertTrue(exception.getMessage().contains("Failed to create Salesforce Bulk query job"));
        assertTrue(exception.getMessage().contains("protocol=BULK_API"));
        assertTrue(exception.getMessage().contains("instanceUrl=https://example.my.salesforce.com"));
        assertTrue(exception.getMessage().contains("requestPath=/jobs/query"));
        assertTrue(exception.getMessage().contains("queryLength=22"));
        assertTrue(exception.getMessage().contains("httpStatus=500"));
        assertTrue(exception.getMessage().contains("bodySnippet={\"error\":\"boom\"}"));
        assertTrue(!exception.getMessage().contains("access_token=secret"), exception.getMessage());
    }

    @Test
    void waitForJobCompletionIncludesStateInFailureMessage() {
        SalesforceBulkQueryClientImpl client = new SalesforceBulkQueryClientImpl(
                clientResponding(request -> response(request, 200, "{\"state\":\"Failed\",\"errorMessage\":\"invalid query\"}")),
                new ObjectMapper()
        );

        AppException exception = assertThrows(AppException.class, () ->
                client.waitForJobCompletion(
                        "https://example.my.salesforce.com",
                        "61.0",
                        "token",
                        "750xx0000000001AAA",
                        1L,
                        10L
                )
        );

        assertTrue(exception.getMessage().contains("context={protocol=BULK_API"));
        assertTrue(exception.getMessage().contains("jobId=750xx0000000001AAA"));
        assertTrue(exception.getMessage().contains("jobState=Failed"));
        assertTrue(exception.getMessage().contains("errorMessage=invalid query"));
    }

    @Test
    void fetchResultsReturnsNullLocatorWhenSalesforceSignalsCompletion() throws Exception {
        SalesforceBulkQueryClientImpl client = new SalesforceBulkQueryClientImpl(
                clientResponding(request -> response(request, 200, "Id,Name\n001,Acme\n", "Sforce-Locator", "null")),
                new ObjectMapper()
        );

        SalesforceBulkQueryClient.BulkQueryPage page = client.fetchResults(
                "https://example.my.salesforce.com",
                "61.0",
                "token",
                "750xx0000000001AAA",
                500,
                "abc"
        );

        assertEquals("Id,Name\n001,Acme\n", page.csvBody());
        assertNull(page.locator());
        assertTrue(page.done());
    }

    private OkHttpClient clientResponding(ThrowingInterceptor interceptor) {
        return new OkHttpClient.Builder()
                .addInterceptor(chain -> interceptor.intercept(chain.request()))
                .build();
    }

    private Response response(Request request, int code, String body, String... headers) {
        Response.Builder builder = new Response.Builder()
                .request(request)
                .protocol(Protocol.HTTP_1_1)
                .code(code)
                .message("test")
                .body(ResponseBody.create(body, MediaType.get("application/json; charset=utf-8")));
        for (int i = 0; i + 1 < headers.length; i += 2) {
            builder.addHeader(headers[i], headers[i + 1]);
        }
        return builder.build();
    }

    @FunctionalInterface
    private interface ThrowingInterceptor {
        Response intercept(Request request) throws IOException;
    }
}
