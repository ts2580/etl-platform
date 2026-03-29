package com.apache.sfdc.common;

import java.io.IOException;

public interface SalesforceBulkQueryClient {

    String createQueryJob(String instanceUrl, String apiVersion, String accessToken, String soql) throws IOException;

    void waitForJobCompletion(String instanceUrl,
                              String apiVersion,
                              String accessToken,
                              String jobId,
                              long pollIntervalMillis,
                              long timeoutMillis) throws IOException;

    BulkQueryPage fetchResults(String instanceUrl,
                               String apiVersion,
                               String accessToken,
                               String jobId,
                               Integer maxRecords,
                               String locator) throws IOException;

    record BulkQueryPage(String csvBody, String locator, boolean done) {
    }
}
