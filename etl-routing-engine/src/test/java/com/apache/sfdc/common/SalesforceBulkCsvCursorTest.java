package com.apache.sfdc.common;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SalesforceBulkCsvCursorTest {

    @Test
    void parsesCsvPagesAndStopsOnLastLocator() throws Exception {
        StubBulkQueryClient client = new StubBulkQueryClient();
        SalesforceBulkCsvCursor cursor = new SalesforceBulkCsvCursor(client, "https://example.my.salesforce.com", "60.0", "token", "750xx", 2);

        List<Map<String, String>> first = cursor.nextBatch();
        List<Map<String, String>> second = cursor.nextBatch();
        List<Map<String, String>> third = cursor.nextBatch();

        assertEquals(2, first.size());
        assertEquals("001000000000001AAA", first.get(0).get("Id"));
        assertEquals("Acme", first.get(0).get("Name"));
        assertEquals("2026-03-28T00:00:00.000+0000", first.get(0).get("LastModifiedDate"));
        assertEquals(1, second.size());
        assertEquals("001000000000003AAA", second.get(0).get("Id"));
        assertTrue(third.isEmpty());
    }

    private static final class StubBulkQueryClient implements SalesforceBulkQueryClient {
        private int page;

        @Override
        public String createQueryJob(String instanceUrl, String apiVersion, String accessToken, String soql) {
            return "job";
        }

        @Override
        public void waitForJobCompletion(String instanceUrl, String apiVersion, String accessToken, String jobId, long pollIntervalMillis, long timeoutMillis) {
        }

        @Override
        public BulkQueryPage fetchResults(String instanceUrl, String apiVersion, String accessToken, String jobId, Integer maxRecords, String locator) throws IOException {
            page++;
            return switch (page) {
                case 1 -> new BulkQueryPage("Id,Name,LastModifiedDate\n001000000000001AAA,Acme,2026-03-28T00:00:00.000+0000\n001000000000002AAA,Globex,2026-03-28T00:01:00.000+0000\n", "next", false);
                case 2 -> new BulkQueryPage("Id,Name,LastModifiedDate\n001000000000003AAA,Initech,2026-03-28T00:02:00.000+0000\n", null, true);
                default -> new BulkQueryPage("", null, true);
            };
        }
    }
}
