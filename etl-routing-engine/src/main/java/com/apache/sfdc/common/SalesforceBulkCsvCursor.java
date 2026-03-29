package com.apache.sfdc.common;

import lombok.RequiredArgsConstructor;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@RequiredArgsConstructor
public class SalesforceBulkCsvCursor {

    private final SalesforceBulkQueryClient bulkQueryClient;
    private final String instanceUrl;
    private final String apiVersion;
    private final String accessToken;
    private final String jobId;
    private final int chunkSize;
    private String locator;
    private boolean done;

    public List<Map<String, String>> nextBatch() throws IOException {
        if (done) {
            return List.of();
        }
        SalesforceBulkQueryClient.BulkQueryPage page = bulkQueryClient.fetchResults(
                instanceUrl,
                apiVersion,
                accessToken,
                jobId,
                chunkSize,
                locator
        );
        locator = page.locator();
        done = page.done();
        return parse(page.csvBody());
    }

    private List<Map<String, String>> parse(String csvBody) throws IOException {
        if (csvBody == null || csvBody.isBlank()) {
            return List.of();
        }
        try (CSVParser parser = CSVFormat.DEFAULT.builder()
                .setHeader()
                .setSkipHeaderRecord(true)
                .setIgnoreSurroundingSpaces(false)
                .get()
                .parse(new StringReader(csvBody))) {
            List<Map<String, String>> rows = new ArrayList<>();
            List<String> headers = parser.getHeaderNames();
            for (CSVRecord record : parser) {
                Map<String, String> row = new LinkedHashMap<>();
                for (String header : headers) {
                    String value = record.isMapped(header) ? record.get(header) : null;
                    row.put(header, normalize(value));
                }
                rows.add(row);
            }
            return rows;
        }
    }

    private String normalize(String value) {
        if (value == null) {
            return null;
        }
        return value.isEmpty() ? null : value;
    }
}
