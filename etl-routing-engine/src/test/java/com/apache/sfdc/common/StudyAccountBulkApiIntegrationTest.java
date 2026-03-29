package com.apache.sfdc.common;

import com.etlplatform.common.salesforce.SalesforceClientCredentialsClient;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(classes = com.apache.sfdc.SfdcApplication.class)
@TestPropertySource(properties = {
        "app.db.enabled=true",
        "spring.main.lazy-initialization=true"
})
class StudyAccountBulkApiIntegrationTest {

    private static final String STUDY_ORG_NAME = "Study";
    private static final String API_VERSION = "60.0";

    @Autowired
    private DataSource dataSource;

    @Autowired
    @Qualifier("salesforceBulkQueryClientImpl")
    private SalesforceBulkQueryClient bulkQueryClient;

    @Autowired
    private SalesforceClientCredentialsClient salesforceOAuthClient;

    @Value("${salesforce.tokenUrl:/services/oauth2/token}")
    private String tokenPath;

    @Test
    void studyAccountBulkQueryCsvCanBeParsedInsertedAndCleanedUp() throws Exception {
        String tempTable = "routing.study_account_bulk_it_" + Instant.now().toEpochMilli();
        String createdTable = null;
        String tokenPrefix = null;
        int insertedCount = 0;

        try (Connection connection = dataSource.getConnection()) {
            SalesforceOrgCredential credential = findStudyCredential(connection);
            assertNotNull(credential, "Study org credential should exist in config.salesforce_org_credentials");
            assertTrue(hasText(credential.getClientId()), "Study org client_id should be present");
            assertTrue(hasText(credential.getClientSecret()), "Study org client_secret should be present");

            String tokenUrl = buildTokenUrl(credential.getMyDomain());
            SalesforceClientCredentialsClient.TokenResponse tokenResponse = salesforceOAuthClient.exchangeClientCredentials(
                    tokenUrl,
                    credential.getClientId(),
                    credential.getClientSecret()
            );

            assertTrue(hasText(tokenResponse.accessToken()), "access token should be returned");
            tokenPrefix = maskToken(tokenResponse.accessToken());
            String instanceUrl = hasText(tokenResponse.instanceUrl())
                    ? tokenResponse.instanceUrl()
                    : ensureHttps(credential.getMyDomain());

            String soql = "SELECT Id, Name, LastModifiedDate FROM Account ORDER BY LastModifiedDate DESC LIMIT 5";
            String jobId = bulkQueryClient.createQueryJob(instanceUrl, API_VERSION, tokenResponse.accessToken(), soql);
            assertTrue(hasText(jobId), "Bulk API job id should be returned");

            bulkQueryClient.waitForJobCompletion(instanceUrl, API_VERSION, tokenResponse.accessToken(), jobId, 2000L, 120000L);

            SalesforceBulkQueryClient.BulkQueryPage page = bulkQueryClient.fetchResults(
                    instanceUrl,
                    API_VERSION,
                    tokenResponse.accessToken(),
                    jobId,
                    5,
                    null
            );

            assertNotNull(page.csvBody(), "Bulk API CSV body should not be null");
            assertTrue(page.csvBody().contains("Id") && page.csvBody().contains("LastModifiedDate"),
                    "Bulk API should return CSV headers");

            SalesforceBulkCsvCursor cursor = new SalesforceBulkCsvCursor(
                    bulkQueryClient,
                    instanceUrl,
                    API_VERSION,
                    tokenResponse.accessToken(),
                    jobId,
                    5
            );
            List<Map<String, String>> rows = cursor.nextBatch();
            assertFalse(rows.isEmpty(), "Parsed CSV rows should not be empty");
            assertTrue(rows.stream().allMatch(row -> hasText(row.get("Id"))), "Every parsed row should have Id");

            createdTable = tempTable;
            try (Statement statement = connection.createStatement()) {
                statement.execute("CREATE TABLE " + tempTable + " (sfid VARCHAR(32) PRIMARY KEY, name VARCHAR(255) NULL, last_modified_date VARCHAR(64) NULL)");
            }

            try (PreparedStatement ps = connection.prepareStatement(
                    "INSERT INTO " + tempTable + " (sfid, name, last_modified_date) VALUES (?, ?, ?)")) {
                for (Map<String, String> row : rows) {
                    ps.setString(1, row.get("Id"));
                    ps.setString(2, row.get("Name"));
                    ps.setString(3, row.get("LastModifiedDate"));
                    ps.addBatch();
                }
                for (int count : ps.executeBatch()) {
                    insertedCount += Math.max(count, 0);
                }
            }

            assertEquals(rows.size(), insertedCount, "Inserted row count should match parsed row count");
            assertEquals(insertedCount, countRows(connection, tempTable), "Inserted rows should be queryable from DB");

            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("DELETE FROM " + tempTable);
                assertEquals(0, countRows(connection, tempTable), "Inserted rows should be deleted during cleanup");
                statement.execute("DROP TABLE " + tempTable);
                createdTable = null;
            }
        } catch (Exception e) {
            throw new AssertionError("Study Account Bulk API integration failed. tokenPrefix=" + (tokenPrefix == null ? "n/a" : tokenPrefix)
                    + ", insertedCount=" + insertedCount + ", tempTable=" + tempTable, e);
        } finally {
            cleanupTableIfExists(createdTable);
        }
    }

    private SalesforceOrgCredential findStudyCredential(Connection connection) throws Exception {
        String sql = "SELECT id, org_key, org_name, my_domain, schema_name, client_id, client_secret, access_token, access_token_issued_at, is_active, is_default, created_at, updated_at " +
                "FROM config.salesforce_org_credentials WHERE org_name = ? AND is_active = true LIMIT 1";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, STUDY_ORG_NAME);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                SalesforceOrgCredential credential = new SalesforceOrgCredential();
                credential.setId(rs.getLong("id"));
                credential.setOrgKey(rs.getString("org_key"));
                credential.setOrgName(rs.getString("org_name"));
                credential.setMyDomain(rs.getString("my_domain"));
                credential.setSchemaName(rs.getString("schema_name"));
                credential.setClientId(rs.getString("client_id"));
                credential.setClientSecret(rs.getString("client_secret"));
                credential.setAccessToken(rs.getString("access_token"));
                credential.setAccessTokenIssuedAt(rs.getString("access_token_issued_at"));
                credential.setIsActive(rs.getBoolean("is_active"));
                credential.setIsDefault(rs.getBoolean("is_default"));
                credential.setCreatedAt(rs.getString("created_at"));
                credential.setUpdatedAt(rs.getString("updated_at"));
                return credential;
            }
        }
    }

    private int countRows(Connection connection, String tableName) throws Exception {
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM " + tableName)) {
            assertTrue(rs.next(), "count query should return one row");
            return rs.getInt(1);
        }
    }

    private void cleanupTableIfExists(String tableName) throws Exception {
        if (!hasText(tableName)) {
            return;
        }
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            try {
                statement.executeUpdate("DELETE FROM " + tableName);
            } catch (Exception ignored) {
            }
            statement.execute("DROP TABLE IF EXISTS " + tableName);
        }
    }

    private String buildTokenUrl(String myDomain) {
        String base = ensureHttps(myDomain);
        if (tokenPath.startsWith("http://") || tokenPath.startsWith("https://")) {
            return tokenPath;
        }
        return base + (tokenPath.startsWith("/") ? tokenPath : "/" + tokenPath);
    }

    private String ensureHttps(String domainOrUrl) {
        if (domainOrUrl == null || domainOrUrl.isBlank()) {
            throw new IllegalArgumentException("myDomain is required");
        }
        if (domainOrUrl.startsWith("http://") || domainOrUrl.startsWith("https://")) {
            return domainOrUrl;
        }
        return "https://" + domainOrUrl;
    }

    private boolean hasText(String value) {
        return value != null && !value.isBlank();
    }

    private String maskToken(String token) {
        if (!hasText(token)) {
            return "n/a";
        }
        if (token.length() <= 10) {
            return token.charAt(0) + "***" + token.charAt(token.length() - 1);
        }
        return token.substring(0, 6) + "***" + token.substring(token.length() - 4);
    }
}
