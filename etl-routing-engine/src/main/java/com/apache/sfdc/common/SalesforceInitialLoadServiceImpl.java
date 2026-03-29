package com.apache.sfdc.common;

import com.apache.sfdc.storage.service.ExternalStorageRoutingJdbcExecutor;
import com.etlplatform.common.error.AppException;
import com.etlplatform.common.storage.database.sql.BoundBatchSql;
import com.etlplatform.common.storage.database.sql.DatabaseVendorStrategy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
@ConditionalOnProperty(name = "app.db.enabled", havingValue = "true")
public class SalesforceInitialLoadServiceImpl implements SalesforceInitialLoadService {

    private final SalesforceBulkQueryClient bulkQueryClient;
    private final SalesforceBulkResultMapper bulkResultMapper;
    private final ExternalStorageRoutingJdbcExecutor routingJdbcExecutor;

    @Value("${app.salesforce.bulk-initial-load.chunk-size:2000}")
    private int chunkSize;

    @Value("${app.salesforce.bulk-initial-load.poll-interval-millis:2000}")
    private long pollIntervalMillis;

    @Value("${app.salesforce.bulk-initial-load.timeout-millis:300000}")
    private long timeoutMillis;

    @Override
    public int load(String routingProtocol,
                    Map<String, String> mapProperty,
                    String accessToken,
                    String instanceUrl,
                    String apiVersion,
                    String selectedObject,
                    SalesforceObjectSchemaBuilder.SchemaResult schemaResult,
                    Long targetStorageId) {
        DatabaseVendorStrategy strategy = routingJdbcExecutor.resolveStrategy(targetStorageId);
        SalesforceTargetTableResolver.ResolvedTargetTable resolvedTargetTable = resolveTargetTable(mapProperty, selectedObject, strategy);
        String physicalTableName = resolvedTargetTable.physicalTableName();
        String soql = SalesforceObjectSchemaBuilder.buildInitialQuery(selectedObject, schemaResult.fields());
        Instant start = Instant.now();
        Map<String, Object> errorContext = SalesforceHttpErrorHelper.with(
                SalesforceHttpErrorHelper.context(
                        routingProtocol,
                        selectedObject,
                        mapProperty.get("orgKey"),
                        instanceUrl,
                        targetStorageId
                ),
                "targetTable",
                physicalTableName
        );

        try {
            String jobId = bulkQueryClient.createQueryJob(instanceUrl, apiVersion, accessToken, soql);
            bulkQueryClient.waitForJobCompletion(instanceUrl, apiVersion, accessToken, jobId, pollIntervalMillis, timeoutMillis);

            SalesforceBulkCsvCursor cursor = new SalesforceBulkCsvCursor(
                    bulkQueryClient,
                    instanceUrl,
                    apiVersion,
                    accessToken,
                    jobId,
                    chunkSize
            );

            int insertedData = 0;
            while (true) {
                List<Map<String, String>> batch = cursor.nextBatch();
                if (batch.isEmpty()) {
                    break;
                }
                if (routingJdbcExecutor.usesExternalStorage(targetStorageId)) {
                    BoundBatchSql batchSql = new BoundBatchSql(
                            strategy.buildUpsertSql(
                                    SalesforceObjectSchemaBuilder.qualifiedName(mapProperty.get("targetSchema"), physicalTableName, null, strategy),
                                    SalesforceObjectSchemaBuilder.buildInsertColumns(schemaResult.fields()),
                                    schemaResult.fields(),
                                    "sfid",
                                    SalesforceObjectSchemaBuilder.INTERNAL_LAST_MODIFIED_COLUMN,
                                    SalesforceObjectSchemaBuilder.INTERNAL_LAST_EVENT_AT_COLUMN
                            ),
                            bulkResultMapper.toParameterGroups(batch, schemaResult, strategy)
                    );
                    insertedData += routingJdbcExecutor.insert(batchSql, targetStorageId);
                } else {
                    String upperQuery = SalesforceObjectSchemaBuilder.buildInsertSql(mapProperty.get("targetSchema"), physicalTableName, schemaResult.soql());
                    String tailQuery = SalesforceObjectSchemaBuilder.buildInsertTail(schemaResult.fields(), strategy);
                    insertedData += routingJdbcExecutor.insert(
                            routingProtocol,
                            upperQuery,
                            bulkResultMapper.toInsertRows(batch, schemaResult, strategy),
                            tailQuery,
                            targetStorageId
                    );
                }
            }

            Duration interval = Duration.between(start, Instant.now());
            log.info("테이블 : {}. Bulk 초기 적재 데이터 수 : {}. 소요시간 : {}시간 {}분 {}초",
                    selectedObject, insertedData, interval.toHours(), interval.toMinutesPart(), interval.toSecondsPart());
            return insertedData;
        } catch (Exception e) {
            throw SalesforceHttpErrorHelper.failure("Failed to load Salesforce object records via Bulk API", errorContext, e);
        }
    }

    private SalesforceTargetTableResolver.ResolvedTargetTable resolveTargetTable(Map<String, String> mapProperty,
                                                                                  String selectedObject,
                                                                                  DatabaseVendorStrategy strategy) {
        if (mapProperty == null) {
            return SalesforceTargetTableResolver.resolveTargetTable(null, selectedObject, null, null, strategy);
        }

        SalesforceTargetTableResolver.ResolvedTargetTable resolvedTargetTable = SalesforceTargetTableResolver.resolveTargetTable(
                mapProperty.get("targetSchema"),
                selectedObject,
                mapProperty.get("targetTable"),
                mapProperty.get("orgName"),
                strategy
        );
        mapProperty.put("targetTable", resolvedTargetTable.physicalTableName());
        return resolvedTargetTable;
    }
}
