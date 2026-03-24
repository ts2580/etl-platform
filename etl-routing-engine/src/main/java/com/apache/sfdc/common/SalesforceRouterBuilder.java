package com.apache.sfdc.common;

import com.apache.sfdc.storage.service.ExternalStorageRoutingJdbcExecutor;
import com.etlplatform.common.storage.database.sql.BoundBatchSql;
import com.etlplatform.common.storage.database.sql.BoundSql;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.builder.RouteBuilder;

import java.util.List;
import java.util.Map;

@Slf4j
public class SalesforceRouterBuilder extends RouteBuilder {
    private final String targetSchema;
    private final String selectedObject;
    private final Map<String, Object> mapType;
    private final ExternalStorageRoutingJdbcExecutor routingJdbcExecutor;
    private final Long targetStorageId;
    private final SalesforceStreamingPayloadMapper payloadMapper = new SalesforceStreamingPayloadMapper();
    private final SalesforceRecordMutationProcessor mutationProcessor = new SalesforceRecordMutationProcessor();

    public SalesforceRouterBuilder(String targetSchema,
                                   String selectedObject,
                                   Map<String, Object> mapType,
                                   ExternalStorageRoutingJdbcExecutor routingJdbcExecutor,
                                   Long targetStorageId) {
        this.targetSchema = targetSchema;
        this.selectedObject = selectedObject;
        this.mapType = mapType;
        this.routingJdbcExecutor = routingJdbcExecutor;
        this.targetStorageId = targetStorageId;
    }

    @Override
    public void configure() throws Exception {
        SqlSanitizer.validateSchemaName(targetSchema);
        SqlSanitizer.validateTableName(selectedObject);

        SalesforceMutationRepositoryPort repositoryPort = new SalesforceMutationRepositoryPort() {
            @Override
            public int insertObject(String upperQuery, List<String> listUnderQuery, String tailQuery) {
                return routingJdbcExecutor.insert("STREAMING", upperQuery, listUnderQuery, tailQuery, targetStorageId);
            }

            @Override
            public int updateObject(StringBuilder strUpdate) {
                return routingJdbcExecutor.update("STREAMING", strUpdate, targetStorageId);
            }

            @Override
            public int deleteObject(StringBuilder strDelete) {
                return routingJdbcExecutor.delete("STREAMING", strDelete, targetStorageId);
            }

            @Override
            public boolean supportsBoundStatements() {
                return routingJdbcExecutor.usesExternalStorage(targetStorageId);
            }

            @Override
            public com.etlplatform.common.storage.database.sql.DatabaseVendorStrategy vendorStrategy() {
                return routingJdbcExecutor.resolveStrategy(targetStorageId);
            }

            @Override
            public int insertObject(BoundBatchSql batchSql) {
                return routingJdbcExecutor.insert(batchSql, targetStorageId);
            }

            @Override
            public int updateObject(BoundSql boundSql) {
                return routingJdbcExecutor.update(boundSql, targetStorageId);
            }

            @Override
            public int deleteObject(BoundSql boundSql) {
                return routingJdbcExecutor.delete(boundSql, targetStorageId);
            }
        };

        from("sf:subscribe:" + selectedObject)
                .aggregate(constant(true), new ArrayListAggregationStrategy())
                .completionInterval(5000)
                .process((exchange) -> {

                    Map<String, List<Object>> messageBodies = exchange.getIn().getBody(Map.class);
                    if (messageBodies == null || messageBodies.isEmpty()) {
                        return;
                    }

                    int insertedTotal = 0;
                    int updatedTotal = 0;
                    int deletedTotal = 0;

                    for (Map.Entry<String, List<Object>> entry : messageBodies.entrySet()) {
                        String eventType = entry.getKey();
                        List<Object> messageBody = entry.getValue();
                        if (messageBody == null || messageBody.isEmpty()) {
                            continue;
                        }

                        for (Object body : messageBody) {
                            var mutationOptional = payloadMapper.map(eventType, body, mapType);
                            if (mutationOptional.isEmpty()) {
                                continue;
                            }

                            SalesforceRecordMutationProcessor.MutationResult result = mutationProcessor.apply(
                                    targetSchema,
                                    selectedObject,
                                    mapType,
                                    mutationOptional.get(),
                                    repositoryPort,
                                    "STREAMING"
                            );
                            insertedTotal += result.inserted();
                            updatedTotal += result.updated();
                            deletedTotal += result.deleted();
                        }
                    }

                    log.info("[STREAMING] summary inserted={}, updated={}, deleted={}", insertedTotal, updatedTotal, deletedTotal);
                });
    }
}
