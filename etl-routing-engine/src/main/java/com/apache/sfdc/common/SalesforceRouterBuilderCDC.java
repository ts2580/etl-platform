package com.apache.sfdc.common;

import com.apache.sfdc.storage.service.ExternalStorageRoutingJdbcExecutor;
import com.etlplatform.common.storage.database.sql.BoundBatchSql;
import com.etlplatform.common.storage.database.sql.BoundSql;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.builder.RouteBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

@Slf4j
public class SalesforceRouterBuilderCDC extends RouteBuilder {
    private final String targetSchema;
    private final String selectedObject;
    private final Map<String, Object> mapType;
    private final ExternalStorageRoutingJdbcExecutor routingJdbcExecutor;
    private final Long targetStorageId;
    private final Consumer<Integer> activityCallback;
    private final SalesforceCdcPayloadMapper payloadMapper = new SalesforceCdcPayloadMapper();
    private final SalesforceRecordMutationProcessor mutationProcessor = new SalesforceRecordMutationProcessor();

    public SalesforceRouterBuilderCDC(String targetSchema,
                                      String selectedObject,
                                      Map<String, Object> mapType,
                                      ExternalStorageRoutingJdbcExecutor routingJdbcExecutor,
                                      Long targetStorageId,
                                      Consumer<Integer> activityCallback) {
        this.targetSchema = targetSchema;
        this.selectedObject = selectedObject;
        this.mapType = mapType;
        this.routingJdbcExecutor = routingJdbcExecutor;
        this.targetStorageId = targetStorageId;
        this.activityCallback = activityCallback != null ? activityCallback : ignored -> { };
    }

    @Override
    public void configure() throws Exception {
        SqlSanitizer.validateSchemaName(targetSchema);
        SqlSanitizer.validateTableName(selectedObject);

        String eventName = selectedObject.contains("__c")
                ? selectedObject.replace("__c", "__ChangeEvent")
                : selectedObject + "ChangeEvent";

        SalesforceMutationRepositoryPort repositoryPort = new SalesforceMutationRepositoryPort() {
            @Override
            public int insertObject(String upperQuery, List<String> listUnderQuery, String tailQuery) {
                return routingJdbcExecutor.insert("CDC", upperQuery, listUnderQuery, tailQuery, targetStorageId);
            }

            @Override
            public int updateObject(StringBuilder strUpdate) {
                return routingJdbcExecutor.update("CDC", strUpdate, targetStorageId);
            }

            @Override
            public int deleteObject(StringBuilder strDelete) {
                return routingJdbcExecutor.delete("CDC", strDelete, targetStorageId);
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

        from("sf:pubSubSubscribe:/data/" + eventName)
                .aggregate(constant(true), new ArrayListAggregationStrategy())
                .completionInterval(5000)
                .process((exchange) -> {

                    Map<String, List<Object>> messageBodies = exchange.getIn().getBody(Map.class);
                    if (messageBodies == null || messageBodies.isEmpty()) {
                        return;
                    }

                    List<Object> payloads = new ArrayList<>();
                    for (List<Object> bodies : messageBodies.values()) {
                        if (bodies != null && !bodies.isEmpty()) {
                            payloads.addAll(bodies);
                        }
                    }

                    int updatedCount = 0;
                    int insertedCount = 0;
                    int deletedCount = 0;

                    for (Object body : payloads) {
                        var mutationOptional = payloadMapper.map(body, mapType);
                        if (mutationOptional.isEmpty()) {
                            continue;
                        }

                        SalesforceRecordMutationProcessor.MutationResult result = mutationProcessor.apply(
                                targetSchema,
                                selectedObject,
                                mapType,
                                mutationOptional.get(),
                                repositoryPort,
                                "CDC"
                        );
                        updatedCount += result.updated();
                        insertedCount += result.inserted();
                        deletedCount += result.deleted();
                    }

                    log.info("[CDC] updated={}, inserted={}, deleted={}, received={}", updatedCount, insertedCount, deletedCount, payloads.size());
                });
    }
}
