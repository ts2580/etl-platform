package com.apache.sfdc.common;

import com.etlplatform.common.storage.database.sql.BoundBatchSql;
import com.etlplatform.common.storage.database.sql.BoundSql;
import com.etlplatform.common.storage.database.sql.DatabaseVendorStrategy;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SalesforceRecordMutationProcessor {

    private static final Logger log = LoggerFactory.getLogger(SalesforceRecordMutationProcessor.class);

    private final SalesforceRecordMutationPayloadResolver payloadResolver = new SalesforceRecordMutationPayloadResolver();
    private final SalesforceRecordMutationSqlBuilder sqlBuilder = new SalesforceRecordMutationSqlBuilder();

    public MutationResult apply(String targetSchema,
                                String selectedObject,
                                String targetTable,
                                String orgName,
                                Map<String, Object> mapType,
                                SalesforceRecordMutation mutation,
                                SalesforceMutationRepositoryPort repository,
                                String sourceLabel) {

        SalesforceTargetTableResolver.ResolvedTargetTable resolvedTargetTable =
                resolveTargetTable(targetSchema, selectedObject, targetTable, orgName, repository.vendorStrategy());
        String physicalTableName = resolvedTargetTable.physicalTableName();

        if (mutation.type().isDelete()) {
            int deleted = delete(targetSchema, physicalTableName, mutation, repository, sourceLabel);
            return new MutationResult(0, 0, deleted);
        }

        if (repository.supportsBoundStatements()) {
            BoundSql boundUpdate = buildBoundUpdate(targetSchema, physicalTableName, mapType, mutation, repository.vendorStrategy());
            if (boundUpdate != null) {
                int updated = repository.updateObject(boundUpdate);
                if (updated == 0 && mutation.type().isCreateLike()) {
                    int inserted = repository.insertObject(buildBoundInsertFallback(targetSchema, physicalTableName, mapType, mutation, repository.vendorStrategy()));
                    return new MutationResult(updated, inserted, 0);
                }
                return new MutationResult(updated, 0, 0);
            }
        } else {
            StringBuilder strUpdate = new StringBuilder();
            strUpdate.append("UPDATE ").append(SalesforceObjectSchemaBuilder.qualifiedName(targetSchema, physicalTableName, repository.vendorStrategy())).append(" SET ");

            int assignmentCount = appendAssignments(strUpdate, mapType, mutation.payload(), mutation.targetFields(), mutation.nulledFields());
            if (assignmentCount > 0) {
                SalesforceObjectSchemaBuilder.appendFreshnessAssignments(
                        strUpdate,
                        mutation.incomingLastModifiedLiteral(),
                        mutation.incomingEventLiteral()
                );
                strUpdate.deleteCharAt(strUpdate.length() - 1);
                SalesforceObjectSchemaBuilder.appendFreshnessWhere(
                        strUpdate,
                        SqlSanitizer.quoteString(mutation.sfid()),
                        mutation.incomingLastModifiedLiteral()
                );

                log.warn("[CDC-UPDATE-SQL]\n  sfid: {}\n  sql:\n{}", mutation.sfid(), formatIndentedBlock(strUpdate.toString()));
                int updated = repository.updateObject(strUpdate);
                if (updated == 0 && mutation.type().isCreateLike()) {
                    int inserted = insertFallback(targetSchema, physicalTableName, mapType, mutation, repository);
                    return new MutationResult(updated, inserted, 0);
                }
                return new MutationResult(updated, 0, 0);
            }
        }

        if (mutation.type().isCreateLike()) {
            if (repository.supportsBoundStatements()) {
                int inserted = repository.insertObject(buildBoundMinimalInsert(targetSchema, physicalTableName, mutation.sfid(), repository.vendorStrategy()));
                return new MutationResult(0, inserted, 0);
            }
            int inserted = insertMinimal(targetSchema, physicalTableName, mutation.sfid(), repository);
            return new MutationResult(0, inserted, 0);
        }

        log.warn("[CDC-APPLY] no-op mutation. type={}, sfid={}, targetFields={}, payloadKeys={}",
                mutation.type(), mutation.sfid(), mutation.targetFields(), mutation.payload().properties().stream().map(Map.Entry::getKey).toList());
        return new MutationResult(0, 0, 0);
    }

    private int delete(String targetSchema,
                       String selectedObject,
                       SalesforceRecordMutation mutation,
                       SalesforceMutationRepositoryPort repository,
                       String sourceLabel) {
        if (repository.supportsBoundStatements()) {
            return repository.deleteObject(buildBoundDelete(targetSchema, selectedObject, mutation, repository.vendorStrategy()));
        }

        StringBuilder strDelete = SalesforceObjectSchemaBuilder.buildConditionalDeleteSql(
                targetSchema,
                selectedObject,
                SqlSanitizer.quoteSfid(mutation.sfid()),
                mutation.incomingEventLiteral()
        );
        log.warn("[CDC-DELETE-SQL]\n  sfid: {}\n  sql:\n{}", mutation.sfid(), formatIndentedBlock(strDelete.toString()));
        return repository.deleteObject(strDelete);
    }

    private BoundSql buildBoundUpdate(String targetSchema,
                                      String selectedObject,
                                      Map<String, Object> mapType,
                                      SalesforceRecordMutation mutation,
                                      DatabaseVendorStrategy strategy) {
        return sqlBuilder.buildBoundUpdate(targetSchema, selectedObject, mapType, mutation, strategy, payloadResolver);
    }

    private BoundSql buildBoundDelete(String targetSchema,
                                      String selectedObject,
                                      SalesforceRecordMutation mutation,
                                      DatabaseVendorStrategy strategy) {
        return sqlBuilder.buildBoundDelete(targetSchema, selectedObject, mutation, strategy);
    }

    private BoundBatchSql buildBoundInsertFallback(String targetSchema,
                                                   String selectedObject,
                                                   Map<String, Object> mapType,
                                                   SalesforceRecordMutation mutation,
                                                   DatabaseVendorStrategy strategy) {
        return sqlBuilder.buildBoundInsertFallback(targetSchema, selectedObject, mapType, mutation, strategy, payloadResolver);
    }

    private BoundBatchSql buildBoundMinimalInsert(String targetSchema,
                                                  String selectedObject,
                                                  String sfid,
                                                  DatabaseVendorStrategy strategy) {
        return sqlBuilder.buildBoundMinimalInsert(targetSchema, selectedObject, sfid, strategy);
    }

    private int appendAssignments(StringBuilder strUpdate,
                                  Map<String, Object> mapType,
                                  JsonNode payload,
                                  Set<String> targetFields,
                                  Set<String> nulledFields) {
        int assignmentCount = 0;
        for (String fieldName : targetFields) {
            strUpdate.append(SqlSanitizer.quoteIdentifier(fieldName)).append(" = ");
            JsonNode rawFieldValue = payloadResolver.resolveRawFieldValue(payload, fieldName);
            JsonNode fieldValue = payloadResolver.normalizeFieldValue(rawFieldValue, fieldName);
            if (nulledFields.contains(fieldName) || fieldValue == null || fieldValue.isNull()) {
                strUpdate.append("null,");
            } else {
                strUpdate.append(sanitizeFieldValue(fieldName, fieldValue, String.valueOf(mapType.get(fieldName)))).append(",");
            }
            assignmentCount++;
        }
        return assignmentCount;
    }

    private String sanitizeFieldValue(String fieldName, JsonNode fieldValue, String sfType) {
        if (fieldValue == null || fieldValue.isNull()) {
            return "null";
        }
        try {
            return SqlSanitizer.sanitizeValue(fieldValue, sfType);
        } catch (Exception primary) {
            if (fieldValue.isObject() || fieldValue.isArray()) {
                return SqlSanitizer.quoteString(fieldValue.toString());
            }
            throw primary;
        }
    }

    private int insertFallback(String targetSchema,
                               String selectedObject,
                               Map<String, Object> mapType,
                               SalesforceRecordMutation mutation,
                               SalesforceMutationRepositoryPort repository) {
        List<String> insertFields = new ArrayList<>(mutation.targetFields());
        StringBuilder valuesBuilder = new StringBuilder(SqlSanitizer.quoteString(mutation.sfid()));

        for (String fieldName : insertFields) {
            JsonNode fieldValue = payloadResolver.normalizeFieldValue(payloadResolver.resolveRawFieldValue(mutation.payload(), fieldName), fieldName);
            if (mutation.nulledFields().contains(fieldName) || fieldValue == null || fieldValue.isNull()) {
                valuesBuilder.append(",null");
            } else {
                valuesBuilder.append(",").append(sanitizeFieldValue(fieldName, fieldValue, String.valueOf(mapType.get(fieldName))));
            }
        }

        valuesBuilder.append(",")
                .append(mutation.incomingLastModifiedLiteral())
                .append(",")
                .append(mutation.incomingEventLiteral());

        String upperQuery = SalesforceObjectSchemaBuilder.buildInsertSql(targetSchema, selectedObject, String.join(",", insertFields));
        String tailQuery = SalesforceObjectSchemaBuilder.buildInsertTail(selectedObject, insertFields);
        return repository.insertObject(upperQuery, List.of("(" + valuesBuilder + ")"), tailQuery);
    }

    private int insertMinimal(String targetSchema, String selectedObject, String sfid, SalesforceMutationRepositoryPort repository) {
        String upperQuery = "INSERT INTO " + SalesforceObjectSchemaBuilder.qualifiedName(targetSchema, selectedObject)
                + "(" + SqlSanitizer.quoteIdentifier("sfid") + ", "
                + SqlSanitizer.quoteIdentifier(SalesforceObjectSchemaBuilder.INTERNAL_LAST_MODIFIED_COLUMN) + ", "
                + SqlSanitizer.quoteIdentifier(SalesforceObjectSchemaBuilder.INTERNAL_LAST_EVENT_AT_COLUMN) + ") VALUES";
        String valuesSql = "(" + SqlSanitizer.quoteSfid(sfid) + ", null, null)";
        return repository.insertObject(upperQuery, List.of(valuesSql), "");
    }

    private SalesforceTargetTableResolver.ResolvedTargetTable resolveTargetTable(String targetSchema,
                                                                                  String selectedObject,
                                                                                  String targetTable,
                                                                                  String orgName,
                                                                                  DatabaseVendorStrategy strategy) {
        return SalesforceTargetTableResolver.resolveTargetTable(targetSchema, selectedObject, targetTable, orgName, strategy);
    }

    private String formatIndentedBlock(String text) {
        if (text == null || text.isBlank()) {
            return "    (empty)";
        }
        return "    " + text.replace("\r", "").replace("\n", "\n    ");
    }

    public record MutationResult(int updated, int inserted, int deleted) {
    }
}
