package com.apache.sfdc.common;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
public class SalesforceRecordMutationProcessor {

    public MutationResult apply(String selectedObject,
                                Map<String, Object> mapType,
                                SalesforceRecordMutation mutation,
                                SalesforceMutationRepositoryPort repository,
                                String sourceLabel) {

        if (mutation.type().isDelete()) {
            int deleted = delete(selectedObject, mutation, repository, sourceLabel);
            return new MutationResult(0, 0, deleted);
        }

        StringBuilder strUpdate = new StringBuilder();
        strUpdate.append("UPDATE config.").append(selectedObject).append(" SET ");

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

            int updated = repository.updateObject(strUpdate);
            if (updated == 0 && mutation.type().isCreateLike()) {
                int inserted = insertFallback(selectedObject, mapType, mutation, repository, sourceLabel);
                return new MutationResult(updated, inserted, 0);
            }
            return new MutationResult(updated, 0, 0);
        }

        if (mutation.type().isCreateLike()) {
            int inserted = insertMinimal(selectedObject, mutation.sfid(), repository);
            return new MutationResult(0, inserted, 0);
        }

        return new MutationResult(0, 0, 0);
    }

    private int delete(String selectedObject,
                       SalesforceRecordMutation mutation,
                       SalesforceMutationRepositoryPort repository,
                       String sourceLabel) {
        Instant deleteStart = Instant.now();
        StringBuilder strDelete = SalesforceObjectSchemaBuilder.buildConditionalDeleteSql(
                selectedObject,
                SqlSanitizer.quoteSfid(mutation.sfid()),
                mutation.incomingEventLiteral()
        );
        int deleted = repository.deleteObject(strDelete);
        Duration interval = Duration.between(deleteStart, Instant.now());
        log.info("[{}] deleted={}, sfid={}, took={}ms", sourceLabel, deleted, mutation.sfid(), interval.toMillis());
        return deleted;
    }

    private int appendAssignments(StringBuilder strUpdate,
                                  Map<String, Object> mapType,
                                  JsonNode payload,
                                  Set<String> targetFields,
                                  Set<String> nulledFields) {
        int assignmentCount = 0;
        for (String fieldName : targetFields) {
            strUpdate.append(fieldName).append(" = ");
            JsonNode fieldValue = payload.get(fieldName);
            if (nulledFields.contains(fieldName) || fieldValue == null || fieldValue.isNull()) {
                strUpdate.append("null,");
            } else {
                strUpdate.append(SqlSanitizer.sanitizeValue(fieldValue, String.valueOf(mapType.get(fieldName)))).append(",");
            }
            assignmentCount++;
        }
        return assignmentCount;
    }

    private int insertFallback(String selectedObject,
                               Map<String, Object> mapType,
                               SalesforceRecordMutation mutation,
                               SalesforceMutationRepositoryPort repository,
                               String sourceLabel) {
        List<String> insertFields = new ArrayList<>(mutation.targetFields());
        StringBuilder valuesBuilder = new StringBuilder(SqlSanitizer.quoteString(mutation.sfid()));

        for (String fieldName : insertFields) {
            JsonNode fieldValue = mutation.payload().get(fieldName);
            if (mutation.nulledFields().contains(fieldName) || fieldValue == null || fieldValue.isNull()) {
                valuesBuilder.append(",null");
            } else {
                valuesBuilder.append(",").append(SqlSanitizer.sanitizeValue(fieldValue, String.valueOf(mapType.get(fieldName))));
            }
        }

        valuesBuilder.append(",")
                .append(mutation.incomingLastModifiedLiteral())
                .append(",")
                .append(mutation.incomingEventLiteral());

        String upperQuery = SalesforceObjectSchemaBuilder.buildInsertSql(selectedObject, String.join(",", insertFields));
        String tailQuery = SalesforceObjectSchemaBuilder.buildInsertTail(selectedObject, insertFields);

        Instant insertStart = Instant.now();
        int inserted = repository.insertObject(upperQuery, List.of("(" + valuesBuilder + ")"), tailQuery);
        Duration interval = Duration.between(insertStart, Instant.now());
        log.info("[{}] insert fallback inserted={}, took={}ms", sourceLabel, inserted, interval.toMillis());
        return inserted;
    }

    private int insertMinimal(String selectedObject, String sfid, SalesforceMutationRepositoryPort repository) {
        String upperQuery = "Insert Into config." + selectedObject + "(sfid, "
                + SalesforceObjectSchemaBuilder.INTERNAL_LAST_MODIFIED_COLUMN + ", "
                + SalesforceObjectSchemaBuilder.INTERNAL_LAST_EVENT_AT_COLUMN + ") values";
        return repository.insertObject(upperQuery, List.of("(" + SqlSanitizer.quoteSfid(sfid) + ", null, null)"), "");
    }

    public record MutationResult(int updated, int inserted, int deleted) {
    }
}
