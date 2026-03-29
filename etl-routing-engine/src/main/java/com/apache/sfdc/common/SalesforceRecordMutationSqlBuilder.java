package com.apache.sfdc.common;

import com.etlplatform.common.storage.database.sql.BoundBatchSql;
import com.etlplatform.common.storage.database.sql.BoundSql;
import com.etlplatform.common.storage.database.sql.DatabaseVendorStrategy;
import com.etlplatform.common.storage.database.sql.SqlParameter;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class SalesforceRecordMutationSqlBuilder {

    public BoundSql buildBoundUpdate(String targetSchema,
                                    String selectedObject,
                                    Map<String, Object> mapType,
                                    SalesforceRecordMutation mutation,
                                    DatabaseVendorStrategy strategy,
                                    SalesforceRecordMutationPayloadResolver resolver) {
        StringBuilder sql = new StringBuilder();
        List<SqlParameter> parameters = new ArrayList<>();
        int assignmentCount = 0;

        sql.append("UPDATE ").append(SalesforceObjectSchemaBuilder.qualifiedName(targetSchema, selectedObject, strategy)).append(" SET ");
        for (String fieldName : mutation.targetFields()) {
            if (assignmentCount > 0) {
                sql.append(", ");
            }
            sql.append(strategy.quoteIdentifier(fieldName)).append(" = ?");

            JsonNode rawFieldValue = resolver.resolveRawFieldValue(mutation.payload(), fieldName);
            JsonNode fieldValue = resolver.normalizeFieldValue(rawFieldValue, fieldName);
            String sfType = String.valueOf(mapType.get(fieldName));
            if (mutation.nulledFields().contains(fieldName) || fieldValue == null || fieldValue.isNull()) {
                parameters.add(strategy.bindValue(null, sfType));
            } else {
                parameters.add(strategy.bindValue(resolver.toBindableFieldValue(fieldValue), sfType));
            }
            assignmentCount++;
        }

        if (assignmentCount == 0) {
            return null;
        }

        sql.append(", ").append(strategy.quoteIdentifier(SalesforceObjectSchemaBuilder.INTERNAL_LAST_MODIFIED_COLUMN)).append(" = ?");
        parameters.add(strategy.bindValue(mutation.incomingLastModifiedValue(), "datetime"));

        sql.append(", ").append(strategy.quoteIdentifier(SalesforceObjectSchemaBuilder.INTERNAL_LAST_EVENT_AT_COLUMN)).append(" = ?");
        parameters.add(strategy.bindValue(mutation.incomingEventValue(), "datetime"));

        sql.append(" WHERE ").append(strategy.quoteIdentifier("sfid")).append(" = ?");
        parameters.add(strategy.bindValue(mutation.sfid(), "id"));

        if (mutation.incomingLastModifiedValue() != null) {
            sql.append(" AND (")
                    .append(strategy.quoteIdentifier(SalesforceObjectSchemaBuilder.INTERNAL_LAST_MODIFIED_COLUMN)).append(" IS NULL OR ")
                    .append(strategy.quoteIdentifier(SalesforceObjectSchemaBuilder.INTERNAL_LAST_MODIFIED_COLUMN)).append(" <= ?)");
            parameters.add(strategy.bindValue(mutation.incomingLastModifiedValue(), "datetime"));
        }

        return new BoundSql(sql.toString(), parameters);
    }

    public BoundSql buildBoundDelete(String targetSchema,
                                    String selectedObject,
                                    SalesforceRecordMutation mutation,
                                    DatabaseVendorStrategy strategy) {
        StringBuilder sql = new StringBuilder();
        sql.append("DELETE FROM ")
                .append(SalesforceObjectSchemaBuilder.qualifiedName(targetSchema, selectedObject, strategy))
                .append(" WHERE ")
                .append(strategy.quoteIdentifier("sfid"))
                .append(" = ?")
                .append(" AND (")
                .append(strategy.quoteIdentifier(SalesforceObjectSchemaBuilder.INTERNAL_LAST_EVENT_AT_COLUMN))
                .append(" IS NULL OR ")
                .append(strategy.quoteIdentifier(SalesforceObjectSchemaBuilder.INTERNAL_LAST_EVENT_AT_COLUMN))
                .append(" <= ?)");

        return new BoundSql(sql.toString(), List.of(
                strategy.bindValue(mutation.sfid(), "id"),
                strategy.bindValue(mutation.incomingEventValue(), "datetime")
        ));
    }

    public BoundBatchSql buildBoundInsertFallback(String targetSchema,
                                                 String selectedObject,
                                                 Map<String, Object> mapType,
                                                 SalesforceRecordMutation mutation,
                                                 DatabaseVendorStrategy strategy,
                                                 SalesforceRecordMutationPayloadResolver resolver) {
        List<String> insertFields = new ArrayList<>(mutation.targetFields());
        List<String> columns = new ArrayList<>();
        columns.add("sfid");
        columns.addAll(insertFields);
        columns.add(SalesforceObjectSchemaBuilder.INTERNAL_LAST_MODIFIED_COLUMN);
        columns.add(SalesforceObjectSchemaBuilder.INTERNAL_LAST_EVENT_AT_COLUMN);

        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ").append(SalesforceObjectSchemaBuilder.qualifiedName(targetSchema, selectedObject, strategy)).append(" (");
        appendQuotedColumns(sql, columns, strategy);
        sql.append(") VALUES (");
        appendPlaceholders(sql, columns.size());
        sql.append(") ").append(SalesforceObjectSchemaBuilder.buildInsertTail(insertFields, strategy));

        List<SqlParameter> parameters = new ArrayList<>();
        parameters.add(strategy.bindValue(mutation.sfid(), "id"));
        for (String fieldName : insertFields) {
            JsonNode fieldValue = resolver.normalizeFieldValue(resolver.resolveRawFieldValue(mutation.payload(), fieldName), fieldName);
            String sfType = String.valueOf(mapType.get(fieldName));
            if (mutation.nulledFields().contains(fieldName) || fieldValue == null || fieldValue.isNull()) {
                parameters.add(strategy.bindValue(null, sfType));
            } else {
                parameters.add(strategy.bindValue(resolver.toBindableFieldValue(fieldValue), sfType));
            }
        }
        parameters.add(strategy.bindValue(mutation.incomingLastModifiedValue(), "datetime"));
        parameters.add(strategy.bindValue(mutation.incomingEventValue(), "datetime"));
        return new BoundBatchSql(sql.toString(), List.of(parameters));
    }

    public BoundBatchSql buildBoundMinimalInsert(String targetSchema,
                                                String selectedObject,
                                                String sfid,
                                                DatabaseVendorStrategy strategy) {
        String sql = "INSERT INTO " + SalesforceObjectSchemaBuilder.qualifiedName(targetSchema, selectedObject, strategy)
                + " (" + strategy.quoteIdentifier("sfid") + ", "
                + strategy.quoteIdentifier(SalesforceObjectSchemaBuilder.INTERNAL_LAST_MODIFIED_COLUMN) + ", "
                + strategy.quoteIdentifier(SalesforceObjectSchemaBuilder.INTERNAL_LAST_EVENT_AT_COLUMN) + ") VALUES (?, ?, ?)";
        return new BoundBatchSql(sql, List.of(List.of(
                strategy.bindValue(sfid, "id"),
                strategy.bindValue(null, "datetime"),
                strategy.bindValue(null, "datetime")
        )));
    }

    private void appendQuotedColumns(StringBuilder sql, List<String> columns, DatabaseVendorStrategy strategy) {
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append(strategy.quoteIdentifier(columns.get(i)));
        }
    }

    private void appendPlaceholders(StringBuilder sql, int count) {
        for (int i = 0; i < count; i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append("?");
        }
    }
}
