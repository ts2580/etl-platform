package com.apache.sfdc.common;

import com.apache.sfdc.streaming.dto.FieldDefinition;
import com.etlplatform.common.storage.database.DatabaseVendor;
import com.etlplatform.common.storage.database.OracleRoutingNaming;
import com.etlplatform.common.storage.database.sql.BoundBatchSql;
import com.etlplatform.common.storage.database.sql.DatabaseVendorStrategies;
import com.etlplatform.common.storage.database.sql.DatabaseVendorStrategy;
import com.etlplatform.common.storage.database.sql.SqlParameter;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class SalesforceObjectSchemaBuilder {

    public static final String LAST_MODIFIED_FIELD = "LastModifiedDate";
    public static final String INTERNAL_LAST_MODIFIED_COLUMN = "_oc_last_modified_at";
    public static final String INTERNAL_LAST_EVENT_AT_COLUMN = "_oc_last_event_at";

    private SalesforceObjectSchemaBuilder() {
    }

    public static SchemaResult buildSchema(String targetSchema, String selectedObject, JsonNode fieldsNode) throws IOException {
        return buildSchema(targetSchema, selectedObject, null, fieldsNode, new ObjectMapper(), DatabaseVendorStrategies.defaultStrategy());
    }

    public static SchemaResult buildSchema(String targetSchema, String selectedObject, JsonNode fieldsNode, ObjectMapper objectMapper) throws IOException {
        return buildSchema(targetSchema, selectedObject, null, fieldsNode, objectMapper, DatabaseVendorStrategies.defaultStrategy());
    }

    public static SchemaResult buildSchema(String targetSchema,
                                           String selectedObject,
                                           JsonNode fieldsNode,
                                           ObjectMapper objectMapper,
                                           DatabaseVendorStrategy strategy) throws IOException {
        return buildSchema(targetSchema, selectedObject, null, fieldsNode, objectMapper, strategy);
    }

    public static SchemaResult buildSchema(String targetSchema,
                                           String selectedObject,
                                           String orgName,
                                           JsonNode fieldsNode,
                                           ObjectMapper objectMapper,
                                           DatabaseVendorStrategy strategy) throws IOException {
        if (fieldsNode == null || !fieldsNode.isArray()) {
            throw new IllegalArgumentException("Invalid Salesforce describe fields response");
        }

        SqlSanitizer.validateSchemaName(targetSchema);
        SqlSanitizer.validateTableName(selectedObject);

        List<FieldDefinition> listDef = objectMapper.convertValue(fieldsNode, new TypeReference<List<FieldDefinition>>() { });
        StringBuilder ddl = new StringBuilder();
        StringBuilder soql = new StringBuilder();
        StringBuilder soqlForPushTopic = new StringBuilder();
        List<String> listFields = new ArrayList<>();
        Map<String, String> mapType = new HashMap<>();
        List<DatabaseVendorStrategy.ColumnDefinition> columnDefinitions = new ArrayList<>();

        String physicalTableName = resolvePhysicalTableName(targetSchema, selectedObject, orgName, strategy);

        ddl.append("CREATE TABLE ");
        if (strategy.vendor() != DatabaseVendor.ORACLE) {
            ddl.append("IF NOT EXISTS ");
        }
        ddl.append(qualifiedName(targetSchema, physicalTableName, strategy)).append("(");

        for (FieldDefinition obj : listDef) {
            mapType.put(obj.name, obj.type);
            String label = (obj.label == null ? "" : obj.label).replace("'", "`");

            if ("id".equals(obj.type)) {
                appendColumn(ddl, columnDefinitions, strategy, "sfid", strategy.salesforceColumnType("id", 18) + " PRIMARY KEY NOT NULL", false, label);
                continue;
            }

            String type = strategy.salesforceColumnType(obj.type, obj.length);
            appendColumn(ddl, columnDefinitions, strategy, obj.name, type, true, label);
            listFields.add(obj.name);
        }

        appendColumn(ddl, columnDefinitions, strategy,
                INTERNAL_LAST_MODIFIED_COLUMN,
                strategy.salesforceColumnType("datetime", 0),
                true,
                "OpenClaw latest LastModifiedDate");
        appendColumn(ddl, columnDefinitions, strategy,
                INTERNAL_LAST_EVENT_AT_COLUMN,
                strategy.salesforceColumnType("datetime", 0),
                true,
                "OpenClaw latest applied event time");

        ddl.deleteCharAt(ddl.length() - 1);
        ddl.append(")");

        for (String field : listFields) {
            SqlSanitizer.validateIdentifier(field);
            soql.append(field).append(",");
            if (!"textarea".equals(mapType.get(field))) {
                soqlForPushTopic.append(field).append(",");
            }
        }

        if (soql.isEmpty()) {
            throw new IllegalArgumentException("No supported fields for object: " + selectedObject);
        }

        soql.deleteCharAt(soql.length() - 1);
        soqlForPushTopic.deleteCharAt(soqlForPushTopic.length() - 1);

        List<String> ddlStatements = new ArrayList<>();
        if (strategy.vendor() == DatabaseVendor.ORACLE) {
            ddlStatements.add(wrapOracleCreateTableIfAbsent(ddl.toString()));
        } else {
            ddlStatements.add(ddl.toString());
        }
        ddlStatements.addAll(strategy.afterCreateTableStatements(targetSchema, selectedObject, columnDefinitions));

        return new SchemaResult(String.join(";\n", ddlStatements) + ";", mapType, listFields, soql.toString(), soqlForPushTopic.toString());
    }

    public static String buildInsertValues(JsonNode record, List<String> fields, Map<String, String> mapType) {
        StringBuilder underQuery = new StringBuilder();
        underQuery.append("(").append(SqlSanitizer.quoteString(record.get("Id").asText())).append(",");

        for (String field : fields) {
            underQuery.append(SqlSanitizer.sanitizeValue(record.get(field), mapType.get(field))).append(",");
        }

        String lastModifiedLiteral = lastModifiedLiteral(record);
        underQuery.append(lastModifiedLiteral).append(",").append(lastModifiedLiteral).append(")");
        return underQuery.toString();
    }

    public static BoundBatchSql buildPreparedInsertBatch(String targetSchema,
                                                         String selectedObject,
                                                         SchemaResult schemaResult,
                                                         JsonNode records,
                                                         DatabaseVendorStrategy strategy) {
        List<String> columns = buildInsertColumns(schemaResult.fields());
        String sql = strategy.buildUpsertSql(
                qualifiedName(targetSchema, selectedObject, null, strategy),
                columns,
                schemaResult.fields(),
                "sfid",
                INTERNAL_LAST_MODIFIED_COLUMN,
                INTERNAL_LAST_EVENT_AT_COLUMN
        );

        List<List<SqlParameter>> parameterGroups = new ArrayList<>();
        for (JsonNode record : records) {
            List<SqlParameter> params = new ArrayList<>();
            params.add(strategy.bindValue(record.path("Id").asText(), "id"));
            for (String field : schemaResult.fields()) {
                params.add(strategy.bindValue(SqlSanitizer.toRawValue(record.get(field)), schemaResult.mapType().get(field)));
            }
            Object lastModifiedValue = lastModifiedValue(record);
            params.add(strategy.bindValue(lastModifiedValue, "datetime"));
            params.add(strategy.bindValue(lastModifiedValue, "datetime"));
            parameterGroups.add(params);
        }

        return new BoundBatchSql(sql, parameterGroups);
    }

    public static String buildInsertSql(String targetSchema, String selectedObject, String soql) {
        DatabaseVendorStrategy strategy = DatabaseVendorStrategies.defaultStrategy();
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ").append(qualifiedName(targetSchema, selectedObject, strategy)).append("(")
                .append(strategy.quoteIdentifier("sfid")).append(", ");
        for (String field : soql.split(",")) {
            sql.append(strategy.quoteIdentifier(field.trim())).append(", ");
        }
        sql.append(strategy.quoteIdentifier(INTERNAL_LAST_MODIFIED_COLUMN)).append(", ")
                .append(strategy.quoteIdentifier(INTERNAL_LAST_EVENT_AT_COLUMN))
                .append(") VALUES");
        return sql.toString();
    }

    public static String buildInsertTail(String selectedObject, List<String> fields) {
        return buildInsertTail(fields, DatabaseVendorStrategies.defaultStrategy());
    }

    public static String buildInsertTail(List<String> fields, DatabaseVendorStrategy strategy) {
        return strategy.vendor() == DatabaseVendorStrategies.defaultStrategy().vendor()
                ? buildDefaultInsertTail(fields, strategy)
                : "";
    }

    private static String buildDefaultInsertTail(List<String> fields, DatabaseVendorStrategy strategy) {
        StringBuilder tail = new StringBuilder();
        tail.append("ON DUPLICATE KEY UPDATE ");

        for (String field : fields) {
            tail.append(strategy.quoteIdentifier(field)).append(" = IF(")
                    .append(strategy.upsertValueReference(INTERNAL_LAST_MODIFIED_COLUMN)).append(" IS NULL OR ")
                    .append(strategy.quoteIdentifier(INTERNAL_LAST_MODIFIED_COLUMN)).append(" IS NULL OR ")
                    .append(strategy.upsertValueReference(INTERNAL_LAST_MODIFIED_COLUMN)).append(" >= ")
                    .append(strategy.quoteIdentifier(INTERNAL_LAST_MODIFIED_COLUMN))
                    .append(", ")
                    .append(strategy.upsertValueReference(field))
                    .append(", ")
                    .append(strategy.quoteIdentifier(field))
                    .append("), ");
        }

        tail.append(strategy.quoteIdentifier(INTERNAL_LAST_MODIFIED_COLUMN)).append(" = GREATEST(COALESCE(")
                .append(strategy.quoteIdentifier(INTERNAL_LAST_MODIFIED_COLUMN)).append(", '1970-01-01 00:00:00'), COALESCE(")
                .append(strategy.upsertValueReference(INTERNAL_LAST_MODIFIED_COLUMN)).append(", '1970-01-01 00:00:00')), ")
                .append(strategy.quoteIdentifier(INTERNAL_LAST_EVENT_AT_COLUMN)).append(" = GREATEST(COALESCE(")
                .append(strategy.quoteIdentifier(INTERNAL_LAST_EVENT_AT_COLUMN)).append(", '1970-01-01 00:00:00'), COALESCE(")
                .append(strategy.upsertValueReference(INTERNAL_LAST_EVENT_AT_COLUMN)).append(", '1970-01-01 00:00:00'))");
        return tail.toString();
    }

    public static String buildInitialQuery(String selectedObject, List<String> fields) {
        return "SELECT Id, " + String.join(",", fields) + " FROM " + selectedObject;
    }

    public static Object lastModifiedValue(JsonNode payload) {
        JsonNode node = payload != null ? payload.get(LAST_MODIFIED_FIELD) : null;
        return SqlSanitizer.toRawValue(node);
    }

    public static String lastModifiedLiteral(JsonNode payload) {
        Object value = lastModifiedValue(payload);
        return value == null ? "null" : DatabaseVendorStrategies.defaultStrategy().renderLiteral(value, "datetime");
    }

    public static void appendFreshnessAssignments(StringBuilder sql, String incomingLastModifiedLiteral, String incomingEventLiteral) {
        sql.append(SqlSanitizer.quoteIdentifier(INTERNAL_LAST_MODIFIED_COLUMN)).append(" = ").append(incomingLastModifiedLiteral).append(",")
                .append(SqlSanitizer.quoteIdentifier(INTERNAL_LAST_EVENT_AT_COLUMN)).append(" = ").append(incomingEventLiteral).append(",");
    }

    public static void appendFreshnessWhere(StringBuilder sql, String sfidLiteral, String incomingLastModifiedLiteral) {
        sql.append(" WHERE ").append(SqlSanitizer.quoteIdentifier("sfid")).append(" = ").append(sfidLiteral);
        if (!"null".equalsIgnoreCase(incomingLastModifiedLiteral)) {
            sql.append(" AND (")
                    .append(SqlSanitizer.quoteIdentifier(INTERNAL_LAST_MODIFIED_COLUMN)).append(" IS NULL OR ")
                    .append(SqlSanitizer.quoteIdentifier(INTERNAL_LAST_MODIFIED_COLUMN)).append(" <= ").append(incomingLastModifiedLiteral)
                    .append(")");
        }
        sql.append(";");
    }

    public static StringBuilder buildConditionalDeleteSql(String targetSchema, String selectedObject, String sfidLiteral, String incomingEventLiteral) {
        StringBuilder sql = new StringBuilder();
        sql.append("DELETE FROM ").append(qualifiedName(targetSchema, selectedObject, DatabaseVendorStrategies.defaultStrategy()))
                .append(" WHERE ").append(SqlSanitizer.quoteIdentifier("sfid")).append(" = ").append(sfidLiteral)
                .append(" AND (")
                .append(SqlSanitizer.quoteIdentifier(INTERNAL_LAST_EVENT_AT_COLUMN)).append(" IS NULL OR ")
                .append(SqlSanitizer.quoteIdentifier(INTERNAL_LAST_EVENT_AT_COLUMN)).append(" <= ").append(incomingEventLiteral)
                .append(");");
        return sql;
    }

    public static String qualifiedName(String targetSchema, String tableName) {
        return qualifiedName(targetSchema, tableName, DatabaseVendorStrategies.defaultStrategy());
    }

    public static String qualifiedName(String targetSchema, String tableName, DatabaseVendorStrategy strategy) {
        SqlSanitizer.validateSchemaName(targetSchema);
        SqlSanitizer.validateTableName(tableName);
        return strategy.qualifyTableName(targetSchema, tableName);
    }

    public static String resolvePhysicalTableName(String targetSchema,
                                                  String tableName,
                                                  String orgName,
                                                  DatabaseVendorStrategy strategy) {
        SqlSanitizer.validateSchemaName(targetSchema);
        SqlSanitizer.validateTableName(tableName);
        if (strategy.vendor() == DatabaseVendor.ORACLE) {
            return OracleRoutingNaming.buildTableName(orgName, tableName);
        }
        return tableName;
    }

    public static String qualifiedName(String targetSchema,
                                       String logicalTableName,
                                       String orgName,
                                       DatabaseVendorStrategy strategy) {
        String physicalTableName = resolvePhysicalTableName(targetSchema, logicalTableName, orgName, strategy);
        return qualifiedName(targetSchema, physicalTableName, strategy);
    }

    public static String buildDropTableSql(String targetSchema, String tableName, DatabaseVendorStrategy strategy) {
        return buildDropTableSql(targetSchema, tableName, null, strategy);
    }

    public static String buildDropTableSql(String targetSchema, String tableName, String orgName, DatabaseVendorStrategy strategy) {
        String qualifiedName = qualifiedName(targetSchema, tableName, orgName, strategy);
        return switch (strategy.vendor()) {
            case ORACLE -> "BEGIN EXECUTE IMMEDIATE 'DROP TABLE " + qualifiedName.replace("'", "''")
                    + " CASCADE CONSTRAINTS PURGE'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END";
            default -> "DROP TABLE IF EXISTS " + qualifiedName;
        };
    }

    private static String wrapOracleCreateTableIfAbsent(String createTableSql) {
        String escaped = createTableSql.replace("'", "''");
        return "BEGIN EXECUTE IMMEDIATE '" + escaped
                + "'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -955 THEN RAISE; END IF; END";
    }

    private static List<String> buildInsertColumns(List<String> fields) {
        List<String> columns = new ArrayList<>();
        columns.add("sfid");
        columns.addAll(fields);
        columns.add(INTERNAL_LAST_MODIFIED_COLUMN);
        columns.add(INTERNAL_LAST_EVENT_AT_COLUMN);
        return columns;
    }

    private static String buildOracleUpsertSql(String qualifiedTableName,
                                               List<String> insertColumns,
                                               List<String> mutableColumns,
                                               Map<String, String> mapType,
                                               DatabaseVendorStrategy strategy,
                                               String keyColumn,
                                               String freshnessColumn,
                                               String eventColumn) {
        String sourceAlias = "src";
        String targetAlias = "target";

        StringBuilder sql = new StringBuilder();
        sql.append("MERGE INTO ").append(qualifiedTableName).append(" ").append(targetAlias)
                .append(" USING (SELECT ");
        for (int i = 0; i < insertColumns.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }
            String column = insertColumns.get(i);
            sql.append(oracleCastExpression(column, mapType)).append(" AS ").append(strategy.quoteIdentifier(column));
        }
        sql.append(" FROM dual) ").append(sourceAlias)
                .append(" ON (").append(targetAlias).append(".").append(strategy.quoteIdentifier(keyColumn))
                .append(" = ").append(sourceAlias).append(".").append(strategy.quoteIdentifier(keyColumn)).append(") ")
                .append("WHEN MATCHED THEN UPDATE SET ");

        for (String column : mutableColumns) {
            sql.append(targetAlias).append(".").append(strategy.quoteIdentifier(column)).append(" = ")
                    .append(sourceAlias).append(".").append(strategy.quoteIdentifier(column)).append(", ");
        }

        sql.append(targetAlias).append(".").append(strategy.quoteIdentifier(freshnessColumn)).append(" = NVL(")
                .append(sourceAlias).append(".").append(strategy.quoteIdentifier(freshnessColumn)).append(", ")
                .append(targetAlias).append(".").append(strategy.quoteIdentifier(freshnessColumn)).append("), ")
                .append(targetAlias).append(".").append(strategy.quoteIdentifier(eventColumn)).append(" = NVL(")
                .append(sourceAlias).append(".").append(strategy.quoteIdentifier(eventColumn)).append(", ")
                .append(targetAlias).append(".").append(strategy.quoteIdentifier(eventColumn)).append(") ")
                .append("WHERE ")
                .append(sourceAlias).append(".").append(strategy.quoteIdentifier(freshnessColumn)).append(" IS NULL OR ")
                .append(targetAlias).append(".").append(strategy.quoteIdentifier(freshnessColumn)).append(" IS NULL OR ")
                .append(sourceAlias).append(".").append(strategy.quoteIdentifier(freshnessColumn)).append(" >= ")
                .append(targetAlias).append(".").append(strategy.quoteIdentifier(freshnessColumn)).append(" ")
                .append("WHEN NOT MATCHED THEN INSERT (");
        appendQuotedColumns(sql, insertColumns, strategy);
        sql.append(") VALUES (");
        for (int i = 0; i < insertColumns.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append(sourceAlias).append(".").append(strategy.quoteIdentifier(insertColumns.get(i)));
        }
        sql.append(")");
        return sql.toString();
    }

    private static String oracleCastExpression(String column, Map<String, String> mapType) {
        String sfType = switch (column) {
            case "sfid" -> "id";
            case INTERNAL_LAST_MODIFIED_COLUMN, INTERNAL_LAST_EVENT_AT_COLUMN -> "datetime";
            default -> mapType.getOrDefault(column, "string");
        };

        return switch (sfType) {
            case "boolean" -> "CAST(? AS NUMBER(1))";
            case "double", "percent", "currency" -> "CAST(? AS BINARY_DOUBLE)";
            case "int" -> "CAST(? AS NUMBER(10))";
            case "datetime" -> "CAST(? AS TIMESTAMP(6))";
            case "date" -> "CAST(? AS DATE)";
            case "textarea" -> "TO_CLOB(?)";
            case "id", "reference" -> "CAST(? AS VARCHAR2(18))";
            case "time" -> "CAST(? AS VARCHAR2(18))";
            default -> "CAST(? AS VARCHAR2(4000))";
        };
    }

    private static void appendQuotedColumns(StringBuilder sql, List<String> columns, DatabaseVendorStrategy strategy) {
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append(strategy.quoteIdentifier(columns.get(i)));
        }
    }

    private static void appendColumn(StringBuilder ddl,
                                     List<DatabaseVendorStrategy.ColumnDefinition> columnDefinitions,
                                     DatabaseVendorStrategy strategy,
                                     String name,
                                     String typeDefinition,
                                     boolean nullable,
                                     String comment) {
        ddl.append(strategy.quoteIdentifier(name)).append(" ").append(typeDefinition);
        if (!nullable && !typeDefinition.toUpperCase().contains("NOT NULL")) {
            ddl.append(" NOT NULL");
        }
        if (nullable && !typeDefinition.toUpperCase().contains("NULL") && !typeDefinition.toUpperCase().contains("PRIMARY KEY")) {
            ddl.append(" NULL");
        }
        ddl.append(strategy.columnCommentClause(comment)).append(",");
        columnDefinitions.add(new DatabaseVendorStrategy.ColumnDefinition(name, typeDefinition, nullable, comment));
    }

    public record SchemaResult(String ddl, Map<String, String> mapType, List<String> fields,
                               String soql, String soqlForPushTopic) {
    }
}
