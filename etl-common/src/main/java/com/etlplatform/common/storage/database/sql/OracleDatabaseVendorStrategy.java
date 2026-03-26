package com.etlplatform.common.storage.database.sql;

import com.etlplatform.common.storage.database.DatabaseVendor;

import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public final class OracleDatabaseVendorStrategy extends BaseDatabaseVendorStrategy {

    @Override
    public DatabaseVendor vendor() {
        return DatabaseVendor.ORACLE;
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    @Override
    public String qualifyTableName(String schemaName, String tableName) {
        String normalizedSchema = schemaName == null ? null : schemaName.trim().toUpperCase(Locale.ROOT);
        return quoteIdentifier(normalizedSchema) + "." + quoteIdentifier(tableName);
    }

    @Override
    public SqlParameter bindValue(Object value, String sfType) {
        if ("textarea".equals(sfType)) {
            return new SqlParameter(value == null ? null : String.valueOf(value), Types.CLOB);
        }
        return super.bindValue(value, sfType);
    }

    @Override
    public String salesforceColumnType(String sfType, int length) {
        return switch (sfType) {
            case "id", "reference" -> "VARCHAR2(18)";
            case "textarea" -> "CLOB";
            case "string", "picklist", "multipicklist", "phone", "url" -> "VARCHAR2(" + Math.max(length, 255) + ")";
            case "boolean" -> "NUMBER(1)";
            case "datetime" -> "TIMESTAMP(6)";
            case "date" -> "DATE";
            case "time" -> "VARCHAR2(18)";
            case "double", "percent", "currency" -> "BINARY_DOUBLE";
            case "int" -> "NUMBER(10)";
            default -> "VARCHAR2(255)";
        };
    }

    @Override
    public SqlParameter bindBoolean(Boolean value) {
        return new SqlParameter(value == null ? null : (value ? 1 : 0), Types.NUMERIC);
    }

    @Override
    public SqlParameter bindDateTime(LocalDateTime value) {
        return new SqlParameter(value, Types.TIMESTAMP);
    }

    @Override
    public SqlParameter bindDate(LocalDate value) {
        return new SqlParameter(value, Types.DATE);
    }

    @Override
    public SqlParameter bindTime(LocalTime value) {
        return new SqlParameter(value == null ? null : formatTime(value), Types.VARCHAR);
    }

    @Override
    protected int sqlType(String sfType) {
        return switch (sfType) {
            case "double", "percent", "currency" -> Types.DOUBLE;
            case "int" -> Types.NUMERIC;
            case "boolean" -> Types.NUMERIC;
            case "datetime" -> Types.TIMESTAMP;
            case "date" -> Types.DATE;
            case "time" -> Types.VARCHAR;
            default -> Types.VARCHAR;
        };
    }

    @Override
    protected String renderBooleanLiteral(Boolean value) {
        return value ? "1" : "0";
    }

    @Override
    public String upsertValueReference(String columnName) {
        return quoteIdentifier(columnName);
    }

    @Override
    public String buildUpsertSql(String qualifiedTableName,
                                 List<String> insertColumns,
                                 List<String> mutableColumns,
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
            sql.append("? AS ").append(quoteIdentifier(insertColumns.get(i)));
        }
        sql.append(" FROM dual) ").append(sourceAlias)
                .append(" ON (").append(targetAlias).append(".").append(quoteIdentifier(keyColumn))
                .append(" = ").append(sourceAlias).append(".").append(quoteIdentifier(keyColumn)).append(") ")
                .append("WHEN MATCHED THEN UPDATE SET ");

        for (String column : mutableColumns) {
            sql.append(targetAlias).append(".").append(quoteIdentifier(column)).append(" = ")
                    .append(sourceAlias).append(".").append(quoteIdentifier(column)).append(", ");
        }

        sql.append(targetAlias).append(".").append(quoteIdentifier(freshnessColumn)).append(" = NVL(")
                .append(sourceAlias).append(".").append(quoteIdentifier(freshnessColumn)).append(", ")
                .append(targetAlias).append(".").append(quoteIdentifier(freshnessColumn)).append("), ")
                .append(targetAlias).append(".").append(quoteIdentifier(eventColumn)).append(" = NVL(")
                .append(sourceAlias).append(".").append(quoteIdentifier(eventColumn)).append(", ")
                .append(targetAlias).append(".").append(quoteIdentifier(eventColumn)).append(") ")
                .append("WHERE ")
                .append(sourceAlias).append(".").append(quoteIdentifier(freshnessColumn)).append(" IS NULL OR ")
                .append(targetAlias).append(".").append(quoteIdentifier(freshnessColumn)).append(" IS NULL OR ")
                .append(sourceAlias).append(".").append(quoteIdentifier(freshnessColumn)).append(" >= ")
                .append(targetAlias).append(".").append(quoteIdentifier(freshnessColumn)).append(" ")
                .append("WHEN NOT MATCHED THEN INSERT (");
        appendQuotedColumns(sql, insertColumns);
        sql.append(") VALUES (");
        for (int i = 0; i < insertColumns.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append(sourceAlias).append(".").append(quoteIdentifier(insertColumns.get(i)));
        }
        sql.append(")");
        return sql.toString();
    }

    @Override
    public List<String> afterCreateTableStatements(String schemaName, String tableName, List<ColumnDefinition> columns) {
        List<String> statements = new ArrayList<>();
        String qualifiedTableName = qualifyTableName(schemaName, tableName);
        for (ColumnDefinition column : columns) {
            if (column.comment() == null || column.comment().isBlank()) {
                continue;
            }
            statements.add("COMMENT ON COLUMN " + qualifiedTableName + "." + quoteIdentifier(column.name())
                    + " IS '" + normalizeComment(column.comment()) + "'");
        }
        return statements;
    }

    private String oracleCastExpression(String column) {
        return switch (column) {
            case "sfid", "MasterRecordId", "ParentId", "OwnerId", "CreatedById", "LastModifiedById", "JigsawCompanyId", "DandbCompanyId", "OperatingHoursId" -> "CAST(? AS VARCHAR2(18))";
            case "IsDeleted", "Active__c" -> "CAST(? AS NUMBER(1))";
            case "BillingLatitude", "BillingLongitude", "ShippingLatitude", "ShippingLongitude", "AnnualRevenue", "NumberofLocations__c" -> "CAST(? AS BINARY_DOUBLE)";
            case "NumberOfEmployees", "YearStarted" -> "CAST(? AS NUMBER(10))";
            case "CreatedDate", "LastModifiedDate", "SystemModstamp", "LastViewedDate", "LastReferencedDate", "_oc_last_modified_at", "_oc_last_event_at" -> "CAST(? AS TIMESTAMP(6))";
            case "LastActivityDate", "SLAExpirationDate__c" -> "CAST(? AS DATE)";
            case "BillingStreet", "BillingAddress", "ShippingStreet", "ShippingAddress", "Description" -> "TO_CLOB(?)";
            default -> "CAST(? AS VARCHAR2(4000))";
        };
    }

    private void appendQuotedColumns(StringBuilder sql, List<String> columns) {
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append(quoteIdentifier(columns.get(i)));
        }
    }
}
