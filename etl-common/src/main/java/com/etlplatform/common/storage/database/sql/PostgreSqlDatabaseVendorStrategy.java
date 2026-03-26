package com.etlplatform.common.storage.database.sql;

import com.etlplatform.common.storage.database.DatabaseVendor;

import java.util.List;

public final class PostgreSqlDatabaseVendorStrategy extends BaseDatabaseVendorStrategy {

    @Override
    public DatabaseVendor vendor() {
        return DatabaseVendor.POSTGRESQL;
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    @Override
    public String salesforceColumnType(String sfType, int length) {
        return switch (sfType) {
            case "id", "reference" -> "VARCHAR(18)";
            case "textarea" -> "TEXT";
            case "string", "picklist", "multipicklist", "phone", "url" -> "VARCHAR(" + Math.max(length, 255) + ")";
            case "boolean" -> "BOOLEAN";
            case "datetime" -> "TIMESTAMP(6)";
            case "date" -> "DATE";
            case "time" -> "TIME(6)";
            case "double", "percent", "currency" -> "DOUBLE PRECISION";
            case "int" -> "INTEGER";
            default -> "VARCHAR(255)";
        };
    }

    @Override
    public String upsertValueReference(String columnName) {
        return "EXCLUDED." + quoteIdentifier(columnName);
    }

    @Override
    public String buildUpsertSql(String qualifiedTableName,
                                 List<String> insertColumns,
                                 List<String> mutableColumns,
                                 String keyColumn,
                                 String freshnessColumn,
                                 String eventColumn) {
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ").append(qualifiedTableName).append(" (");
        appendQuotedColumns(sql, insertColumns);
        sql.append(") VALUES (");
        appendPlaceholders(sql, insertColumns.size());
        sql.append(") ON CONFLICT (").append(quoteIdentifier(keyColumn)).append(") DO UPDATE SET ");

        for (String column : mutableColumns) {
            sql.append(quoteIdentifier(column)).append(" = CASE WHEN ")
                    .append(upsertValueReference(freshnessColumn)).append(" IS NULL OR ")
                    .append(qualifiedTableName).append(".").append(quoteIdentifier(freshnessColumn)).append(" IS NULL OR ")
                    .append(upsertValueReference(freshnessColumn)).append(" >= ")
                    .append(qualifiedTableName).append(".").append(quoteIdentifier(freshnessColumn)).append(" THEN ")
                    .append(upsertValueReference(column)).append(" ELSE ")
                    .append(qualifiedTableName).append(".").append(quoteIdentifier(column)).append(" END, ");
        }

        sql.append(quoteIdentifier(freshnessColumn)).append(" = GREATEST(COALESCE(")
                .append(qualifiedTableName).append(".").append(quoteIdentifier(freshnessColumn)).append(", TIMESTAMP '1970-01-01 00:00:00'), COALESCE(")
                .append(upsertValueReference(freshnessColumn)).append(", TIMESTAMP '1970-01-01 00:00:00')), ")
                .append(quoteIdentifier(eventColumn)).append(" = GREATEST(COALESCE(")
                .append(qualifiedTableName).append(".").append(quoteIdentifier(eventColumn)).append(", TIMESTAMP '1970-01-01 00:00:00'), COALESCE(")
                .append(upsertValueReference(eventColumn)).append(", TIMESTAMP '1970-01-01 00:00:00'))");
        return sql.toString();
    }

    private void appendQuotedColumns(StringBuilder sql, List<String> columns) {
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append(quoteIdentifier(columns.get(i)));
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
