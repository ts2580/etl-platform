package com.etlplatform.common.storage.database.sql;

import com.etlplatform.common.storage.database.DatabaseVendor;

import java.util.List;

public interface DatabaseVendorStrategy {

    DatabaseVendor vendor();

    String quoteIdentifier(String identifier);

    String salesforceColumnType(String sfType, int length);

    String upsertValueReference(String columnName);

    SqlParameter bindValue(Object value, String sfType);

    String renderLiteral(Object value, String sfType);

    String columnCommentClause(String comment);

    List<String> afterCreateTableStatements(String schemaName, String tableName, List<ColumnDefinition> columns);

    String buildUpsertSql(String qualifiedTableName,
                          List<String> insertColumns,
                          List<String> mutableColumns,
                          String keyColumn,
                          String freshnessColumn,
                          String eventColumn);

    default String qualifyTableName(String schemaName, String tableName) {
        return quoteIdentifier(schemaName) + "." + quoteIdentifier(tableName);
    }

    record ColumnDefinition(String name, String typeDefinition, boolean nullable, String comment) {
    }
}
