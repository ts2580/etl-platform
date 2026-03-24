package com.apache.sfdc.common;

import com.etlplatform.common.error.AppException;
import com.etlplatform.common.storage.database.sql.DatabaseVendorStrategies;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.Set;
import java.util.regex.Pattern;

public final class SqlSanitizer {

    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
    private static final Pattern SFID = Pattern.compile("[A-Za-z0-9]{18}");

    private SqlSanitizer() {
    }

    public static void validateIdentifier(String identifier) {
        if (identifier == null || !IDENTIFIER.matcher(identifier).matches()) {
            throw new AppException("Invalid identifier: " + identifier);
        }
    }

    public static void validateTableName(String tableName) {
        validateIdentifier(tableName);
    }

    public static void validateSchemaName(String schemaName) {
        validateIdentifier(schemaName);
    }

    public static String sanitizeValue(JsonNode valueNode, String sfType) {
        return DatabaseVendorStrategies.defaultStrategy().renderLiteral(toRawValue(valueNode), sfType);
    }

    public static Object toRawValue(JsonNode valueNode) {
        if (valueNode == null || valueNode.isNull()) {
            return null;
        }
        if (valueNode.isBoolean()) {
            return valueNode.booleanValue();
        }
        if (valueNode.isIntegralNumber()) {
            return valueNode.longValue();
        }
        if (valueNode.isFloatingPointNumber()) {
            return valueNode.decimalValue();
        }
        if (valueNode.isTextual()) {
            return valueNode.textValue();
        }
        if (valueNode.isNumber()) {
            return valueNode.numberValue();
        }
        return valueNode.asText();
    }

    public static String quoteString(String value) {
        return "'" + value.replace("'", "''") + "'";
    }

    public static void validateAllowedFields(Set<String> fields) {
        for (String field : fields) {
            validateIdentifier(field);
        }
    }

    public static String quoteSfid(String sfid) {
        if (sfid == null || !SFID.matcher(sfid).matches()) {
            throw new AppException("Invalid sfid: " + sfid);
        }
        return quoteString(sfid);
    }

    public static String quoteIdentifier(String fieldName) {
        validateIdentifier(fieldName);
        return DatabaseVendorStrategies.defaultStrategy().quoteIdentifier(fieldName);
    }
}
