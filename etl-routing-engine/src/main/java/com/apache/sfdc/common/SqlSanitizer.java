package com.apache.sfdc.common;

import com.etlplatform.common.error.AppException;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.Set;
import java.util.regex.Pattern;

public final class SqlSanitizer {

    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
    private static final Pattern SFID = Pattern.compile("[A-Za-z0-9]{18}");

    private SqlSanitizer() {}

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
        if (valueNode == null || valueNode.isNull()) {
            return "null";
        }

        return switch (sfType) {
            case "double", "percent", "currency" -> toNumeric(valueNode, sfType);
            case "int" -> toInt(valueNode);
            case "boolean" -> toBoolean(valueNode);
            case "datetime" -> toDateTimeLiteral(valueNode);
            case "date" -> toDateLiteral(valueNode);
            case "time" -> quoteAndNormalizeTime(valueNode.asText());
            default -> quoteString(valueNode.asText());
        };
    }

    private static String toNumeric(JsonNode valueNode, String sfType) {
        String raw = valueNode.asText();
        try {
            Double.parseDouble(raw);
            return raw;
        } catch (NumberFormatException ex) {
            throw new AppException("Invalid numeric value for type " + sfType + ": " + raw);
        }
    }

    private static String toInt(JsonNode valueNode) {
        String raw = valueNode.asText();
        try {
            Integer.parseInt(raw);
            return raw;
        } catch (NumberFormatException ex) {
            throw new AppException("Invalid integer value: " + raw);
        }
    }

    private static String toBoolean(JsonNode valueNode) {
        return valueNode.asBoolean() ? "true" : "false";
    }

    private static String toDateTimeLiteral(JsonNode valueNode) {
        String raw = valueNode.asText();
        if (raw.matches("^\\d{13}$")) {
            return "FROM_UNIXTIME(" + raw + " / 1000)";
        }
        if (raw.matches("^\\d{10}$")) {
            return "FROM_UNIXTIME(" + raw + ")";
        }
        return quoteAndNormalizeDateTime(raw);
    }

    private static String toDateLiteral(JsonNode valueNode) {
        String raw = valueNode.asText();
        if (raw.matches("^\\d{13}$")) {
            return "DATE(FROM_UNIXTIME(" + raw + " / 1000))";
        }
        if (raw.matches("^\\d{10}$")) {
            return "DATE(FROM_UNIXTIME(" + raw + "))";
        }
        return quoteString(raw);
    }

    private static String quoteAndNormalizeDateTime(String value) {
        String normalized = value.replace(".000+0000", "")
                .replace("T", " ")
                .replace("Z", "");
        return quoteString(normalized);
    }

    private static String quoteAndNormalizeTime(String value) {
        return quoteString(value.replace("Z", ""));
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
        return "'" + sfid + "'";
    }

    public static String quoteIdentifier(String fieldName) {
        validateIdentifier(fieldName);
        return "`" + fieldName + "`";
    }
}
