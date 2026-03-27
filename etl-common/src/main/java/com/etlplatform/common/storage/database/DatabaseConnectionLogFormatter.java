package com.etlplatform.common.storage.database;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.TreeSet;

public final class DatabaseConnectionLogFormatter {

    private DatabaseConnectionLogFormatter() {
    }

    public static String requestSummary(DatabaseStorageRegistrationRequest request) {
        if (request == null) {
            return "request=null";
        }
        List<String> parts = new ArrayList<>();
        add(parts, "vendor", request.getVendor());
        add(parts, "authMethod", request.getAuthMethod());
        add(parts, "user", blankToNull(request.getUsername()));
        add(parts, "rawUrl", blankToNull(request.getJdbcUrl()));
        add(parts, "port", request.getPort());

        if (request.getVendor() == DatabaseVendor.POSTGRESQL) {
            add(parts, "schemaName", blankToNull(request.getSchemaName()));
            add(parts, "databaseName", blankToNull(request.getDatabaseName()));
        } else if (request.getVendor() == DatabaseVendor.ORACLE) {
            add(parts, "serviceNameOrSid", blankToNull(request.getSchemaName()));
        }

        return String.join(", ", parts);
    }

    public static String connectionSummary(DatabaseVendor vendor,
                                           DatabaseAuthMethod authMethod,
                                           String rawUrl,
                                           Integer port,
                                           String username,
                                           String jdbcUrl,
                                           DatabaseJdbcMetadata metadata,
                                           Properties properties) {
        List<String> parts = new ArrayList<>();
        add(parts, "vendor", vendor);
        add(parts, "authMethod", authMethod);
        add(parts, "rawUrl", blankToNull(rawUrl));
        add(parts, "port", port);
        add(parts, "username", blankToNull(username));
        add(parts, "jdbcUrl", jdbcUrl);
        add(parts, "metadata", metadataSummary(vendor, metadata));
        add(parts, "properties", summarizeProperties(properties));
        return String.join(", ", parts);
    }

    public static String outcomeSummary(DatabaseVendor vendor,
                                        DatabaseAuthMethod authMethod,
                                        String jdbcUrl,
                                        DatabaseJdbcMetadata metadata) {
        List<String> parts = new ArrayList<>();
        add(parts, "vendor", vendor);
        add(parts, "authMethod", authMethod);
        add(parts, "jdbcUrl", jdbcUrl);
        add(parts, "metadata", metadataSummary(vendor, metadata));
        return String.join(", ", parts);
    }

    public static String metadataSummary(DatabaseVendor vendor, DatabaseJdbcMetadata metadata) {
        if (metadata == null) {
            return "null";
        }

        List<String> parts = new ArrayList<>();
        add(parts, "host", metadata.host());
        add(parts, "port", metadata.port());

        if (vendor == DatabaseVendor.POSTGRESQL) {
            add(parts, "databaseName", blankToNull(metadata.databaseName()));
        } else if (vendor == DatabaseVendor.ORACLE) {
            add(parts, "serviceName", blankToNull(metadata.serviceName()));
            add(parts, "sid", blankToNull(metadata.sid()));
        }

        return "{" + String.join(", ", parts) + "}";
    }

    public static String summarizeProperties(Properties props) {
        if (props == null || props.isEmpty()) {
            return "{}";
        }

        List<String> entries = new ArrayList<>();
        for (String key : new TreeSet<>(props.stringPropertyNames())) {
            entries.add(key + "=" + maskPropertyValue(key, props.getProperty(key)));
        }
        return "{" + String.join(", ", entries) + "}";
    }

    private static String maskPropertyValue(String key, String value) {
        if (value == null) {
            return "null";
        }
        String normalizedKey = key == null ? "" : key.toLowerCase();
        if (normalizedKey.contains("password") || normalizedKey.contains("secret") || normalizedKey.contains("token")) {
            return "****";
        }
        return value;
    }

    private static String blankToNull(String value) {
        return value == null || value.isBlank() ? null : value;
    }

    private static void add(List<String> parts, String key, Object value) {
        if (value == null) {
            return;
        }
        String text = Objects.toString(value, null);
        if (text == null || text.isBlank()) {
            return;
        }
        parts.add(key + "=" + text);
    }
}
