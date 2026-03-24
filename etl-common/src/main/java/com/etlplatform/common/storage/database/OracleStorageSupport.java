package com.etlplatform.common.storage.database;

import java.util.Locale;

public final class OracleStorageSupport {

    private OracleStorageSupport() {
    }

    public static String normalizeSchemaName(String schemaName, String username, String serviceName) {
        return normalizeSchemaName(schemaName, username, serviceName, null);
    }

    public static String normalizeSchemaName(String schemaName, String username, String serviceName, String sid) {
        String normalizedSchema = normalizeBlankToNull(schemaName);
        String normalizedUsername = normalizeBlankToNull(username);
        String normalizedService = normalizeBlankToNull(serviceName);
        String normalizedSid = normalizeBlankToNull(sid);

        String resolved = normalizedSchema;
        if (resolved == null) {
            resolved = normalizedUsername;
        } else if (normalizedUsername != null
                && ((normalizedService != null && normalizedService.equalsIgnoreCase(resolved))
                || (normalizedSid != null && normalizedSid.equalsIgnoreCase(resolved)))
                && !normalizedUsername.equalsIgnoreCase(resolved)) {
            resolved = normalizedUsername;
        }

        return resolved == null ? null : resolved.toUpperCase(Locale.ROOT);
    }

    public static String resolveJdbcServiceName(String currentServiceName, String sid, String schemaName) {
        if (normalizeBlankToNull(currentServiceName) != null || normalizeBlankToNull(sid) != null) {
            return normalizeBlankToNull(currentServiceName);
        }
        return normalizeBlankToNull(schemaName);
    }

    public static boolean hasSchemaContext(String schemaName, String username) {
        return normalizeBlankToNull(schemaName) != null || normalizeBlankToNull(username) != null;
    }

    public static String normalizeBlankToNull(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }
}
