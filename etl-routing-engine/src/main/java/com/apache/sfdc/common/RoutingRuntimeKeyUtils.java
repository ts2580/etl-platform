package com.apache.sfdc.common;

import java.net.URI;
import java.util.Locale;

public final class RoutingRuntimeKeyUtils {

    private RoutingRuntimeKeyUtils() {
    }

    public static String normalizeOrgKey(String raw) {
        if (raw == null) {
            return "";
        }
        String value = raw.trim();
        if (value.isBlank()) {
            return "";
        }

        if (value.startsWith("<") && value.endsWith(">") && value.contains("|")) {
            value = value.substring(1, value.length() - 1);
            value = value.substring(0, value.indexOf('|'));
        }

        try {
            if (value.startsWith("http://") || value.startsWith("https://")) {
                URI uri = URI.create(value);
                if (uri.getHost() != null && !uri.getHost().isBlank()) {
                    value = uri.getHost();
                }
            }
        } catch (Exception ignore) {
            // fall through
        }

        value = value.replaceAll("^https?://", "")
                .replaceAll("/+$", "")
                .trim();
        int slashIndex = value.indexOf('/');
        if (slashIndex >= 0) {
            value = value.substring(0, slashIndex);
        }
        return value.toLowerCase(Locale.ROOT);
    }

    public static String buildRouteKey(String orgKey, String selectedObject, String protocol) {
        String normalizedOrgKey = normalizeOrgKey(orgKey);
        String normalizedObject = selectedObject == null ? "" : selectedObject.trim();
        String normalizedProtocol = protocol == null ? "" : protocol.trim().toUpperCase(Locale.ROOT);
        return normalizedOrgKey + "::" + normalizedProtocol + "::" + normalizedObject;
    }
}
