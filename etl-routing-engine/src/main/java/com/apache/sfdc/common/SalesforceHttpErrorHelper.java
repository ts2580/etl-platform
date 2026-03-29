package com.apache.sfdc.common;

import com.etlplatform.common.error.AppException;
import org.slf4j.Logger;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.StringJoiner;

public final class SalesforceHttpErrorHelper {

    private static final int MAX_BODY_LENGTH = 1000;

    private SalesforceHttpErrorHelper() {
    }

    public static String truncateBody(String value) {
        if (value == null || value.isBlank()) {
            return "-";
        }
        String normalized = value.replaceAll("\\s+", " ").trim();
        return normalized.length() > MAX_BODY_LENGTH
                ? normalized.substring(0, MAX_BODY_LENGTH) + "..."
                : normalized;
    }

    public static Map<String, Object> context(String protocol,
                                              String selectedObject,
                                              String orgKey,
                                              String instanceUrl,
                                              Long targetStorageId) {
        Map<String, Object> context = new LinkedHashMap<>();
        putIfHasText(context, "protocol", protocol);
        putIfHasText(context, "selectedObject", selectedObject);
        putIfHasText(context, "orgKey", orgKey);
        putIfHasText(context, "instanceUrl", sanitizeInstanceUrl(instanceUrl));
        if (targetStorageId != null) {
            context.put("targetStorageId", targetStorageId);
        }
        return context;
    }

    public static Map<String, Object> with(Map<String, Object> context, String key, Object value) {
        Map<String, Object> enriched = new LinkedHashMap<>();
        if (context != null) {
            enriched.putAll(context);
        }
        if (value instanceof String text) {
            putIfHasText(enriched, key, text);
        } else if (value != null) {
            enriched.put(key, value);
        }
        return enriched;
    }

    public static void logHttpFailure(Logger log,
                                      String action,
                                      Map<String, Object> context,
                                      int status,
                                      String bodySnippet) {
        log.error("Salesforce HTTP failure. action={}, context={}, httpStatus={}, bodySnippet={}",
                action,
                formatContext(context),
                status,
                truncateBody(bodySnippet));
    }

    public static AppException httpFailure(String action,
                                           Map<String, Object> context,
                                           int status,
                                           String bodySnippet) {
        return new AppException(action
                + ": context={" + formatContext(context) + "}, httpStatus=" + status
                + ", bodySnippet=" + truncateBody(bodySnippet));
    }

    public static AppException failure(String action,
                                       Map<String, Object> context,
                                       Throwable cause) {
        return new AppException(action + ": context={" + formatContext(context) + "}", cause);
    }

    public static String formatContext(Map<String, Object> context) {
        if (context == null || context.isEmpty()) {
            return "-";
        }
        StringJoiner joiner = new StringJoiner(", ");
        context.forEach((key, value) -> {
            if (value == null) {
                return;
            }
            String rendered = value instanceof String text ? text.trim() : String.valueOf(value);
            if (!rendered.isBlank()) {
                joiner.add(key + "=" + rendered);
            }
        });
        String result = joiner.toString();
        return result.isBlank() ? "-" : result;
    }

    private static void putIfHasText(Map<String, Object> context, String key, String value) {
        if (value != null && !value.isBlank()) {
            context.put(key, value.trim());
        }
    }

    private static String sanitizeInstanceUrl(String instanceUrl) {
        if (instanceUrl == null || instanceUrl.isBlank()) {
            return null;
        }
        String trimmed = instanceUrl.trim();
        int queryIndex = trimmed.indexOf('?');
        return queryIndex >= 0 ? trimmed.substring(0, queryIndex) : trimmed;
    }
}
