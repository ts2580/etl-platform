package com.etlplatform.common.storage.database;

import com.etlplatform.common.error.AppException;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Locale;

public final class OracleRoutingNaming {

    private static final int ORACLE_IDENTIFIER_MAX_LENGTH = 30;
    private static final int HASH_LENGTH = 8;
    private static final String DEFAULT_PREFIX = "ORG";
    private static final String DEFAULT_OBJECT = "OBJECT";

    private OracleRoutingNaming() {
    }

    public static String buildTableName(String orgName, String selectedObject) {
        String normalizedSelectedObject = selectedObject == null ? "" : selectedObject.trim().toUpperCase(Locale.ROOT);
        if (normalizedSelectedObject.matches("ORG_[A-Z0-9_]+_[A-Z0-9_]+")) {
            validateOracleIdentifier(normalizedSelectedObject);
            return normalizedSelectedObject;
        }

        String normalizedOrg = stripLeadingOrgPrefix(stripLeadingSchemaPrefix(normalizePart(orgName, DEFAULT_PREFIX)));
        String normalizedObject = stripLeadingOrgPrefix(normalizePart(selectedObject, DEFAULT_OBJECT));
        String combined = DEFAULT_PREFIX + "_" + normalizedOrg + "_" + normalizedObject;
        if (combined.length() <= ORACLE_IDENTIFIER_MAX_LENGTH) {
            validateOracleIdentifier(combined);
            return combined;
        }

        String hash = shortHash(combined);
        int maxBaseLength = ORACLE_IDENTIFIER_MAX_LENGTH - HASH_LENGTH - 1;
        String truncated = combined.substring(0, Math.max(maxBaseLength, 1));
        truncated = trimUnderscores(truncated);
        if (truncated.isBlank()) {
            truncated = DEFAULT_PREFIX;
        }
        String finalName = truncated + "_" + hash;
        if (finalName.length() > ORACLE_IDENTIFIER_MAX_LENGTH) {
            finalName = finalName.substring(0, ORACLE_IDENTIFIER_MAX_LENGTH);
            finalName = trimUnderscores(finalName);
        }
        if (finalName.isBlank()) {
            throw new AppException("Oracle table name 생성에 실패했어요.");
        }
        validateOracleIdentifier(finalName);
        return finalName;
    }

    private static String normalizePart(String value, String fallback) {
        String normalized = value == null ? "" : value.trim().toUpperCase(Locale.ROOT)
                .replaceAll("[^A-Z0-9]+", "_")
                .replaceAll("_+", "_");
        normalized = trimUnderscores(normalized);
        if (normalized.isBlank()) {
            normalized = fallback;
        }
        if (!Character.isLetter(normalized.charAt(0)) && normalized.charAt(0) != '_') {
            normalized = DEFAULT_PREFIX + "_" + normalized;
        }
        return normalized;
    }

    private static String stripLeadingSchemaPrefix(String value) {
        if (value == null || value.isBlank()) {
            return DEFAULT_PREFIX;
        }
        String normalized = value;
        while (normalized.startsWith("ORG_")) {
            normalized = normalized.substring(4);
        }
        return normalized.isBlank() ? DEFAULT_PREFIX : normalized;
    }

    private static String stripLeadingOrgPrefix(String value) {
        if (value == null || value.isBlank()) {
            return DEFAULT_PREFIX;
        }
        String normalized = value;
        while (normalized.startsWith(DEFAULT_PREFIX + "_")) {
            normalized = normalized.substring((DEFAULT_PREFIX + "_").length());
        }
        return normalized.isBlank() ? DEFAULT_PREFIX : normalized;
    }

    private static void validateOracleIdentifier(String identifier) {
        if (identifier == null || !identifier.matches("[A-Za-z_][A-Za-z0-9_]*")) {
            throw new AppException("Invalid Oracle identifier: " + identifier);
        }
    }

    private static String trimUnderscores(String value) {
        return value == null ? "" : value.replaceAll("^_+|_+$", "");
    }

    private static String shortHash(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-1");
            byte[] hash = digest.digest(value.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash).substring(0, HASH_LENGTH).toUpperCase(Locale.ROOT);
        } catch (NoSuchAlgorithmException e) {
            throw new AppException("Oracle table hash 생성에 실패했어요.", e);
        }
    }
}
