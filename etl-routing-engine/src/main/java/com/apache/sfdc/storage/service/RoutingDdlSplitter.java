package com.apache.sfdc.storage.service;

import com.etlplatform.common.storage.database.DatabaseVendor;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static java.util.Objects.requireNonNull;

/**
 * SPLIT utilities for DDL statements by vendor rules.
 */
public final class RoutingDdlSplitter {

    private RoutingDdlSplitter() {
        // no-op
    }

    public static List<String> split(String ddl, DatabaseVendor vendor) {
        if (ddl == null || ddl.isBlank()) {
            return List.of();
        }

        String trimmed = ddl.trim();
        if (vendor == DatabaseVendor.ORACLE) {
            return splitOracle(trimmed);
        }

        return splitBySemicolonRespectingStrings(trimmed);
    }

    private static List<String> splitOracle(String ddl) {
        if (!isOracleAnonymousBlock(ddl)) {
            return splitBySemicolonRespectingStrings(ddl);
        }

        int blockEnd = findOracleAnonymousBlockEnd(ddl);
        if (blockEnd < 0) {
            return List.of(trimOracleTerminator(ddl));
        }

        List<String> statements = new ArrayList<>();
        String block = trimOracleTerminator(ddl.substring(0, blockEnd));
        if (!block.isBlank()) {
            statements.add(block);
        }

        String tail = trimLeadingOracleTerminator(ddl.substring(blockEnd));
        if (!tail.isBlank()) {
            statements.addAll(splitBySemicolonRespectingStrings(tail));
        }
        return statements;
    }

    private static List<String> splitBySemicolonRespectingStrings(String ddl) {
        List<String> statements = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inString = false;
        for (int i = 0; i < ddl.length(); i++) {
            char ch = ddl.charAt(i);
            if (ch == '\'') {
                inString = !inString;
            }
            if (ch == ';' && !inString) {
                String statement = current.toString().trim();
                if (!statement.isBlank()) {
                    statements.add(statement);
                }
                current.setLength(0);
                continue;
            }
            current.append(ch);
        }
        String tail = current.toString().trim();
        if (!tail.isBlank()) {
            statements.add(tail);
        }
        return statements;
    }

    private static boolean isOracleAnonymousBlock(String ddl) {
        String upper = requireNonNull(ddl).stripLeading().toUpperCase(Locale.ROOT);
        return upper.startsWith("BEGIN") || upper.startsWith("DECLARE");
    }

    private static int findOracleAnonymousBlockEnd(String ddl) {
        boolean inString = false;
        for (int i = 0; i < ddl.length(); i++) {
            char ch = ddl.charAt(i);
            if (ch == '\'') {
                if (inString && i + 1 < ddl.length() && ddl.charAt(i + 1) == '\'') {
                    i++;
                    continue;
                }
                inString = !inString;
                continue;
            }
            if (!inString && i + 4 <= ddl.length() && ddl.regionMatches(true, i, "END;", 0, 4)) {
                return i + 4;
            }
        }
        return -1;
    }

    private static String trimOracleTerminator(String ddl) {
        String trimmed = ddl.trim();
        if (trimmed.endsWith("/")) {
            return trimmed.substring(0, trimmed.length() - 1).trim();
        }
        return trimmed;
    }

    private static String trimLeadingOracleTerminator(String ddl) {
        String trimmed = ddl == null ? "" : ddl.trim();
        if (trimmed.startsWith("/")) {
            return trimmed.substring(1).trim();
        }
        return trimmed;
    }
}
