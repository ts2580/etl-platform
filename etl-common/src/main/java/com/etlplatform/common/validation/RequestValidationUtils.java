package com.etlplatform.common.validation;

import com.etlplatform.common.error.AppException;

import java.util.regex.Pattern;

public final class RequestValidationUtils {
    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
    private static final Pattern SIMPLE_KEY = Pattern.compile("[A-Za-z0-9._:-]{1,120}");

    private RequestValidationUtils() {
    }

    public static String requireText(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new AppException(fieldName + " 값은 필수입니다.");
        }
        return value.trim();
    }

    public static String requireIdentifier(String value, String fieldName) {
        String trimmed = requireText(value, fieldName);
        if (!IDENTIFIER.matcher(trimmed).matches()) {
            throw new AppException(fieldName + " 값이 유효하지 않습니다: " + value);
        }
        return trimmed;
    }

    public static String requireSimpleKey(String value, String fieldName) {
        String trimmed = requireText(value, fieldName);
        if (!SIMPLE_KEY.matcher(trimmed).matches()) {
            throw new AppException(fieldName + " 값이 유효하지 않습니다: " + value);
        }
        return trimmed;
    }

    public static int requirePositive(int value, String fieldName) {
        if (value <= 0) {
            throw new AppException(fieldName + " 값은 1 이상이어야 합니다.");
        }
        return value;
    }
}
