package com.etlplatform.common.error;

public record ApiErrorResponse(String code, String error, String message) {
}
