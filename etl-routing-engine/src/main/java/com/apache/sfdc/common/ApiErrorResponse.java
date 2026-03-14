package com.apache.sfdc.common;

public record ApiErrorResponse(String code, String message, String detail) {}