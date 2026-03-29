package com.apache.sfdc.common;

import org.apache.camel.CamelContext;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Runtime state shared by Salesforce routing engines (CDC and Streaming).
 */
public record SalesforceRouteRuntimeState(
        Map<String, String> mapProperty,
        Map<String, Object> mapType,
        CamelContext camelContext,
        long credentialVersion,
        AtomicLong lastEventAt,
        AtomicLong lastRestartAttemptAt,
        AtomicBoolean restartInProgress
) {
}
