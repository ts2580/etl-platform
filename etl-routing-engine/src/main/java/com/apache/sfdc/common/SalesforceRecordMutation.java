package com.apache.sfdc.common;

import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Set;

public record SalesforceRecordMutation(
        SalesforceMutationType type,
        String sfid,
        ObjectNode payload,
        Set<String> targetFields,
        Set<String> nulledFields,
        Object incomingLastModifiedValue,
        Object incomingEventValue,
        String incomingLastModifiedLiteral,
        String incomingEventLiteral
) {
}
