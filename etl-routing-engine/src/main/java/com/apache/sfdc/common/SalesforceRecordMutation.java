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
    public String summarizeForLog(java.util.Map<String, Object> mapType) {
        if (payload == null || payload.isEmpty() || targetFields == null || targetFields.isEmpty()) {
            return "{}";
        }
        java.util.List<String> orderedFields = new java.util.ArrayList<>(targetFields);
        orderedFields.sort((left, right) -> {
            int rightLen = valueLength(payload.get(right));
            int leftLen = valueLength(payload.get(left));
            return Integer.compare(rightLen, leftLen);
        });

        StringBuilder summary = new StringBuilder("{");
        for (int i = 0; i < orderedFields.size(); i++) {
            String fieldName = orderedFields.get(i);
            var valueNode = payload.get(fieldName);
            String valueText = valueNode == null || valueNode.isNull() ? "null" : valueNode.toString();
            if (i > 0) {
                summary.append(", ");
            }
            summary.append(fieldName)
                    .append("=")
                    .append(valueText)
                    .append("(type=")
                    .append(mapType == null ? null : mapType.get(fieldName))
                    .append(",len=")
                    .append(valueLength(valueNode))
                    .append(")");
        }
        summary.append("}");
        return summary.toString();
    }

    private int valueLength(com.fasterxml.jackson.databind.JsonNode valueNode) {
        if (valueNode == null || valueNode.isNull()) {
            return 0;
        }
        return valueNode.isValueNode() ? valueNode.asText().length() : valueNode.toString().length();
    }
}
