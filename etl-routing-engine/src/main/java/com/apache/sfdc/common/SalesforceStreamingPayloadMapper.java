package com.apache.sfdc.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class SalesforceStreamingPayloadMapper {

    private final ObjectMapper objectMapper = new ObjectMapper();

    public Optional<SalesforceRecordMutation> map(String eventType, Object body, Map<String, Object> mapType) {
        SalesforceMutationType mutationType = parseType(eventType);
        if (mutationType == null) {
            return Optional.empty();
        }

        JsonNode rootNode = objectMapper.valueToTree(body);
        if (rootNode == null || rootNode.isNull() || !rootNode.isObject()) {
            return Optional.empty();
        }

        String sfid = mutationType.isDelete()
                ? firstTextValue(rootNode)
                : textOf(rootNode, "Id");

        if (sfid == null || sfid.isBlank()) {
            return Optional.empty();
        }

        ObjectNode payload = mutationType.isDelete() ? objectMapper.createObjectNode() : ((ObjectNode) rootNode).deepCopy();
        Set<String> targetFields = new LinkedHashSet<>();
        if (!mutationType.isDelete()) {
            rootNode.fieldNames().forEachRemaining(fieldName -> {
                if (isDataField(fieldName) && mapType.containsKey(fieldName)) {
                    targetFields.add(fieldName);
                }
            });
        }

        String incomingLastModifiedLiteral = SalesforceObjectSchemaBuilder.lastModifiedLiteral(payload);
        String incomingEventLiteral = mutationType.isDelete() ? "CURRENT_TIMESTAMP" : incomingLastModifiedLiteral;

        return Optional.of(new SalesforceRecordMutation(
                mutationType,
                sfid,
                payload,
                targetFields,
                Set.of(),
                incomingLastModifiedLiteral,
                incomingEventLiteral
        ));
    }

    private SalesforceMutationType parseType(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        return switch (raw.toLowerCase()) {
            case "created" -> SalesforceMutationType.CREATE;
            case "updated" -> SalesforceMutationType.UPDATE;
            case "deleted" -> SalesforceMutationType.DELETE;
            default -> null;
        };
    }

    private String textOf(JsonNode node, String field) {
        return (node != null && node.has(field) && !node.get(field).isNull()) ? node.get(field).asText() : "";
    }

    private String firstTextValue(JsonNode node) {
        if (node == null || !node.isObject()) {
            return null;
        }
        var iterator = node.fields();
        if (!iterator.hasNext()) {
            return null;
        }
        return iterator.next().getValue().asText(null);
    }

    private boolean isDataField(String fieldName) {
        return fieldName != null && !fieldName.isBlank() && !"Id".equals(fieldName) && !"sfid".equals(fieldName);
    }
}
