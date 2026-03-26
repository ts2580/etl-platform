package com.apache.sfdc.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

@Slf4j
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
            normalizeFiscalFields(payload, mapType, targetFields);
            rootNode.fieldNames().forEachRemaining(fieldName -> {
                if (isDataField(fieldName) && mapType.containsKey(fieldName)) {
                    targetFields.add(fieldName);
                }
            });
        }

        Object incomingLastModifiedValue = SalesforceObjectSchemaBuilder.lastModifiedValue(payload);
        String incomingLastModifiedLiteral = SalesforceObjectSchemaBuilder.lastModifiedLiteral(payload);
        Object incomingEventValue = mutationType.isDelete() ? LocalDateTime.now(ZoneOffset.UTC) : incomingLastModifiedValue;
        String incomingEventLiteral = mutationType.isDelete() ? "CURRENT_TIMESTAMP" : incomingLastModifiedLiteral;

        SalesforceRecordMutation mutation = new SalesforceRecordMutation(
                mutationType,
                sfid,
                payload,
                targetFields,
                Set.of(),
                incomingLastModifiedValue,
                incomingEventValue,
                incomingLastModifiedLiteral,
                incomingEventLiteral
        );
        log.debug("[PUSHTOPIC-MUTATION] eventType={}, sfid={}, targetFields={}, payloadSummary={}",
                eventType,
                sfid,
                targetFields,
                summarizePayload(payload, targetFields, mapType));
        String fiscalSummary = summarizeFiscalPayload(payload, mapType);
        if (!"{}".equals(fiscalSummary)) {
            log.info("[PUSHTOPIC-FISCAL] eventType={}, sfid={}, fiscalPayload={}", eventType, sfid, fiscalSummary);
        }
        return Optional.of(mutation);
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

    private void normalizeFiscalFields(ObjectNode payload, Map<String, Object> mapType, Set<String> targetFields) {
        if (payload == null || mapType == null || mapType.isEmpty()) {
            return;
        }
        JsonNode fiscalNode = payload.get("Fiscal");
        if (fiscalNode == null || fiscalNode.isNull() || !fiscalNode.isObject()) {
            return;
        }

        JsonNode yearNode = fiscalNode.get("Year");
        JsonNode quarterNode = fiscalNode.get("Quarter");
        String year = yearNode == null || yearNode.isNull() ? null : yearNode.asText(null);
        String quarter = quarterNode == null || quarterNode.isNull() ? null : quarterNode.asText(null);
        if (year == null || year.isBlank() || quarter == null || quarter.isBlank()) {
            payload.remove("Fiscal");
            targetFields.remove("Fiscal");
            targetFields.remove("FiscalYear");
            targetFields.remove("FiscalQuarter");
            return;
        }

        String normalizedFiscal = year.trim() + " " + quarter.trim();

        if (mapType.containsKey("Fiscal")) {
            payload.put("Fiscal", normalizedFiscal);
            targetFields.add("Fiscal");
        }
        if (mapType.containsKey("FiscalYear")) {
            payload.put("FiscalYear", Integer.parseInt(year.trim()));
            targetFields.add("FiscalYear");
        }
        if (mapType.containsKey("FiscalQuarter")) {
            payload.put("FiscalQuarter", Integer.parseInt(quarter.trim()));
            targetFields.add("FiscalQuarter");
        }
    }

    private String summarizePayload(ObjectNode payload, Set<String> targetFields, Map<String, Object> mapType) {
        if (payload == null || payload.isEmpty() || targetFields == null || targetFields.isEmpty()) {
            return "{}";
        }
        StringBuilder summary = new StringBuilder("{");
        int count = 0;
        for (String fieldName : targetFields) {
            if (count >= 12) {
                summary.append("...");
                break;
            }
            JsonNode valueNode = payload.get(fieldName);
            String valueText = valueNode == null || valueNode.isNull() ? "null" : valueNode.asText();
            if (valueText != null && valueText.length() > 60) {
                valueText = valueText.substring(0, 60) + "...";
            }
            if (count > 0) {
                summary.append(", ");
            }
            summary.append(fieldName)
                    .append("=")
                    .append(valueText)
                    .append("(type=")
                    .append(mapType == null ? null : mapType.get(fieldName))
                    .append(",len=")
                    .append(valueText == null ? 0 : valueText.length())
                    .append(")");
            count++;
        }
        summary.append("}");
        return summary.toString();
    }

    private String summarizeFiscalPayload(ObjectNode payload, Map<String, Object> mapType) {
        if (payload == null || payload.isEmpty()) {
            return "{}";
        }
        StringBuilder summary = new StringBuilder("{");
        int count = 0;
        var fields = payload.fieldNames();
        while (fields.hasNext()) {
            String fieldName = fields.next();
            String lower = fieldName == null ? "" : fieldName.toLowerCase();
            if (!(lower.contains("fiscal") || lower.contains("quarter") || lower.contains("year"))) {
                continue;
            }
            JsonNode valueNode = payload.get(fieldName);
            String valueText = valueNode == null || valueNode.isNull() ? "null" : valueNode.asText();
            if (count > 0) {
                summary.append(", ");
            }
            summary.append(fieldName)
                    .append("=")
                    .append(valueText)
                    .append("(type=")
                    .append(mapType == null ? null : mapType.get(fieldName))
                    .append(",len=")
                    .append(valueText == null ? 0 : valueText.length())
                    .append(")");
            count++;
        }
        summary.append("}");
        return count == 0 ? "{}" : summary.toString();
    }

    private boolean isDataField(String fieldName) {
        return fieldName != null && !fieldName.isBlank() && !"Id".equals(fieldName) && !"sfid".equals(fieldName);
    }
}
