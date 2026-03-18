package com.apache.sfdc.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

@Slf4j
public class SalesforceCdcPayloadMapper {

    private final ObjectMapper objectMapper = new ObjectMapper();

    public Optional<SalesforceRecordMutation> map(Object body, Map<String, Object> mapType) {
        JsonNode payload = extractPayloadNode(body);
        if (payload == null || payload.isNull() || !payload.isObject()) {
            log.warn("[CDC] payload extraction failed or payload is not an object. bodyType={}", body == null ? "null" : body.getClass().getName());
            return Optional.empty();
        }

        JsonNode header = payload.get("ChangeEventHeader");
        if (header == null || header.isNull()) {
            log.warn("[CDC] ChangeEventHeader missing. payload={}", payload);
            return Optional.empty();
        }

        SalesforceMutationType mutationType = parseType(textOf(header, "changeType"));
        if (mutationType == null) {
            return Optional.empty();
        }

        String sfid = extractSfid(payload, header);
        if (!mutationType.isDelete() && (sfid == null || sfid.isBlank())) {
            log.warn("[CDC] sfid missing. mutationType={}, payload={}", mutationType, payload);
            return Optional.empty();
        }

        Set<String> targetFields = resolveTargetFields(payload, header, mapType);
        Set<String> nulledFields = extractFieldSet(header.get("nulledFields"));
        String incomingLastModifiedLiteral = SalesforceObjectSchemaBuilder.lastModifiedLiteral(payload);
        String incomingEventLiteral = eventTimeLiteral(header, mutationType.isDelete() ? "CURRENT_TIMESTAMP" : incomingLastModifiedLiteral);

        return Optional.of(new SalesforceRecordMutation(
                mutationType,
                sfid,
                (ObjectNode) payload,
                targetFields,
                nulledFields,
                incomingLastModifiedLiteral,
                incomingEventLiteral
        ));
    }

    private JsonNode extractPayloadNode(Object body) {
        if (body == null) {
            return null;
        }

        Object payloadObject = body;
        if (body instanceof Map<?, ?> map && map.containsKey("payload")) {
            payloadObject = map.get("payload");
        } else if (body instanceof GenericRecord genericRecord) {
            Schema.Field payloadField = genericRecord.getSchema().getField("payload");
            if (payloadField != null) {
                Object candidate = genericRecord.get("payload");
                if (candidate != null) {
                    payloadObject = candidate;
                }
            }
        }

        return toJsonNode(payloadObject);
    }

    private JsonNode toJsonNode(Object value) {
        if (value == null) {
            return JsonNodeFactory.instance.nullNode();
        }
        if (value instanceof JsonNode jsonNode) {
            return jsonNode;
        }
        if (value instanceof GenericRecord genericRecord) {
            ObjectNode objectNode = JsonNodeFactory.instance.objectNode();
            for (Schema.Field field : genericRecord.getSchema().getFields()) {
                objectNode.set(field.name(), toJsonNode(genericRecord.get(field.name())));
            }
            return objectNode;
        }
        if (value instanceof Map<?, ?> map) {
            ObjectNode objectNode = JsonNodeFactory.instance.objectNode();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                String key = entry.getKey() == null ? null : String.valueOf(entry.getKey());
                if (key == null || key.isBlank() || "schema".equals(key)) {
                    continue;
                }
                objectNode.set(key, toJsonNode(entry.getValue()));
            }
            return objectNode;
        }
        if (value instanceof Collection<?> collection) {
            ArrayNode arrayNode = JsonNodeFactory.instance.arrayNode();
            for (Object item : collection) {
                arrayNode.add(toJsonNode(item));
            }
            return arrayNode;
        }
        if (value.getClass().isArray() && value instanceof Object[] array) {
            ArrayNode arrayNode = JsonNodeFactory.instance.arrayNode();
            for (Object item : array) {
                arrayNode.add(toJsonNode(item));
            }
            return arrayNode;
        }
        if (value instanceof GenericData.EnumSymbol || value instanceof Enum<?>) {
            return JsonNodeFactory.instance.textNode(String.valueOf(value));
        }
        if (value instanceof CharSequence) {
            return JsonNodeFactory.instance.textNode(value.toString());
        }
        if (value instanceof Integer integer) {
            return JsonNodeFactory.instance.numberNode(integer);
        }
        if (value instanceof Long longValue) {
            return JsonNodeFactory.instance.numberNode(longValue);
        }
        if (value instanceof Double doubleValue) {
            return JsonNodeFactory.instance.numberNode(doubleValue);
        }
        if (value instanceof Float floatValue) {
            return JsonNodeFactory.instance.numberNode(floatValue);
        }
        if (value instanceof Boolean boolValue) {
            return JsonNodeFactory.instance.booleanNode(boolValue);
        }
        if (value instanceof Number number) {
            return objectMapper.valueToTree(number);
        }
        return objectMapper.valueToTree(value);
    }

    private SalesforceMutationType parseType(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        try {
            return SalesforceMutationType.valueOf(raw.toUpperCase());
        } catch (IllegalArgumentException e) {
            log.warn("[CDC] unsupported changeType={}", raw);
            return null;
        }
    }

    private String textOf(JsonNode node, String field) {
        return (node != null && node.has(field) && !node.get(field).isNull()) ? node.get(field).asText() : "";
    }

    private String extractSfid(JsonNode payload, JsonNode header) {
        JsonNode recordIdsNode = header.get("recordIds");
        if (recordIdsNode != null && recordIdsNode.isArray() && !recordIdsNode.isEmpty()) {
            return recordIdsNode.get(0).asText();
        }
        if (payload.has("Id") && !payload.get("Id").isNull()) {
            return payload.get("Id").asText();
        }
        return null;
    }

    private Set<String> resolveTargetFields(JsonNode payload, JsonNode header, Map<String, Object> mapType) {
        Set<String> candidateFields = new LinkedHashSet<>();
        addFieldArray(header.get("changedFields"), candidateFields);
        addFieldArray(header.get("nulledFields"), candidateFields);

        if (candidateFields.isEmpty()) {
            payload.fieldNames().forEachRemaining(fieldName -> {
                if (isDataField(fieldName)) {
                    candidateFields.add(fieldName);
                }
            });
        }

        Set<String> targetFields = new LinkedHashSet<>(candidateFields);
        targetFields.removeIf(field -> mapType == null || !mapType.containsKey(field));

        log.warn("[CDC-FIELDS] mapTypeSize={}, mapTypeKeys={}, candidateFields={}, filteredTargetFields={}",
                mapType == null ? 0 : mapType.size(),
                summarizeKeys(mapType),
                candidateFields,
                targetFields);
        return targetFields;
    }

    private Set<String> extractFieldSet(JsonNode arrayNode) {
        Set<String> output = new HashSet<>();
        addFieldArray(arrayNode, output);
        return output;
    }

    private void addFieldArray(JsonNode arrayNode, Set<String> output) {
        if (arrayNode == null || !arrayNode.isArray()) {
            return;
        }
        for (JsonNode item : arrayNode) {
            String raw = item.asText();
            if (raw == null || raw.isBlank() || raw.contains(".")) {
                continue;
            }
            if (isDataField(raw)) {
                output.add(raw);
            }
        }
    }

    private boolean isDataField(String fieldName) {
        return fieldName != null
                && !fieldName.isBlank()
                && !"ChangeEventHeader".equals(fieldName)
                && !"Id".equals(fieldName)
                && !"schema".equals(fieldName);
    }

    private Set<String> payloadFieldNames(JsonNode payload) {
        Set<String> names = new LinkedHashSet<>();
        if (payload != null && payload.isObject()) {
            payload.fieldNames().forEachRemaining(names::add);
        }
        return names;
    }

    private Object summarizeKeys(Map<String, Object> mapType) {
        if (mapType == null || mapType.isEmpty()) {
            return java.util.List.of();
        }
        return mapType.keySet().stream().sorted().limit(80).toList();
    }

    private String eventTimeLiteral(JsonNode header, String fallbackLiteral) {
        String commitTimestamp = textOf(header, "commitTimestamp");
        if (commitTimestamp == null || commitTimestamp.isBlank()) {
            return fallbackLiteral;
        }
        if (commitTimestamp.matches("^\\d+$")) {
            return "FROM_UNIXTIME(" + commitTimestamp + " / 1000)";
        }
        return SqlSanitizer.quoteString(commitTimestamp.replace("T", " ").replace("Z", ""));
    }
}
