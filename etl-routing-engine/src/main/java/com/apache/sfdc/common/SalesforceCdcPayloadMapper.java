package com.apache.sfdc.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class SalesforceCdcPayloadMapper {

    private static final Logger log = LoggerFactory.getLogger(SalesforceCdcPayloadMapper.class);

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
        Object incomingLastModifiedValue = SalesforceObjectSchemaBuilder.lastModifiedValue(payload);
        String incomingLastModifiedLiteral = SalesforceObjectSchemaBuilder.lastModifiedLiteral(payload);
        Object incomingEventValue = eventTimeValue(header, mutationType.isDelete() ? LocalDateTime.now(ZoneOffset.UTC) : incomingLastModifiedValue);
        String incomingEventLiteral = eventTimeLiteral(header, mutationType.isDelete() ? "CURRENT_TIMESTAMP" : incomingLastModifiedLiteral);

        return Optional.of(new SalesforceRecordMutation(
                mutationType,
                sfid,
                (ObjectNode) payload,
                targetFields,
                nulledFields,
                incomingLastModifiedValue,
                incomingEventValue,
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
        augmentCompoundNameFields(payload, mapType, candidateFields);
        augmentCompoundAddressFields(payload, mapType, candidateFields);

        Set<String> targetFields = mapToSupportedFields(candidateFields, mapType);

        // Quick hotfix: CDC changedFields can be bitmap/hex (ex: 0x06000100) in some payload formats,
        // so if header-derived names are unmapped, fallback to payload keys that are actually present.
        if (targetFields.isEmpty() && hasBitmapLikeCandidate(candidateFields)) {
            targetFields = mapToSupportedFields(payloadFieldNames(payload), mapType);
        }

        if (candidateFields.isEmpty()) {
            targetFields = mapToSupportedFields(payloadFieldNames(payload), mapType);
        }

        if (targetFields.isEmpty()) {
            payload.fieldNames().forEachRemaining(fieldName -> {
                if (isDataField(fieldName)) {
                    candidateFields.add(fieldName);
                }
            });
            targetFields = mapToSupportedFields(candidateFields, mapType);
        }

        return targetFields;
    }

    private Set<String> mapToSupportedFields(Set<String> sourceFields, Map<String, Object> mapType) {
        Set<String> output = new LinkedHashSet<>(sourceFields);
        output.removeIf(field -> mapType == null || !mapType.containsKey(field));
        return output;
    }

    private boolean hasBitmapLikeCandidate(Set<String> candidateFields) {
        if (candidateFields == null || candidateFields.isEmpty()) {
            return false;
        }
        return candidateFields.stream().allMatch(field -> isBitmapLikeField(field));
    }

    private boolean isBitmapLikeField(String field) {
        return field != null && field.matches("(?i)^0x[0-9a-f]+$");
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

    private void augmentCompoundNameFields(JsonNode payload, Map<String, Object> mapType, Set<String> candidateFields) {
        if (payload == null || !payload.isObject() || candidateFields == null) {
            return;
        }
        JsonNode nameNode = payload.get("Name");
        if (nameNode == null || !nameNode.isObject()) {
            return;
        }
        if (mapType != null && mapType.containsKey("Name")) {
            candidateFields.add("Name");
        }
        if (mapType != null && mapType.containsKey("FirstName")) {
            candidateFields.add("FirstName");
        }
        if (mapType != null && mapType.containsKey("LastName")) {
            candidateFields.add("LastName");
        }
    }

    private void augmentCompoundAddressFields(JsonNode payload, Map<String, Object> mapType, Set<String> candidateFields) {
        if (payload == null || !payload.isObject() || candidateFields == null || mapType == null || mapType.isEmpty()) {
            return;
        }
        String[][] mappings = {
                {"Other", "OtherAddress"},
                {"Mailing", "MailingAddress"},
                {"Billing", "BillingAddress"},
                {"Shipping", "ShippingAddress"}
        };
        String[] suffixes = {"Street", "City", "State", "PostalCode", "Country", "Latitude", "Longitude", "GeocodeAccuracy"};
        for (String[] mapping : mappings) {
            String prefix = mapping[0];
            String parentName = mapping[1];
            JsonNode addressNode = payload.get(parentName);
            if (addressNode == null || !addressNode.isObject()) {
                continue;
            }
            if (mapType.containsKey(parentName)) {
                candidateFields.add(parentName);
            }
            for (String suffix : suffixes) {
                String fieldName = prefix + suffix;
                if (mapType.containsKey(fieldName)) {
                    candidateFields.add(fieldName);
                }
            }
        }
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
        return SqlSanitizer.sanitizeValue(com.fasterxml.jackson.databind.node.JsonNodeFactory.instance.textNode(commitTimestamp), "datetime");
    }

    private Object eventTimeValue(JsonNode header, Object fallbackValue) {
        String commitTimestamp = textOf(header, "commitTimestamp");
        if (commitTimestamp == null || commitTimestamp.isBlank()) {
            return fallbackValue;
        }
        if (commitTimestamp.matches("^\\d+$")) {
            return Long.parseLong(commitTimestamp);
        }
        return commitTimestamp;
    }
}
