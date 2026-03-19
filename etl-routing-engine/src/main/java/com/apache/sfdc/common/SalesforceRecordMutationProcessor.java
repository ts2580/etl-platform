package com.apache.sfdc.common;

import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SalesforceRecordMutationProcessor {

    private static final Logger log = LoggerFactory.getLogger(SalesforceRecordMutationProcessor.class);

    public MutationResult apply(String targetSchema,
                                String selectedObject,
                                Map<String, Object> mapType,
                                SalesforceRecordMutation mutation,
                                SalesforceMutationRepositoryPort repository,
                                String sourceLabel) {

        if (mutation.type().isDelete()) {
            int deleted = delete(targetSchema, selectedObject, mutation, repository, sourceLabel);
            return new MutationResult(0, 0, deleted);
        }

        StringBuilder strUpdate = new StringBuilder();
        strUpdate.append("UPDATE ").append(SalesforceObjectSchemaBuilder.qualifiedName(targetSchema, selectedObject)).append(" SET ");

        int assignmentCount = appendAssignments(strUpdate, mapType, mutation.payload(), mutation.targetFields(), mutation.nulledFields());
        if (assignmentCount > 0) {
            SalesforceObjectSchemaBuilder.appendFreshnessAssignments(
                    strUpdate,
                    mutation.incomingLastModifiedLiteral(),
                    mutation.incomingEventLiteral()
            );
            strUpdate.deleteCharAt(strUpdate.length() - 1);
            SalesforceObjectSchemaBuilder.appendFreshnessWhere(
                    strUpdate,
                    SqlSanitizer.quoteString(mutation.sfid()),
                    mutation.incomingLastModifiedLiteral()
            );

            log.warn("""
                    [CDC-UPDATE-SQL]
                      sfid: {}
                      sql:
                    {}
                    """, mutation.sfid(), formatIndentedBlock(strUpdate.toString()));
            int updated = repository.updateObject(strUpdate);
            if (updated == 0 && mutation.type().isCreateLike()) {
                int inserted = insertFallback(targetSchema, selectedObject, mapType, mutation, repository, sourceLabel);
                return new MutationResult(updated, inserted, 0);
            }
            return new MutationResult(updated, 0, 0);
        }

        if (mutation.type().isCreateLike()) {
            int inserted = insertMinimal(targetSchema, selectedObject, mutation.sfid(), repository);
            return new MutationResult(0, inserted, 0);
        }

        log.warn("[CDC-APPLY] no-op mutation. type={}, sfid={}, targetFields={}, payloadKeys={}",
                mutation.type(), mutation.sfid(), mutation.targetFields(), mutation.payload().properties().stream().map(java.util.Map.Entry::getKey).toList());
        return new MutationResult(0, 0, 0);
    }

    private int delete(String targetSchema,
                       String selectedObject,
                       SalesforceRecordMutation mutation,
                       SalesforceMutationRepositoryPort repository,
                       String sourceLabel) {
        StringBuilder strDelete = SalesforceObjectSchemaBuilder.buildConditionalDeleteSql(
                targetSchema,
                selectedObject,
                SqlSanitizer.quoteSfid(mutation.sfid()),
                mutation.incomingEventLiteral()
        );
        log.warn("""
                [CDC-DELETE-SQL]
                  sfid: {}
                  sql:
                {}
                """, mutation.sfid(), formatIndentedBlock(strDelete.toString()));
        return repository.deleteObject(strDelete);
    }

    private int appendAssignments(StringBuilder strUpdate,
                                  Map<String, Object> mapType,
                                  JsonNode payload,
                                  Set<String> targetFields,
                                  Set<String> nulledFields) {
        int assignmentCount = 0;
        for (String fieldName : targetFields) {
            strUpdate.append(fieldName).append(" = ");
            JsonNode rawFieldValue = resolveRawFieldValue(payload, fieldName);
            JsonNode fieldValue = normalizeFieldValue(rawFieldValue, fieldName);
            if (nulledFields.contains(fieldName) || fieldValue == null || fieldValue.isNull()) {
                strUpdate.append("null,");
            } else {
                String sanitized = sanitizeFieldValue(fieldName, fieldValue, String.valueOf(mapType.get(fieldName)));
                strUpdate.append(sanitized).append(",");
            }
            assignmentCount++;
        }
        return assignmentCount;
    }

    private String sanitizeFieldValue(String fieldName, JsonNode fieldValue, String sfType) {
        if (fieldValue == null || fieldValue.isNull()) {
            return "null";
        }
        try {
            return SqlSanitizer.sanitizeValue(fieldValue, sfType);
        } catch (Exception primary) {
            if (fieldValue.isObject() || fieldValue.isArray()) {
                return SqlSanitizer.quoteString(fieldValue.toString());
            }
            throw primary;
        }
    }

    private JsonNode resolveRawFieldValue(JsonNode payload, String fieldName) {
        if (payload == null || fieldName == null) {
            return null;
        }
        JsonNode direct = payload.get(fieldName);
        if (direct != null && !direct.isNull()) {
            return direct;
        }

        JsonNode compoundAddressValue = resolveCompoundAddressNode(payload, fieldName);
        if (compoundAddressValue != null && !compoundAddressValue.isNull()) {
            return compoundAddressValue;
        }

        JsonNode nameNode = payload.get("Name");
        if (nameNode != null && nameNode.isObject()) {
            if ("FirstName".equals(fieldName)) {
                JsonNode firstName = firstNonNull(nameNode, "FirstName", "firstName");
                if (firstName != null) {
                    return firstName;
                }
            }
            if ("LastName".equals(fieldName)) {
                JsonNode lastName = firstNonNull(nameNode, "LastName", "lastName");
                if (lastName != null) {
                    return lastName;
                }
            }
        }
        return direct;
    }

    private JsonNode normalizeFieldValue(JsonNode valueNode, String fieldName) {
        if (valueNode == null || valueNode.isNull()) {
            return valueNode;
        }
        if (!valueNode.isObject()) {
            return valueNode;
        }

        if ("Name".equals(fieldName)) {
            JsonNode display = firstNonNull(valueNode, "displayValue", "value", "stringValue");
            if (display != null && !display.isNull() && !display.asText().isBlank()) {
                return display;
            }
            JsonNode salutation = firstNonNull(valueNode, "Salutation", "salutation");
            JsonNode firstName = firstNonNull(valueNode, "FirstName", "firstName");
            JsonNode lastName = firstNonNull(valueNode, "LastName", "lastName");
            String fullName = joinNameParts(salutation, firstName, lastName);
            if (!fullName.isBlank()) {
                return com.fasterxml.jackson.databind.node.JsonNodeFactory.instance.textNode(fullName);
            }
        }

        if (isCompoundAddressField(fieldName)) {
            String formattedAddress = joinAddressParts(valueNode);
            if (!formattedAddress.isBlank()) {
                return com.fasterxml.jackson.databind.node.JsonNodeFactory.instance.textNode(formattedAddress);
            }
        }

        if (valueNode.has("newValue")) {
            return valueNode.get("newValue");
        }

        if (valueNode.has("value")) {
            return valueNode.get("value");
        }

        if (valueNode.size() == 1) {
            String singleField = valueNode.fieldNames().next();
            if (isScalarAlias(singleField)) {
                return valueNode.get(singleField);
            }
        }

        return valueNode;
    }

    private JsonNode resolveCompoundAddressNode(JsonNode payload, String fieldName) {
        if (payload == null || fieldName == null) {
            return null;
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
            for (String suffix : suffixes) {
                if ((prefix + suffix).equals(fieldName)) {
                    JsonNode parent = payload.get(parentName);
                    if (parent != null && parent.isObject()) {
                        JsonNode value = parent.get(suffix);
                        if (value != null && !value.isNull()) {
                            return value;
                        }
                    }
                }
            }
        }
        return null;
    }

    private JsonNode firstNonNull(JsonNode node, String... names) {
        if (node == null) {
            return null;
        }
        for (String name : names) {
            JsonNode candidate = node.get(name);
            if (candidate != null && !candidate.isNull()) {
                return candidate;
            }
        }
        return null;
    }

    // Compound Name payload is flattened into a display string with single spaces:
    // Salutation + LastName + FirstName (ex: "Mr Doe John", "님 유리유리 이").
    private String joinNameParts(JsonNode salutationNode, JsonNode firstName, JsonNode lastName) {
        String salutation = salutationNode == null ? "" : salutationNode.asText("").trim();
        String first = firstName == null ? "" : firstName.asText("").trim();
        String last = lastName == null ? "" : lastName.asText("").trim();

        java.util.List<String> parts = new java.util.ArrayList<>();
        if (!salutation.isBlank()) {
            parts.add(salutation);
        }
        if (!last.isBlank()) {
            parts.add(last);
        }
        if (!first.isBlank()) {
            parts.add(first);
        }
        return String.join(" ", parts).trim();
    }

    private boolean isCompoundAddressField(String fieldName) {
        return "OtherAddress".equals(fieldName)
                || "MailingAddress".equals(fieldName)
                || "BillingAddress".equals(fieldName)
                || "ShippingAddress".equals(fieldName);
    }

    // Compound Address payload is flattened into one display string in address order
    // with single spaces between non-empty parts.
    private String joinAddressParts(JsonNode addressNode) {
        if (addressNode == null || addressNode.isNull() || !addressNode.isObject()) {
            return "";
        }
        java.util.List<String> values = new java.util.ArrayList<>();
        appendAddressPart(values, firstNonNull(addressNode, "Country", "country"));
        appendAddressPart(values, firstNonNull(addressNode, "State", "state"));
        appendAddressPart(values, firstNonNull(addressNode, "City", "city"));
        appendAddressPart(values, firstNonNull(addressNode, "Street", "street"));
        appendAddressPart(values, firstNonNull(addressNode, "PostalCode", "postalCode"));
        return String.join(" ", values).trim();
    }

    private void appendAddressPart(java.util.List<String> values, JsonNode node) {
        if (node == null || node.isNull()) {
            return;
        }
        String text = node.asText("").trim();
        if (!text.isBlank()) {
            values.add(text);
        }
    }

    private boolean isScalarAlias(String fieldName) {
        return fieldName != null && (
                "stringValue".equals(fieldName)
                        || "booleanValue".equals(fieldName)
                        || "intValue".equals(fieldName)
                        || "doubleValue".equals(fieldName)
                        || "longValue".equals(fieldName)
                        || "dateValue".equals(fieldName)
                        || "dateTimeValue".equals(fieldName)
                        || "timeValue".equals(fieldName)
        );
    }

    private String formatFieldList(java.util.Collection<?> fields) {
        if (fields == null || fields.isEmpty()) {
            return "    - (empty)";
        }
        StringBuilder sb = new StringBuilder();
        for (Object field : fields) {
            sb.append("    - ").append(field).append("\n");
        }
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }

    private String formatIndentedBlock(String text) {
        if (text == null || text.isBlank()) {
            return "    (empty)";
        }
        String formatted = formatSqlForLog(text);
        return "    " + formatted.replace("\r", "").replace("\n", "\n    ");
    }

    private String formatSqlForLog(String sql) {
        if (sql == null || sql.isBlank()) {
            return "(empty)";
        }
        String normalized = sql.replace("\r", "").trim();
        normalized = normalized.replace(" SET ", "\nSET\n    ");
        normalized = normalized.replace(",", ",\n    ");
        normalized = normalized.replace(" WHERE ", "\nWHERE\n    ");
        normalized = normalized.replace(" AND ", "\n    AND ");
        return normalized;
    }

    private String prettyNode(JsonNode node) {
        if (node == null || node.isNull()) {
            return "null";
        }
        if (node.isValueNode()) {
            return node.asText();
        }
        String compact = node.toPrettyString().replace("\r", "");
        return compact.length() > 1000 ? compact.substring(0, 1000) + "..." : compact;
    }

    private String describeNode(JsonNode node) {
        if (node == null) {
            return "null";
        }
        String text = node.asText();
        if (text == null || text.isBlank()) {
            return node.getNodeType() + " size=" + node.size();
        }
        return node.getNodeType() + ":" + text;
    }

    private String extractFieldKeys(JsonNode objectNode) {
        if (objectNode == null || !objectNode.isObject()) {
            return "[]";
        }
        java.util.List<String> keys = new java.util.ArrayList<>();
        objectNode.fieldNames().forEachRemaining(keys::add);
        return keys.toString();
    }

    private int insertFallback(String targetSchema,
                               String selectedObject,
                               Map<String, Object> mapType,
                               SalesforceRecordMutation mutation,
                               SalesforceMutationRepositoryPort repository,
                               String sourceLabel) {
        List<String> insertFields = new ArrayList<>(mutation.targetFields());
        StringBuilder valuesBuilder = new StringBuilder(SqlSanitizer.quoteString(mutation.sfid()));

        for (String fieldName : insertFields) {
            JsonNode fieldValue = normalizeFieldValue(resolveRawFieldValue(mutation.payload(), fieldName), fieldName);
            if (mutation.nulledFields().contains(fieldName) || fieldValue == null || fieldValue.isNull()) {
                valuesBuilder.append(",null");
            } else {
                valuesBuilder.append(",").append(sanitizeFieldValue(fieldName, fieldValue, String.valueOf(mapType.get(fieldName))));
            }
        }

        valuesBuilder.append(",")
                .append(mutation.incomingLastModifiedLiteral())
                .append(",")
                .append(mutation.incomingEventLiteral());

        String upperQuery = SalesforceObjectSchemaBuilder.buildInsertSql(targetSchema, selectedObject, String.join(",", insertFields));
        String tailQuery = SalesforceObjectSchemaBuilder.buildInsertTail(selectedObject, insertFields);

        String insertSql = upperQuery + "\n" + "(" + valuesBuilder + ")" + (tailQuery == null || tailQuery.isBlank() ? "" : "\n" + tailQuery);

        log.warn("""
                [CDC-INSERT-SQL]
                  mode: fallback
                  sfid: {}
                  sql:
                {}
                """, mutation.sfid(), formatIndentedBlock(insertSql));

        return repository.insertObject(upperQuery, List.of("(" + valuesBuilder + ")"), tailQuery);
    }

    private int insertMinimal(String targetSchema, String selectedObject, String sfid, SalesforceMutationRepositoryPort repository) {
        String upperQuery = "Insert Into " + SalesforceObjectSchemaBuilder.qualifiedName(targetSchema, selectedObject) + "(sfid, "
                + SalesforceObjectSchemaBuilder.INTERNAL_LAST_MODIFIED_COLUMN + ", "
                + SalesforceObjectSchemaBuilder.INTERNAL_LAST_EVENT_AT_COLUMN + ") values";
        String valuesSql = "(" + SqlSanitizer.quoteSfid(sfid) + ", null, null)";
        String insertSql = upperQuery + "\n" + valuesSql;

        log.warn("""
                [CDC-INSERT-SQL]
                  mode: minimal
                  sfid: {}
                  sql:
                {}
                """, sfid, formatIndentedBlock(insertSql));

        return repository.insertObject(upperQuery, List.of(valuesSql), "");
    }

    public record MutationResult(int updated, int inserted, int deleted) {
    }
}
