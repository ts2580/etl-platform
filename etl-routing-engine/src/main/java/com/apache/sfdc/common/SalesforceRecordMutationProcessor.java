package com.apache.sfdc.common;

import com.etlplatform.common.storage.database.sql.BoundBatchSql;
import com.etlplatform.common.storage.database.sql.BoundSql;
import com.etlplatform.common.storage.database.sql.DatabaseVendorStrategy;
import com.etlplatform.common.storage.database.sql.SqlParameter;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

        if (repository.supportsBoundStatements()) {
            BoundSql boundUpdate = buildBoundUpdate(targetSchema, selectedObject, mapType, mutation, repository.vendorStrategy());
            if (boundUpdate != null) {
                int updated = repository.updateObject(boundUpdate);
                if (updated == 0 && mutation.type().isCreateLike()) {
                    int inserted = repository.insertObject(buildBoundInsertFallback(targetSchema, selectedObject, mapType, mutation, repository.vendorStrategy()));
                    return new MutationResult(updated, inserted, 0);
                }
                return new MutationResult(updated, 0, 0);
            }
        } else {
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

                log.warn("[CDC-UPDATE-SQL]\n  sfid: {}\n  sql:\n{}", mutation.sfid(), formatIndentedBlock(strUpdate.toString()));
                int updated = repository.updateObject(strUpdate);
                if (updated == 0 && mutation.type().isCreateLike()) {
                    int inserted = insertFallback(targetSchema, selectedObject, mapType, mutation, repository);
                    return new MutationResult(updated, inserted, 0);
                }
                return new MutationResult(updated, 0, 0);
            }
        }

        if (mutation.type().isCreateLike()) {
            if (repository.supportsBoundStatements()) {
                int inserted = repository.insertObject(buildBoundMinimalInsert(targetSchema, selectedObject, mutation.sfid(), repository.vendorStrategy()));
                return new MutationResult(0, inserted, 0);
            }
            int inserted = insertMinimal(targetSchema, selectedObject, mutation.sfid(), repository);
            return new MutationResult(0, inserted, 0);
        }

        log.warn("[CDC-APPLY] no-op mutation. type={}, sfid={}, targetFields={}, payloadKeys={}",
                mutation.type(), mutation.sfid(), mutation.targetFields(), mutation.payload().properties().stream().map(Map.Entry::getKey).toList());
        return new MutationResult(0, 0, 0);
    }

    private int delete(String targetSchema,
                       String selectedObject,
                       SalesforceRecordMutation mutation,
                       SalesforceMutationRepositoryPort repository,
                       String sourceLabel) {
        if (repository.supportsBoundStatements()) {
            return repository.deleteObject(buildBoundDelete(targetSchema, selectedObject, mutation, repository.vendorStrategy()));
        }

        StringBuilder strDelete = SalesforceObjectSchemaBuilder.buildConditionalDeleteSql(
                targetSchema,
                selectedObject,
                SqlSanitizer.quoteSfid(mutation.sfid()),
                mutation.incomingEventLiteral()
        );
        log.warn("[CDC-DELETE-SQL]\n  sfid: {}\n  sql:\n{}", mutation.sfid(), formatIndentedBlock(strDelete.toString()));
        return repository.deleteObject(strDelete);
    }

    private BoundSql buildBoundUpdate(String targetSchema,
                                      String selectedObject,
                                      Map<String, Object> mapType,
                                      SalesforceRecordMutation mutation,
                                      DatabaseVendorStrategy strategy) {
        StringBuilder sql = new StringBuilder();
        List<SqlParameter> parameters = new ArrayList<>();
        int assignmentCount = 0;

        sql.append("UPDATE ").append(SalesforceObjectSchemaBuilder.qualifiedName(targetSchema, selectedObject, strategy)).append(" SET ");
        for (String fieldName : mutation.targetFields()) {
            if (assignmentCount > 0) {
                sql.append(", ");
            }
            sql.append(strategy.quoteIdentifier(fieldName)).append(" = ?");

            JsonNode rawFieldValue = resolveRawFieldValue(mutation.payload(), fieldName);
            JsonNode fieldValue = normalizeFieldValue(rawFieldValue, fieldName);
            String sfType = String.valueOf(mapType.get(fieldName));
            if (mutation.nulledFields().contains(fieldName) || fieldValue == null || fieldValue.isNull()) {
                parameters.add(strategy.bindValue(null, sfType));
            } else {
                parameters.add(strategy.bindValue(toBindableFieldValue(fieldValue), sfType));
            }
            assignmentCount++;
        }

        if (assignmentCount == 0) {
            return null;
        }

        sql.append(", ").append(strategy.quoteIdentifier(SalesforceObjectSchemaBuilder.INTERNAL_LAST_MODIFIED_COLUMN)).append(" = ?");
        parameters.add(strategy.bindValue(mutation.incomingLastModifiedValue(), "datetime"));
        sql.append(", ").append(strategy.quoteIdentifier(SalesforceObjectSchemaBuilder.INTERNAL_LAST_EVENT_AT_COLUMN)).append(" = ?");
        parameters.add(strategy.bindValue(mutation.incomingEventValue(), "datetime"));

        sql.append(" WHERE ").append(strategy.quoteIdentifier("sfid")).append(" = ?");
        parameters.add(strategy.bindValue(mutation.sfid(), "id"));

        if (mutation.incomingLastModifiedValue() != null) {
            sql.append(" AND (")
                    .append(strategy.quoteIdentifier(SalesforceObjectSchemaBuilder.INTERNAL_LAST_MODIFIED_COLUMN)).append(" IS NULL OR ")
                    .append(strategy.quoteIdentifier(SalesforceObjectSchemaBuilder.INTERNAL_LAST_MODIFIED_COLUMN)).append(" <= ?)");
            parameters.add(strategy.bindValue(mutation.incomingLastModifiedValue(), "datetime"));
        }

        return new BoundSql(sql.toString(), parameters);
    }

    private BoundSql buildBoundDelete(String targetSchema,
                                      String selectedObject,
                                      SalesforceRecordMutation mutation,
                                      DatabaseVendorStrategy strategy) {
        String sql = "DELETE FROM " + SalesforceObjectSchemaBuilder.qualifiedName(targetSchema, selectedObject, strategy)
                + " WHERE " + strategy.quoteIdentifier("sfid") + " = ?"
                + " AND (" + strategy.quoteIdentifier(SalesforceObjectSchemaBuilder.INTERNAL_LAST_EVENT_AT_COLUMN) + " IS NULL OR "
                + strategy.quoteIdentifier(SalesforceObjectSchemaBuilder.INTERNAL_LAST_EVENT_AT_COLUMN) + " <= ?)";
        return new BoundSql(sql, List.of(
                strategy.bindValue(mutation.sfid(), "id"),
                strategy.bindValue(mutation.incomingEventValue(), "datetime")
        ));
    }

    private BoundBatchSql buildBoundInsertFallback(String targetSchema,
                                                   String selectedObject,
                                                   Map<String, Object> mapType,
                                                   SalesforceRecordMutation mutation,
                                                   DatabaseVendorStrategy strategy) {
        List<String> insertFields = new ArrayList<>(mutation.targetFields());
        List<String> columns = new ArrayList<>();
        columns.add("sfid");
        columns.addAll(insertFields);
        columns.add(SalesforceObjectSchemaBuilder.INTERNAL_LAST_MODIFIED_COLUMN);
        columns.add(SalesforceObjectSchemaBuilder.INTERNAL_LAST_EVENT_AT_COLUMN);

        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ").append(SalesforceObjectSchemaBuilder.qualifiedName(targetSchema, selectedObject, strategy)).append(" (");
        appendQuotedColumns(sql, columns, strategy);
        sql.append(") VALUES (");
        appendPlaceholders(sql, columns.size());
        sql.append(") ").append(SalesforceObjectSchemaBuilder.buildInsertTail(insertFields, strategy));

        List<SqlParameter> parameters = new ArrayList<>();
        parameters.add(strategy.bindValue(mutation.sfid(), "id"));
        for (String fieldName : insertFields) {
            JsonNode fieldValue = normalizeFieldValue(resolveRawFieldValue(mutation.payload(), fieldName), fieldName);
            String sfType = String.valueOf(mapType.get(fieldName));
            if (mutation.nulledFields().contains(fieldName) || fieldValue == null || fieldValue.isNull()) {
                parameters.add(strategy.bindValue(null, sfType));
            } else {
                parameters.add(strategy.bindValue(toBindableFieldValue(fieldValue), sfType));
            }
        }
        parameters.add(strategy.bindValue(mutation.incomingLastModifiedValue(), "datetime"));
        parameters.add(strategy.bindValue(mutation.incomingEventValue(), "datetime"));
        return new BoundBatchSql(sql.toString(), List.of(parameters));
    }

    private BoundBatchSql buildBoundMinimalInsert(String targetSchema,
                                                  String selectedObject,
                                                  String sfid,
                                                  DatabaseVendorStrategy strategy) {
        String sql = "INSERT INTO " + SalesforceObjectSchemaBuilder.qualifiedName(targetSchema, selectedObject, strategy)
                + " (" + strategy.quoteIdentifier("sfid") + ", "
                + strategy.quoteIdentifier(SalesforceObjectSchemaBuilder.INTERNAL_LAST_MODIFIED_COLUMN) + ", "
                + strategy.quoteIdentifier(SalesforceObjectSchemaBuilder.INTERNAL_LAST_EVENT_AT_COLUMN) + ") VALUES (?, ?, ?)";
        return new BoundBatchSql(sql, List.of(List.of(
                strategy.bindValue(sfid, "id"),
                strategy.bindValue(null, "datetime"),
                strategy.bindValue(null, "datetime")
        )));
    }

    private int appendAssignments(StringBuilder strUpdate,
                                  Map<String, Object> mapType,
                                  JsonNode payload,
                                  Set<String> targetFields,
                                  Set<String> nulledFields) {
        int assignmentCount = 0;
        for (String fieldName : targetFields) {
            strUpdate.append(SqlSanitizer.quoteIdentifier(fieldName)).append(" = ");
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

    private Object toBindableFieldValue(JsonNode fieldValue) {
        if (fieldValue == null || fieldValue.isNull()) {
            return null;
        }
        if (fieldValue.isObject() || fieldValue.isArray()) {
            return fieldValue.toString();
        }
        return SqlSanitizer.toRawValue(fieldValue);
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
        if (valueNode == null || valueNode.isNull() || !valueNode.isObject()) {
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

    private String joinNameParts(JsonNode salutationNode, JsonNode firstName, JsonNode lastName) {
        String salutation = salutationNode == null ? "" : salutationNode.asText("").trim();
        String first = firstName == null ? "" : firstName.asText("").trim();
        String last = lastName == null ? "" : lastName.asText("").trim();

        List<String> parts = new ArrayList<>();
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

    private String joinAddressParts(JsonNode addressNode) {
        if (addressNode == null || addressNode.isNull() || !addressNode.isObject()) {
            return "";
        }
        List<String> values = new ArrayList<>();
        appendAddressPart(values, firstNonNull(addressNode, "Country", "country"));
        appendAddressPart(values, firstNonNull(addressNode, "State", "state"));
        appendAddressPart(values, firstNonNull(addressNode, "City", "city"));
        appendAddressPart(values, firstNonNull(addressNode, "Street", "street"));
        appendAddressPart(values, firstNonNull(addressNode, "PostalCode", "postalCode"));
        return String.join(" ", values).trim();
    }

    private void appendAddressPart(List<String> values, JsonNode node) {
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

    private int insertFallback(String targetSchema,
                               String selectedObject,
                               Map<String, Object> mapType,
                               SalesforceRecordMutation mutation,
                               SalesforceMutationRepositoryPort repository) {
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
        return repository.insertObject(upperQuery, List.of("(" + valuesBuilder + ")"), tailQuery);
    }

    private int insertMinimal(String targetSchema, String selectedObject, String sfid, SalesforceMutationRepositoryPort repository) {
        String upperQuery = "INSERT INTO " + SalesforceObjectSchemaBuilder.qualifiedName(targetSchema, selectedObject)
                + "(" + SqlSanitizer.quoteIdentifier("sfid") + ", "
                + SqlSanitizer.quoteIdentifier(SalesforceObjectSchemaBuilder.INTERNAL_LAST_MODIFIED_COLUMN) + ", "
                + SqlSanitizer.quoteIdentifier(SalesforceObjectSchemaBuilder.INTERNAL_LAST_EVENT_AT_COLUMN) + ") VALUES";
        String valuesSql = "(" + SqlSanitizer.quoteSfid(sfid) + ", null, null)";
        return repository.insertObject(upperQuery, List.of(valuesSql), "");
    }

    private String formatIndentedBlock(String text) {
        if (text == null || text.isBlank()) {
            return "    (empty)";
        }
        return "    " + text.replace("\r", "").replace("\n", "\n    ");
    }

    private void appendQuotedColumns(StringBuilder sql, List<String> columns, DatabaseVendorStrategy strategy) {
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append(strategy.quoteIdentifier(columns.get(i)));
        }
    }

    private void appendPlaceholders(StringBuilder sql, int count) {
        for (int i = 0; i < count; i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append("?");
        }
    }

    public record MutationResult(int updated, int inserted, int deleted) {
    }
}
