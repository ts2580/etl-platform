package com.apache.sfdc.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

import java.util.ArrayList;
import java.util.List;

public final class SalesforceRecordMutationPayloadResolver {

    public JsonNode resolveRawFieldValue(JsonNode payload, String fieldName) {
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

    public JsonNode normalizeFieldValue(JsonNode valueNode, String fieldName) {
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
                return JsonNodeFactory.instance.textNode(fullName);
            }
        }

        if (isCompoundAddressField(fieldName)) {
            String formattedAddress = joinAddressParts(valueNode);
            if (!formattedAddress.isBlank()) {
                return JsonNodeFactory.instance.textNode(formattedAddress);
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

    public Object toBindableFieldValue(JsonNode fieldValue) {
        if (fieldValue == null || fieldValue.isNull()) {
            return null;
        }
        if (fieldValue.isObject() || fieldValue.isArray()) {
            return fieldValue.toString();
        }
        return SqlSanitizer.toRawValue(fieldValue);
    }

    public JsonNode firstNonNull(JsonNode node, String... names) {
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

    public String joinAddressParts(JsonNode addressNode) {
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

    public boolean isScalarAlias(String fieldName) {
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

    private void appendAddressPart(List<String> values, JsonNode node) {
        if (node == null || node.isNull()) {
            return;
        }
        String text = node.asText("").trim();
        if (!text.isBlank()) {
            values.add(text);
        }
    }

    private boolean isCompoundAddressField(String fieldName) {
        return "OtherAddress".equals(fieldName)
                || "MailingAddress".equals(fieldName)
                || "BillingAddress".equals(fieldName)
                || "ShippingAddress".equals(fieldName);
    }
}
