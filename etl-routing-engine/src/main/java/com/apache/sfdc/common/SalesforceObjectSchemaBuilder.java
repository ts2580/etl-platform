package com.apache.sfdc.common;

import com.apache.sfdc.streaming.dto.FieldDefinition;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class SalesforceObjectSchemaBuilder {

    public static final String LAST_MODIFIED_FIELD = "LastModifiedDate";
    public static final String INTERNAL_LAST_MODIFIED_COLUMN = "_oc_last_modified_at";
    public static final String INTERNAL_LAST_EVENT_AT_COLUMN = "_oc_last_event_at";

    private SalesforceObjectSchemaBuilder() {}

    public static SchemaResult buildSchema(String targetSchema, String selectedObject, JsonNode fieldsNode) throws IOException {
        return buildSchema(targetSchema, selectedObject, fieldsNode, new ObjectMapper());
    }

    public static SchemaResult buildSchema(String targetSchema, String selectedObject, JsonNode fieldsNode, ObjectMapper objectMapper) throws IOException {
        if (fieldsNode == null || !fieldsNode.isArray()) {
            throw new IllegalArgumentException("Invalid Salesforce describe fields response");
        }

        SqlSanitizer.validateSchemaName(targetSchema);
        SqlSanitizer.validateTableName(selectedObject);

        List<FieldDefinition> listDef = objectMapper.convertValue(fieldsNode, new TypeReference<List<FieldDefinition>>() {});

        StringBuilder ddl = new StringBuilder();
        StringBuilder soql = new StringBuilder();
        StringBuilder soqlForPushTopic = new StringBuilder();
        List<String> listFields = new ArrayList<>();
        Map<String, String> mapType = new HashMap<>();

        ddl.append("CREATE TABLE IF NOT EXISTS ").append(qualifiedName(targetSchema, selectedObject)).append("(");

        for (FieldDefinition obj : listDef) {
            mapType.put(obj.name, obj.type);
            String label = obj.label.replace("'", "`");

            switch (obj.type) {
                case "id" -> ddl.append("sfid VARCHAR(18) primary key not null comment '").append(label).append("',");
                case "textarea" -> {
                    if (obj.length > 4000) {
                        ddl.append(obj.name).append(" TEXT comment '").append(label).append("',");
                    } else {
                        ddl.append(obj.name).append(" VARCHAR(").append(obj.length).append(") comment '").append(label).append("',");
                    }
                    listFields.add(obj.name);
                }
                case "reference" -> {
                    ddl.append(obj.name).append(" VARCHAR(18) comment '").append(label).append("',");
                    listFields.add(obj.name);
                }
                case "string", "picklist", "multipicklist", "phone", "url" -> {
                    ddl.append(obj.name).append(" VARCHAR(").append(obj.length).append(") comment '").append(label).append("',");
                    listFields.add(obj.name);
                }
                case "boolean" -> {
                    ddl.append(obj.name).append(" boolean comment '").append(label).append("',");
                    listFields.add(obj.name);
                }
                case "datetime" -> {
                    ddl.append(obj.name).append(" TIMESTAMP comment '").append(label).append("',");
                    listFields.add(obj.name);
                }
                case "date" -> {
                    ddl.append(obj.name).append(" date comment '").append(label).append("',");
                    listFields.add(obj.name);
                }
                case "time" -> {
                    ddl.append(obj.name).append(" time comment '").append(label).append("',");
                    listFields.add(obj.name);
                }
                case "double", "percent", "currency" -> {
                    ddl.append(obj.name).append(" double precision comment '").append(label).append("',");
                    listFields.add(obj.name);
                }
                case "int" -> {
                    ddl.append(obj.name).append(" int comment '").append(label).append("',");
                    listFields.add(obj.name);
                }
                default -> {
                    ddl.append(obj.name).append(" VARCHAR(255) comment '").append(label).append("',");
                    listFields.add(obj.name);
                }
            }
        }

        ddl.append(INTERNAL_LAST_MODIFIED_COLUMN).append(" TIMESTAMP null comment 'OpenClaw latest LastModifiedDate',");
        ddl.append(INTERNAL_LAST_EVENT_AT_COLUMN).append(" TIMESTAMP null comment 'OpenClaw latest applied event time',");
        ddl.deleteCharAt(ddl.length() - 1);
        ddl.append(");");

        for (String field : listFields) {
            SqlSanitizer.validateIdentifier(field);
            soql.append(field).append(",");
            if (!"textarea".equals(mapType.get(field))) {
                soqlForPushTopic.append(field).append(",");
            }
        }

        if (soql.isEmpty()) {
            throw new IllegalArgumentException("No supported fields for object: " + selectedObject);
        }

        soql.deleteCharAt(soql.length() - 1);
        soqlForPushTopic.deleteCharAt(soqlForPushTopic.length() - 1);

        return new SchemaResult(ddl.toString(), mapType, listFields, soql.toString(), soqlForPushTopic.toString());
    }

    public static String buildInsertValues(JsonNode record, List<String> fields, Map<String, String> mapType) {
        StringBuilder underQuery = new StringBuilder();
        underQuery.append("('").append(record.get("Id").asText()).append("',");

        for (String field : fields) {
            String sfType = mapType.get(field);
            underQuery.append(SqlSanitizer.sanitizeValue(record.get(field), sfType)).append(",");
        }

        String lastModifiedLiteral = lastModifiedLiteral(record);
        underQuery.append(lastModifiedLiteral).append(",").append(lastModifiedLiteral).append(")");
        return underQuery.toString();
    }

    public static String buildInsertSql(String targetSchema, String selectedObject, String soql) {
        return "Insert Into " + qualifiedName(targetSchema, selectedObject) + "(sfid, " + soql + ", "
                + INTERNAL_LAST_MODIFIED_COLUMN + ", " + INTERNAL_LAST_EVENT_AT_COLUMN + ") values";
    }

    public static String buildInsertTail(String selectedObject, List<String> fields) {
        StringBuilder tail = new StringBuilder();
        tail.append(" ON DUPLICATE KEY UPDATE ");
        for (String field : fields) {
            tail.append(field).append(" = IF(")
                    .append("VALUES(").append(INTERNAL_LAST_MODIFIED_COLUMN).append(") IS NULL OR ")
                    .append(INTERNAL_LAST_MODIFIED_COLUMN).append(" IS NULL OR ")
                    .append("VALUES(").append(INTERNAL_LAST_MODIFIED_COLUMN).append(") >= ")
                    .append(INTERNAL_LAST_MODIFIED_COLUMN)
                    .append(", VALUES(").append(field).append("), ").append(field).append("),");
        }
        tail.append(INTERNAL_LAST_MODIFIED_COLUMN).append(" = GREATEST(COALESCE(")
                .append(INTERNAL_LAST_MODIFIED_COLUMN).append(", '1970-01-01 00:00:00'), COALESCE(VALUES(")
                .append(INTERNAL_LAST_MODIFIED_COLUMN).append("), '1970-01-01 00:00:00')),")
                .append(INTERNAL_LAST_EVENT_AT_COLUMN).append(" = GREATEST(COALESCE(")
                .append(INTERNAL_LAST_EVENT_AT_COLUMN).append(", '1970-01-01 00:00:00'), COALESCE(VALUES(")
                .append(INTERNAL_LAST_EVENT_AT_COLUMN).append("), '1970-01-01 00:00:00'))");
        return tail.toString();
    }

    public static String buildInitialQuery(String selectedObject, List<String> fields) {
        return "SELECT Id, " + String.join(",", fields) + " FROM " + selectedObject;
    }

    public static String lastModifiedLiteral(JsonNode payload) {
        JsonNode node = payload != null ? payload.get(LAST_MODIFIED_FIELD) : null;
        if (node == null || node.isNull() || node.asText().isBlank()) {
            return "null";
        }
        return SqlSanitizer.sanitizeValue(node, "datetime");
    }

    public static void appendFreshnessAssignments(StringBuilder sql, String incomingLastModifiedLiteral, String incomingEventLiteral) {
        sql.append(INTERNAL_LAST_MODIFIED_COLUMN).append(" = ").append(incomingLastModifiedLiteral).append(",")
                .append(INTERNAL_LAST_EVENT_AT_COLUMN).append(" = ").append(incomingEventLiteral).append(",");
    }

    public static void appendFreshnessWhere(StringBuilder sql, String sfidLiteral, String incomingLastModifiedLiteral) {
        sql.append(" WHERE sfid = ").append(sfidLiteral);
        if (!"null".equalsIgnoreCase(incomingLastModifiedLiteral)) {
            sql.append(" AND (")
                    .append(INTERNAL_LAST_MODIFIED_COLUMN).append(" IS NULL OR ")
                    .append(INTERNAL_LAST_MODIFIED_COLUMN).append(" <= ").append(incomingLastModifiedLiteral)
                    .append(")");
        }
        sql.append(";");
    }

    public static StringBuilder buildConditionalDeleteSql(String targetSchema, String selectedObject, String sfidLiteral, String incomingEventLiteral) {
        StringBuilder sql = new StringBuilder();
        sql.append("DELETE FROM ").append(qualifiedName(targetSchema, selectedObject))
                .append(" WHERE sfid = ").append(sfidLiteral)
                .append(" AND (")
                .append(INTERNAL_LAST_EVENT_AT_COLUMN).append(" IS NULL OR ")
                .append(INTERNAL_LAST_EVENT_AT_COLUMN).append(" <= ").append(incomingEventLiteral)
                .append(");");
        return sql;
    }

    public static String qualifiedName(String targetSchema, String tableName) {
        SqlSanitizer.validateSchemaName(targetSchema);
        SqlSanitizer.validateTableName(tableName);
        return "`" + targetSchema + "`.`" + tableName + "`";
    }

    public record SchemaResult(String ddl, Map<String, String> mapType, List<String> fields,
                               String soql, String soqlForPushTopic) {
    }
}
