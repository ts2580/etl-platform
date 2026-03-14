package com.apache.sfdc.common;

import com.apache.sfdc.pubsub.repository.PubSubRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.builder.RouteBuilder;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

@Slf4j
public class SalesforceRouterBuilderCDC extends RouteBuilder {
    private final String selectedObject;
    private final Map<String, Object> mapType;
    private final PubSubRepository pubSubRepository;

    public SalesforceRouterBuilderCDC(String selectedObject, Map<String, Object> mapType, PubSubRepository pubSubRepository) {
        this.selectedObject = selectedObject;
        this.mapType = mapType;
        this.pubSubRepository = pubSubRepository;
    }

    @Override
    public void configure() throws Exception {

        SqlSanitizer.validateTableName(selectedObject);

        String eventName = selectedObject.contains("__c")
                ? selectedObject.replace("__c", "__ChangeEvent")
                : selectedObject + "ChangeEvent";

        from("sf:pubSubSubscribe:/data/" + eventName)
                .aggregate(constant(true), new ArrayListAggregationStrategy())
                .completionInterval(5000)
                .process((exchange) -> {
                    ObjectMapper objectMapper = new ObjectMapper();

                    Map<String, List<Object>> messageBodies = exchange.getIn().getBody(Map.class);
                    if (messageBodies == null || messageBodies.isEmpty()) {
                        return;
                    }

                    List<Object> payloads = new ArrayList<>();
                    for (List<Object> bodies : messageBodies.values()) {
                        if (bodies != null && !bodies.isEmpty()) {
                            payloads.addAll(bodies);
                        }
                    }

                    if (payloads.isEmpty()) {
                        return;
                    }

                    List<String> listDeleteIds = new ArrayList<>();
                    int updatedCount = 0;
                    int insertedCount = 0;

                    for (Object body : payloads) {
                        JsonNode root = objectMapper.valueToTree(body);
                        JsonNode payload = root.has("payload") ? root.get("payload") : root;
                        JsonNode header = payload.get("ChangeEventHeader");

                        if (header == null || header.isNull()) {
                            continue;
                        }

                        String changeType = textOf(header, "changeType");
                        String sfid = extractSfid(payload, header);

                        if ("DELETE".equalsIgnoreCase(changeType)) {
                            collectDeleteIds(header.get("recordIds"), listDeleteIds);
                            continue;
                        }

                        if (sfid == null || sfid.isBlank()) {
                            continue;
                        }

                        Set<String> targetFields = resolveTargetFields(payload, header);

                        StringBuilder strUpdate = new StringBuilder();
                        strUpdate.append("UPDATE config.").append(selectedObject).append(" SET ");

                        int assignmentCount = appendAssignments(strUpdate, payload, header, targetFields);
                        if (assignmentCount > 0) {
                            strUpdate.deleteCharAt(strUpdate.length() - 1);
                            strUpdate.append(" WHERE sfid = ").append(SqlSanitizer.quoteString(sfid)).append(";");

                            int updated = pubSubRepository.updateObject(strUpdate);

                            if (updated == 0 && ("CREATE".equalsIgnoreCase(changeType) || "UNDELETE".equalsIgnoreCase(changeType))) {
                                int inserted = insertFallback(payload, header, sfid, targetFields);
                                insertedCount += inserted;
                            } else {
                                updatedCount += updated;
                            }
                        } else if ("CREATE".equalsIgnoreCase(changeType) || "UNDELETE".equalsIgnoreCase(changeType)) {
                            int inserted = insertMinimal(sfid);
                            insertedCount += inserted;
                        }
                    }

                    if (!listDeleteIds.isEmpty()) {
                        Instant deleteStart = Instant.now();
                        int deleted = pubSubRepository.deleteObject("config." + selectedObject, listDeleteIds);
                        Instant deleteEnd = Instant.now();
                        Duration deleteInterval = Duration.between(deleteStart, deleteEnd);
                        log.info("[CDC] deleted={}, took={}ms", deleted, deleteInterval.toMillis());
                    }

                    log.info("[CDC] updated={}, inserted={}, received={}", updatedCount, insertedCount, payloads.size());
                });
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

    private void collectDeleteIds(JsonNode recordIdsNode, List<String> listDeleteIds) {
        if (recordIdsNode == null || !recordIdsNode.isArray()) {
            return;
        }
        for (JsonNode idNode : recordIdsNode) {
            listDeleteIds.add(SqlSanitizer.quoteSfid(idNode.asText()));
        }
    }

    private Set<String> resolveTargetFields(JsonNode payload, JsonNode header) {
        Set<String> targetFields = new LinkedHashSet<>();

        addFieldArray(header.get("changedFields"), targetFields);
        addFieldArray(header.get("nulledFields"), targetFields);

        if (targetFields.isEmpty()) {
            Iterator<Map.Entry<String, JsonNode>> fields = payload.fields();
            while (fields.hasNext()) {
                String fieldName = fields.next().getKey();
                if (isDataField(fieldName)) {
                    targetFields.add(fieldName);
                }
            }
        }

        targetFields.removeIf(field -> !mapType.containsKey(field));

        return targetFields;
    }

    private void addFieldArray(JsonNode arrayNode, Set<String> output) {
        if (arrayNode == null || !arrayNode.isArray()) {
            return;
        }

        for (JsonNode item : arrayNode) {
            String raw = item.asText();
            if (raw == null || raw.isBlank()) {
                continue;
            }

            if (raw.contains(".")) {
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
                && !"Id".equals(fieldName);
    }

    private int appendAssignments(StringBuilder strUpdate, JsonNode payload, JsonNode header, Set<String> targetFields) {
        Set<String> nulledFields = new HashSet<>();
        JsonNode nulledArray = header.get("nulledFields");
        if (nulledArray != null && nulledArray.isArray()) {
            for (JsonNode n : nulledArray) {
                nulledFields.add(n.asText());
            }
        }

        int assignmentCount = 0;
        for (String fieldName : targetFields) {
            strUpdate.append(fieldName).append(" = ");

            JsonNode fieldValue = payload.get(fieldName);
            if (nulledFields.contains(fieldName) || fieldValue == null || fieldValue.isNull()) {
                strUpdate.append("null,");
                assignmentCount++;
                continue;
            }

            strUpdate.append(SqlSanitizer.sanitizeValue(fieldValue, String.valueOf(mapType.get(fieldName)))).append(",");
            assignmentCount++;
        }

        return assignmentCount;
    }

    private int insertFallback(JsonNode payload, JsonNode header, String sfid, Set<String> targetFields) {
        StringBuilder fieldsBuilder = new StringBuilder("sfid");
        StringBuilder valuesBuilder = new StringBuilder(SqlSanitizer.quoteString(sfid));

        Set<String> nulledFields = new HashSet<>();
        JsonNode nulledArray = header.get("nulledFields");
        if (nulledArray != null && nulledArray.isArray()) {
            for (JsonNode n : nulledArray) {
                nulledFields.add(n.asText());
            }
        }

        for (String fieldName : targetFields) {
            fieldsBuilder.append(",").append(fieldName);

            JsonNode fieldValue = payload.get(fieldName);
            if (nulledFields.contains(fieldName) || fieldValue == null || fieldValue.isNull()) {
                valuesBuilder.append(",null");
            } else {
                valuesBuilder.append(",").append(SqlSanitizer.sanitizeValue(fieldValue, String.valueOf(mapType.get(fieldName))));
            }
        }

        String upperQuery = "Insert Into config." + selectedObject + "(" + fieldsBuilder + ") values";

        Instant insertStart = Instant.now();
        int inserted = pubSubRepository.insertObject(upperQuery, List.of("(" + valuesBuilder + ")"));
        Instant insertEnd = Instant.now();

        Duration insertInterval = Duration.between(insertStart, insertEnd);
        log.info("[CDC] insert fallback inserted={}, took={}ms", inserted, insertInterval.toMillis());

        return inserted;
    }

    private int insertMinimal(String sfid) {
        String upperQuery = "Insert Into config." + selectedObject + "(sfid) values";
        return pubSubRepository.insertObject(upperQuery, List.of("(" + SqlSanitizer.quoteSfid(sfid) + ")"));
    }
}
