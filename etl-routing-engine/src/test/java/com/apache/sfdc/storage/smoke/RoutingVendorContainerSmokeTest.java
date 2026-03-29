package com.apache.sfdc.storage.smoke;

import com.apache.sfdc.common.SalesforceMutationRepositoryPort;
import com.apache.sfdc.common.SalesforceMutationType;
import com.apache.sfdc.common.SalesforceObjectSchemaBuilder;
import com.apache.sfdc.common.SalesforceRecordMutation;
import com.apache.sfdc.common.SalesforceRecordMutationProcessor;
import com.etlplatform.common.storage.database.DatabaseVendor;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

@Tag("vendor-container-smoke")
class RoutingVendorContainerSmokeTest {

    private static final String TARGET_OBJECT = "Account";
    private static final String TARGET_OBJECT_CUSTOM = "Account_Custom_Smoke";
    private static final String ORG_NAME = "Study Org";
    private static final String SFID = "001000000000001AAA";
    private static final String SECOND_SFID = "001000000000002AAA";
    private static final String CREATE_ONLY_SFID = "001000000000003AAA";
    private static final String UNDELETE_SFID = "001000000000004AAA";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final SalesforceRecordMutationProcessor mutationProcessor = new SalesforceRecordMutationProcessor();

    static Stream<VendorContainerDescriptor> vendors() {
        return VendorContainerDescriptor.supported().stream();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("vendors")
    void executesReusableVendorSmokeScenariosAgainstRealVendorContainers(VendorContainerDescriptor descriptor) throws Exception {
        try (var db = RoutingVendorContainerTestSupport.start(descriptor)) {
            runSharedInitialLoadAndLifecycleScenario(db, descriptor);
            runCreateFallbackScenario(db, descriptor);
            runUndeleteReinsertScenario(db, descriptor);
            runCustomTargetTableScenario(db, descriptor);
        }
    }

    private void runSharedInitialLoadAndLifecycleScenario(RoutingVendorContainerTestSupport.RunningVendorDatabase db,
                                                          VendorContainerDescriptor descriptor) throws Exception {
        SalesforceObjectSchemaBuilder.SchemaResult schemaResult = buildSchema(descriptor, db, TARGET_OBJECT, TARGET_OBJECT);
        db.recreateTable(TARGET_OBJECT, defaultOrgName(descriptor), schemaResult);

        ArrayNode initialRecords = OBJECT_MAPPER.createArrayNode();
        initialRecords.add(record(
                SFID,
                "Initial Account",
                "false",
                "101.25",
                "2026-03-28",
                "08:10:15.123456",
                "first load",
                "2026-03-28T00:00:00"
        ));
        initialRecords.add(record(
                SECOND_SFID,
                "Second Account",
                "true",
                "42.10",
                "2026-03-29",
                "07:00:00.000001",
                "batch row",
                "2026-03-28T00:05:00"
        ));

        int inserted = db.repositoryPort().insertObject(
                SalesforceObjectSchemaBuilder.buildPreparedInsertBatch(
                        db.targetSchema(),
                        physicalTargetTable(db, descriptor, TARGET_OBJECT),
                        schemaResult,
                        initialRecords,
                        db.strategy()
                )
        );
        assertEquals(2, inserted);
        assertEquals(2, db.countRows(TARGET_OBJECT, defaultOrgName(descriptor)));

        assertRow(
                db.fetchRow(TARGET_OBJECT, defaultOrgName(descriptor), SFID),
                SFID,
                "Initial Account",
                false,
                "101.25",
                LocalDate.of(2026, 3, 28),
                "08:10:15.123456",
                "first load",
                LocalDateTime.of(2026, 3, 28, 0, 0),
                LocalDateTime.of(2026, 3, 28, 0, 0)
        );
        assertRow(
                db.fetchRow(TARGET_OBJECT, defaultOrgName(descriptor), SECOND_SFID),
                SECOND_SFID,
                "Second Account",
                true,
                "42.10",
                LocalDate.of(2026, 3, 29),
                "07:00:00.000001",
                "batch row",
                LocalDateTime.of(2026, 3, 28, 0, 5),
                LocalDateTime.of(2026, 3, 28, 0, 5)
        );

        SalesforceMutationRepositoryPort repositoryPort = db.repositoryPort();
        LocalDateTime updateTime = LocalDateTime.of(2026, 3, 28, 1, 15, 30, 123000000);
        SalesforceRecordMutation freshUpdate = new SalesforceRecordMutation(
                SalesforceMutationType.UPDATE,
                SFID,
                updatePayload("Updated Account", "205.50", "2026-04-01", "09:45:10.654321", null, false),
                orderedFields("Name", "AnnualRevenue", "CloseDate", "DailyCutoff", "Description", "IsDeleted"),
                Set.of("Description"),
                updateTime,
                updateTime,
                db.strategy().renderLiteral(updateTime, "datetime"),
                db.strategy().renderLiteral(updateTime, "datetime")
        );

        var updateResult = mutationProcessor.apply(
                db.targetSchema(),
                TARGET_OBJECT,
                TARGET_OBJECT,
                defaultOrgName(descriptor),
                new java.util.HashMap<>(schemaResult.mapType()),
                freshUpdate,
                repositoryPort,
                descriptor.id()
        );
        assertEquals(1, updateResult.updated());
        assertEquals(0, updateResult.inserted());
        assertEquals(0, updateResult.deleted());

        assertRow(
                db.fetchRow(TARGET_OBJECT, defaultOrgName(descriptor), SFID),
                SFID,
                "Updated Account",
                false,
                "205.50",
                LocalDate.of(2026, 4, 1),
                "09:45:10.654321",
                null,
                updateTime,
                updateTime
        );

        LocalDateTime staleTime = LocalDateTime.of(2026, 3, 27, 23, 59, 59);
        SalesforceRecordMutation staleUpdate = new SalesforceRecordMutation(
                SalesforceMutationType.UPDATE,
                SFID,
                updatePayload("STALE", "999.99", "2026-05-01", "10:00:00.000000", "should not apply", true),
                orderedFields("Name", "AnnualRevenue", "CloseDate", "DailyCutoff", "Description", "IsDeleted"),
                Set.of(),
                staleTime,
                staleTime,
                db.strategy().renderLiteral(staleTime, "datetime"),
                db.strategy().renderLiteral(staleTime, "datetime")
        );

        var staleResult = mutationProcessor.apply(
                db.targetSchema(),
                TARGET_OBJECT,
                TARGET_OBJECT,
                defaultOrgName(descriptor),
                new java.util.HashMap<>(schemaResult.mapType()),
                staleUpdate,
                repositoryPort,
                descriptor.id()
        );
        assertEquals(0, staleResult.updated());
        assertEquals(0, staleResult.inserted());

        assertRow(
                db.fetchRow(TARGET_OBJECT, defaultOrgName(descriptor), SFID),
                SFID,
                "Updated Account",
                false,
                "205.50",
                LocalDate.of(2026, 4, 1),
                "09:45:10.654321",
                null,
                updateTime,
                updateTime
        );

        LocalDateTime deleteTime = LocalDateTime.of(2026, 3, 28, 2, 0, 0);
        SalesforceRecordMutation deleteMutation = new SalesforceRecordMutation(
                SalesforceMutationType.DELETE,
                SFID,
                OBJECT_MAPPER.createObjectNode(),
                Set.of(),
                Set.of(),
                deleteTime,
                deleteTime,
                db.strategy().renderLiteral(deleteTime, "datetime"),
                db.strategy().renderLiteral(deleteTime, "datetime")
        );

        var deleteResult = mutationProcessor.apply(
                db.targetSchema(),
                TARGET_OBJECT,
                TARGET_OBJECT,
                defaultOrgName(descriptor),
                new java.util.HashMap<>(schemaResult.mapType()),
                deleteMutation,
                repositoryPort,
                descriptor.id()
        );
        assertEquals(1, deleteResult.deleted());
        assertEquals(1, db.countRows(TARGET_OBJECT, defaultOrgName(descriptor)));
        assertNull(db.fetchRow(TARGET_OBJECT, defaultOrgName(descriptor), SFID));
        assertNotNull(db.fetchRow(TARGET_OBJECT, defaultOrgName(descriptor), SECOND_SFID));
    }

    private void runCreateFallbackScenario(RoutingVendorContainerTestSupport.RunningVendorDatabase db,
                                           VendorContainerDescriptor descriptor) throws Exception {
        SalesforceObjectSchemaBuilder.SchemaResult schemaResult = buildSchema(descriptor, db, TARGET_OBJECT, TARGET_OBJECT);
        db.recreateTable(TARGET_OBJECT, defaultOrgName(descriptor), schemaResult);

        LocalDateTime createTime = LocalDateTime.of(2026, 3, 28, 3, 0, 0);
        SalesforceRecordMutation createMutation = new SalesforceRecordMutation(
                SalesforceMutationType.CREATE,
                CREATE_ONLY_SFID,
                updatePayload("Created Account", "88.88", "2026-04-02", "11:22:33.444555", "create fallback", false),
                orderedFields("Name", "AnnualRevenue", "CloseDate", "DailyCutoff", "Description", "IsDeleted"),
                Set.of(),
                createTime,
                createTime,
                db.strategy().renderLiteral(createTime, "datetime"),
                db.strategy().renderLiteral(createTime, "datetime")
        );

        var createResult = mutationProcessor.apply(
                db.targetSchema(),
                TARGET_OBJECT,
                TARGET_OBJECT,
                defaultOrgName(descriptor),
                new java.util.HashMap<>(schemaResult.mapType()),
                createMutation,
                db.repositoryPort(),
                descriptor.id()
        );

        assertEquals(0, createResult.updated());
        assertEquals(1, createResult.inserted());
        assertRow(
                db.fetchRow(TARGET_OBJECT, defaultOrgName(descriptor), CREATE_ONLY_SFID),
                CREATE_ONLY_SFID,
                "Created Account",
                false,
                "88.88",
                LocalDate.of(2026, 4, 2),
                "11:22:33.444555",
                "create fallback",
                createTime,
                createTime
        );
    }

    private void runUndeleteReinsertScenario(RoutingVendorContainerTestSupport.RunningVendorDatabase db,
                                             VendorContainerDescriptor descriptor) throws Exception {
        SalesforceObjectSchemaBuilder.SchemaResult schemaResult = buildSchema(descriptor, db, TARGET_OBJECT, TARGET_OBJECT);
        db.recreateTable(TARGET_OBJECT, defaultOrgName(descriptor), schemaResult);

        int inserted = db.repositoryPort().insertObject(
                SalesforceObjectSchemaBuilder.buildPreparedInsertBatch(
                        db.targetSchema(),
                        physicalTargetTable(db, descriptor, TARGET_OBJECT),
                        schemaResult,
                        singleRecordArray(record(
                                UNDELETE_SFID,
                                "Deleted Account",
                                "true",
                                "15.00",
                                "2026-03-30",
                                "06:00:00.000000",
                                "before delete",
                                "2026-03-28T03:30:00"
                        )),
                        db.strategy()
                )
        );
        assertEquals(1, inserted);

        LocalDateTime deleteTime = LocalDateTime.of(2026, 3, 28, 3, 45, 0);
        var deleteResult = mutationProcessor.apply(
                db.targetSchema(),
                TARGET_OBJECT,
                TARGET_OBJECT,
                defaultOrgName(descriptor),
                new java.util.HashMap<>(schemaResult.mapType()),
                new SalesforceRecordMutation(
                        SalesforceMutationType.DELETE,
                        UNDELETE_SFID,
                        OBJECT_MAPPER.createObjectNode(),
                        Set.of(),
                        Set.of(),
                        deleteTime,
                        deleteTime,
                        db.strategy().renderLiteral(deleteTime, "datetime"),
                        db.strategy().renderLiteral(deleteTime, "datetime")
                ),
                db.repositoryPort(),
                descriptor.id()
        );
        assertEquals(1, deleteResult.deleted());
        assertNull(db.fetchRow(TARGET_OBJECT, defaultOrgName(descriptor), UNDELETE_SFID));

        LocalDateTime undeleteTime = LocalDateTime.of(2026, 3, 28, 4, 0, 0);
        var undeleteResult = mutationProcessor.apply(
                db.targetSchema(),
                TARGET_OBJECT,
                TARGET_OBJECT,
                defaultOrgName(descriptor),
                new java.util.HashMap<>(schemaResult.mapType()),
                new SalesforceRecordMutation(
                        SalesforceMutationType.UNDELETE,
                        UNDELETE_SFID,
                        updatePayload("Restored Account", "16.00", "2026-03-31", "06:30:00.000000", "undeleted", false),
                        orderedFields("Name", "AnnualRevenue", "CloseDate", "DailyCutoff", "Description", "IsDeleted"),
                        Set.of(),
                        undeleteTime,
                        undeleteTime,
                        db.strategy().renderLiteral(undeleteTime, "datetime"),
                        db.strategy().renderLiteral(undeleteTime, "datetime")
                ),
                db.repositoryPort(),
                descriptor.id()
        );

        assertEquals(0, undeleteResult.updated());
        assertEquals(1, undeleteResult.inserted());
        assertRow(
                db.fetchRow(TARGET_OBJECT, defaultOrgName(descriptor), UNDELETE_SFID),
                UNDELETE_SFID,
                "Restored Account",
                false,
                "16.00",
                LocalDate.of(2026, 3, 31),
                "06:30:00.000000",
                "undeleted",
                undeleteTime,
                undeleteTime
        );
    }

    private void runCustomTargetTableScenario(RoutingVendorContainerTestSupport.RunningVendorDatabase db,
                                              VendorContainerDescriptor descriptor) throws Exception {
        SalesforceObjectSchemaBuilder.SchemaResult schemaResult = buildSchema(descriptor, db, TARGET_OBJECT, TARGET_OBJECT_CUSTOM);
        db.recreateTable(TARGET_OBJECT_CUSTOM, null, schemaResult);

        LocalDateTime createTime = LocalDateTime.of(2026, 3, 28, 4, 30, 0);
        var createResult = mutationProcessor.apply(
                db.targetSchema(),
                TARGET_OBJECT,
                TARGET_OBJECT_CUSTOM,
                defaultOrgName(descriptor),
                new java.util.HashMap<>(schemaResult.mapType()),
                new SalesforceRecordMutation(
                        SalesforceMutationType.CREATE,
                        SFID,
                        updatePayload("Custom Table Account", "77.70", "2026-04-03", "12:00:01.000002", "custom target", false),
                        orderedFields("Name", "AnnualRevenue", "CloseDate", "DailyCutoff", "Description", "IsDeleted"),
                        Set.of(),
                        createTime,
                        createTime,
                        db.strategy().renderLiteral(createTime, "datetime"),
                        db.strategy().renderLiteral(createTime, "datetime")
                ),
                db.repositoryPort(),
                descriptor.id()
        );
        assertEquals(1, createResult.inserted());

        LocalDateTime updateTime = LocalDateTime.of(2026, 3, 28, 4, 45, 0);
        var updateResult = mutationProcessor.apply(
                db.targetSchema(),
                TARGET_OBJECT,
                TARGET_OBJECT_CUSTOM,
                defaultOrgName(descriptor),
                new java.util.HashMap<>(schemaResult.mapType()),
                new SalesforceRecordMutation(
                        SalesforceMutationType.UPDATE,
                        SFID,
                        updatePayload("Custom Table Account Updated", "79.90", "2026-04-04", "12:30:01.000002", "custom updated", false),
                        orderedFields("Name", "AnnualRevenue", "CloseDate", "DailyCutoff", "Description", "IsDeleted"),
                        Set.of(),
                        updateTime,
                        updateTime,
                        db.strategy().renderLiteral(updateTime, "datetime"),
                        db.strategy().renderLiteral(updateTime, "datetime")
                ),
                db.repositoryPort(),
                descriptor.id()
        );
        assertEquals(1, updateResult.updated());

        assertRow(
                db.fetchPhysicalRow(TARGET_OBJECT_CUSTOM, SFID),
                SFID,
                "Custom Table Account Updated",
                false,
                "79.90",
                LocalDate.of(2026, 4, 4),
                "12:30:01.000002",
                "custom updated",
                updateTime,
                updateTime
        );
    }

    private SalesforceObjectSchemaBuilder.SchemaResult buildSchema(VendorContainerDescriptor descriptor,
                                                                   RoutingVendorContainerTestSupport.RunningVendorDatabase db,
                                                                   String selectedObject,
                                                                   String targetTable) throws Exception {
        return SalesforceObjectSchemaBuilder.buildSchema(
                db.targetSchema(),
                selectedObject,
                targetTable,
                defaultOrgName(descriptor, selectedObject),
                schemaFields(),
                OBJECT_MAPPER,
                db.strategy()
        );
    }

    private String defaultOrgName(VendorContainerDescriptor descriptor) {
        return defaultOrgName(descriptor, TARGET_OBJECT);
    }

    private String defaultOrgName(VendorContainerDescriptor descriptor, String selectedObject) {
        if (descriptor.vendor() == DatabaseVendor.ORACLE && TARGET_OBJECT.equals(selectedObject)) {
            return ORG_NAME;
        }
        return null;
    }

    private String physicalTargetTable(RoutingVendorContainerTestSupport.RunningVendorDatabase db,
                                       VendorContainerDescriptor descriptor,
                                       String tableName) {
        return descriptor.vendor() == DatabaseVendor.ORACLE && TARGET_OBJECT.equals(tableName)
                ? SalesforceObjectSchemaBuilder.resolvePhysicalTableName(db.targetSchema(), tableName, ORG_NAME, db.strategy())
                : tableName;
    }

    private JsonNode schemaFields() throws Exception {
        return OBJECT_MAPPER.readTree("""
                [
                  {"name":"Id","type":"id","label":"ID","length":18},
                  {"name":"Name","type":"string","label":"Name","length":255},
                  {"name":"IsDeleted","type":"boolean","label":"Deleted","length":1},
                  {"name":"AnnualRevenue","type":"currency","label":"Revenue","length":18},
                  {"name":"CloseDate","type":"date","label":"Close Date","length":0},
                  {"name":"DailyCutoff","type":"time","label":"Cutoff","length":0},
                  {"name":"Description","type":"textarea","label":"Description","length":32000},
                  {"name":"LastModifiedDate","type":"datetime","label":"Modified","length":0}
                ]
                """);
    }

    private static ArrayNode singleRecordArray(ObjectNode record) {
        ArrayNode arrayNode = OBJECT_MAPPER.createArrayNode();
        arrayNode.add(record);
        return arrayNode;
    }

    private static ObjectNode record(String sfid,
                                     String name,
                                     String isDeleted,
                                     String annualRevenue,
                                     String closeDate,
                                     String dailyCutoff,
                                     String description,
                                     String lastModifiedDate) {
        ObjectNode payload = updatePayload(name, annualRevenue, closeDate, dailyCutoff, description, Boolean.parseBoolean(isDeleted));
        payload.put("Id", sfid);
        payload.put(SalesforceObjectSchemaBuilder.LAST_MODIFIED_FIELD, lastModifiedDate);
        return payload;
    }

    private static ObjectNode updatePayload(String name,
                                            String annualRevenue,
                                            String closeDate,
                                            String dailyCutoff,
                                            String description,
                                            boolean isDeleted) {
        ObjectNode payload = OBJECT_MAPPER.createObjectNode();
        payload.put("Name", name);
        payload.put("AnnualRevenue", annualRevenue);
        payload.put("CloseDate", closeDate);
        payload.put("DailyCutoff", dailyCutoff);
        payload.put("IsDeleted", isDeleted);
        if (description == null) {
            payload.putNull("Description");
        } else {
            payload.put("Description", description);
        }
        return payload;
    }

    private static Set<String> orderedFields(String... fieldNames) {
        return new LinkedHashSet<>(java.util.List.of(fieldNames));
    }

    private static void assertRow(RoutingVendorContainerTestSupport.PersistedRow row,
                                  String sfid,
                                  String name,
                                  boolean deleted,
                                  String annualRevenue,
                                  LocalDate closeDate,
                                  String dailyCutoff,
                                  String description,
                                  LocalDateTime lastModifiedAt,
                                  LocalDateTime lastEventAt) {
        assertNotNull(row);
        assertEquals(sfid, row.sfid());
        assertEquals(name, row.name());
        assertEquals(deleted, row.deleted());
        assertEquals(0, row.annualRevenue().compareTo(new BigDecimal(annualRevenue)));
        assertEquals(closeDate, row.closeDate());
        assertEquals(normalizeTime(dailyCutoff), normalizeTime(row.dailyCutoff()));
        assertEquals(description, row.description());
        assertEquals(lastModifiedAt, row.lastModifiedAt());
        assertEquals(lastEventAt, row.lastEventAt());
    }

    private static String normalizeTime(String value) {
        if (value == null) {
            return null;
        }
        String normalized = value.replace(' ', 'T').contains("T")
                ? value.substring(value.indexOf('T') + 1)
                : value;
        if (!normalized.contains(".")) {
            return normalized;
        }
        normalized = normalized.replaceAll("0+$", "");
        return normalized.endsWith(".") ? normalized.substring(0, normalized.length() - 1) : normalized;
    }
}
