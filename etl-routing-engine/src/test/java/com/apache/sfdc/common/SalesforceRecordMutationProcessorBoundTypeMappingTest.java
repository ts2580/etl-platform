package com.apache.sfdc.common;

import com.etlplatform.common.storage.database.DatabaseVendor;
import com.etlplatform.common.storage.database.sql.BoundBatchSql;
import com.etlplatform.common.storage.database.sql.BoundSql;
import com.etlplatform.common.storage.database.sql.DatabaseVendorStrategies;
import com.etlplatform.common.storage.database.sql.DatabaseVendorStrategy;
import com.etlplatform.common.storage.database.sql.SqlParameter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Types;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SalesforceRecordMutationProcessorBoundTypeMappingTest {

    private final SalesforceRecordMutationProcessor processor = new SalesforceRecordMutationProcessor();
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void boundUpdateFallsBackObjectAndArrayValuesToJsonStringsWithoutDroppingTypedScalars() throws Exception {
        Map<String, Object> mapType = new LinkedHashMap<>();
        mapType.put("Attributes__c", "string");
        mapType.put("Tags__c", "textarea");
        mapType.put("IsDeleted", "boolean");
        mapType.put("AnnualRevenue", "currency");

        ObjectNode payload = objectMapper.createObjectNode();
        payload.set("Attributes__c", objectMapper.readTree("{\"source\":\"cdc\",\"count\":2}"));
        payload.set("Tags__c", objectMapper.readTree("[\"gold\",\"priority\"]"));
        payload.put("IsDeleted", true);
        payload.put("AnnualRevenue", "12345678901234567890.123456");

        SalesforceRecordMutation mutation = new SalesforceRecordMutation(
                SalesforceMutationType.UPDATE,
                "001xx000003DHP0AAO",
                payload,
                new LinkedHashSet<>(List.of("Attributes__c", "Tags__c", "IsDeleted", "AnnualRevenue")),
                Set.of(),
                LocalDateTime.of(2026, 3, 28, 0, 15, 30, 123456000),
                LocalDateTime.of(2026, 3, 28, 0, 15, 30, 123456000),
                "'2026-03-28 00:15:30.123456'",
                "'2026-03-28 00:15:30.123456'"
        );

        CapturingBoundRepository repository = new CapturingBoundRepository(DatabaseVendorStrategies.defaultStrategy(), 1);

        SalesforceRecordMutationProcessor.MutationResult result = processor.apply(
                "routing",
                "Account",
                null,
                null,
                mapType,
                mutation,
                repository,
                "CDC"
        );

        assertEquals(1, result.updated());
        BoundSql update = repository.lastBoundUpdate;
        assertNotNull(update);
        assertEquals("{\"source\":\"cdc\",\"count\":2}", update.parameters().get(0).value());
        assertEquals("[\"gold\",\"priority\"]", update.parameters().get(1).value());
        assertEquals(Boolean.TRUE, update.parameters().get(2).value());
        assertEquals(Types.BOOLEAN, update.parameters().get(2).sqlType());
        assertEquals(new BigDecimal("12345678901234567890.123456"), update.parameters().get(3).value());
        assertEquals(Types.DOUBLE, update.parameters().get(3).sqlType());
    }

    @Test
    void oracleCreateFallbackBindsBooleanAndDatetimeWithOracleSpecificTypes() {
        Map<String, Object> mapType = Map.of(
                "IsDeleted", "boolean"
        );

        ObjectNode payload = objectMapper.createObjectNode();
        payload.put("IsDeleted", true);

        SalesforceRecordMutation mutation = new SalesforceRecordMutation(
                SalesforceMutationType.CREATE,
                "001xx000003DHP0AAO",
                payload,
                new LinkedHashSet<>(List.of("IsDeleted")),
                Set.of(),
                "2026-03-28T09:15:30.123456+09:00",
                "2026-03-28T09:15:30.123456+09:00",
                "TIMESTAMP '2026-03-28 00:15:30.123456'",
                "TIMESTAMP '2026-03-28 00:15:30.123456'"
        );

        CapturingBoundRepository repository = new CapturingBoundRepository(
                DatabaseVendorStrategies.require(DatabaseVendor.ORACLE),
                0
        );

        SalesforceRecordMutationProcessor.MutationResult result = processor.apply(
                "ETL_USER",
                "Account",
                "Account",
                "Study Org",
                mapType,
                mutation,
                repository,
                "CDC"
        );

        assertEquals(0, result.updated());
        assertEquals(1, result.inserted());
        BoundBatchSql batchSql = repository.lastBoundInsert;
        assertNotNull(batchSql);
        List<SqlParameter> params = batchSql.parameterGroups().get(0);
        assertEquals("001xx000003DHP0AAO", params.get(0).value());
        assertEquals(1, params.get(1).value());
        assertEquals(Types.NUMERIC, params.get(1).sqlType());
        assertEquals(LocalDateTime.of(2026, 3, 28, 0, 15, 30, 123456000), params.get(2).value());
        assertEquals(Types.TIMESTAMP, params.get(2).sqlType());
        assertEquals(LocalDateTime.of(2026, 3, 28, 0, 15, 30, 123456000), params.get(3).value());
        assertTrue(batchSql.sql().contains("STUDY_ORG_ACCOUNT"), batchSql.sql());
    }

    @Test
    void boundUpdatePreservesNullTypedParametersForExplicitlyNulledFields() {
        Map<String, Object> mapType = new LinkedHashMap<>();
        mapType.put("IsDeleted", "boolean");
        mapType.put("LastSeenDate__c", "date");
        mapType.put("CutoffTime__c", "time");

        ObjectNode payload = objectMapper.createObjectNode();
        payload.putNull("IsDeleted");
        payload.putNull("LastSeenDate__c");
        payload.putNull("CutoffTime__c");

        SalesforceRecordMutation mutation = new SalesforceRecordMutation(
                SalesforceMutationType.UPDATE,
                "001xx000003DHP0AAO",
                payload,
                new LinkedHashSet<>(List.of("IsDeleted", "LastSeenDate__c", "CutoffTime__c")),
                new LinkedHashSet<>(List.of("IsDeleted", "LastSeenDate__c", "CutoffTime__c")),
                null,
                null,
                "null",
                "null"
        );

        CapturingBoundRepository repository = new CapturingBoundRepository(DatabaseVendorStrategies.defaultStrategy(), 1);

        SalesforceRecordMutationProcessor.MutationResult result = processor.apply(
                "routing",
                "Account",
                null,
                null,
                mapType,
                mutation,
                repository,
                "CDC"
        );

        assertEquals(1, result.updated());
        BoundSql update = repository.lastBoundUpdate;
        assertNotNull(update);
        assertEquals(new SqlParameter(null, Types.BOOLEAN), update.parameters().get(0));
        assertEquals(new SqlParameter(null, Types.DATE), update.parameters().get(1));
        assertEquals(new SqlParameter(null, Types.TIME), update.parameters().get(2));
    }

    private static final class CapturingBoundRepository implements SalesforceMutationRepositoryPort {
        private final DatabaseVendorStrategy strategy;
        private final int updateCount;
        private BoundSql lastBoundUpdate;
        private BoundBatchSql lastBoundInsert;

        private CapturingBoundRepository(DatabaseVendorStrategy strategy, int updateCount) {
            this.strategy = strategy;
            this.updateCount = updateCount;
        }

        @Override
        public boolean supportsBoundStatements() {
            return true;
        }

        @Override
        public DatabaseVendorStrategy vendorStrategy() {
            return strategy;
        }

        @Override
        public int insertObject(String upperQuery, List<String> listUnderQuery, String tailQuery) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int updateObject(StringBuilder strUpdate) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int deleteObject(StringBuilder strDelete) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int insertObject(BoundBatchSql batchSql) {
            this.lastBoundInsert = batchSql;
            return 1;
        }

        @Override
        public int updateObject(BoundSql boundSql) {
            this.lastBoundUpdate = boundSql;
            return updateCount;
        }

        @Override
        public int deleteObject(BoundSql boundSql) {
            throw new UnsupportedOperationException();
        }
    }
}
