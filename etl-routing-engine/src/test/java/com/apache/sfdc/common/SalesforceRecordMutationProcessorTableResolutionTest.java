package com.apache.sfdc.common;

import com.etlplatform.common.storage.database.DatabaseVendor;
import com.etlplatform.common.storage.database.OracleRoutingNaming;
import com.etlplatform.common.storage.database.sql.DatabaseVendorStrategies;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SalesforceRecordMutationProcessorTableResolutionTest {

    @Test
    void usesSameOraclePhysicalTableRuleAsInitialLoadWhenTargetTableMatchesSelectedObject() {
        SalesforceRecordMutationProcessor processor = new SalesforceRecordMutationProcessor();
        CapturingRepository repository = new CapturingRepository();

        ObjectNode payload = JsonNodeFactory.instance.objectNode();
        payload.put("Name", "Acme");
        SalesforceRecordMutation mutation = new SalesforceRecordMutation(
                SalesforceMutationType.CREATE,
                "001xx000003DHP0AAO",
                payload,
                Set.of("Name"),
                Set.of(),
                null,
                null,
                "CURRENT_TIMESTAMP",
                "CURRENT_TIMESTAMP"
        );

        SalesforceRecordMutationProcessor.MutationResult result = processor.apply(
                "ETL_USER",
                "Account",
                "Account",
                "Study Org",
                Map.of("Name", "string"),
                mutation,
                repository,
                "CDC"
        );

        String expectedTable = OracleRoutingNaming.buildTableName("Study Org", "Account");
        assertEquals(0, result.updated());
        assertEquals(1, result.inserted());
        assertTrue(repository.insertUpperQuery.contains(expectedTable), repository.insertUpperQuery);
    }

    private static final class CapturingRepository implements SalesforceMutationRepositoryPort {
        private String insertUpperQuery;

        @Override
        public int insertObject(String upperQuery, List<String> listUnderQuery, String tailQuery) {
            this.insertUpperQuery = upperQuery;
            return 1;
        }

        @Override
        public int updateObject(StringBuilder strUpdate) {
            return 0;
        }

        @Override
        public int deleteObject(StringBuilder strDelete) {
            return 0;
        }

        @Override
        public com.etlplatform.common.storage.database.sql.DatabaseVendorStrategy vendorStrategy() {
            return DatabaseVendorStrategies.require(DatabaseVendor.ORACLE);
        }
    }
}
