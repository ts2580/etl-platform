package com.apache.sfdc.common;

import com.etlplatform.common.storage.database.DatabaseVendor;
import com.etlplatform.common.storage.database.OracleRoutingNaming;
import com.etlplatform.common.storage.database.sql.DatabaseVendorStrategies;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SalesforceTargetTableResolverTest {

    @Test
    void keepsExplicitCustomTargetTableForOracle() {
        String resolved = SalesforceTargetTableResolver.resolve(
                "ETL_USER",
                "Account",
                "CUSTOM_ACCOUNT_TABLE",
                "Study Org",
                DatabaseVendorStrategies.require(DatabaseVendor.ORACLE)
        );

        assertEquals("CUSTOM_ACCOUNT_TABLE", resolved);
    }

    @Test
    void normalizesOracleDefaultTargetTableToGeneratedPhysicalNameAndStoresIt() {
        Map<String, String> mapProperty = new LinkedHashMap<>();
        mapProperty.put("targetSchema", "ETL_USER");
        mapProperty.put("targetTable", "Account");
        mapProperty.put("orgName", "Study Org");

        String resolved = SalesforceTargetTableResolver.resolveAndStore(
                mapProperty,
                "Account",
                DatabaseVendorStrategies.require(DatabaseVendor.ORACLE)
        );

        String expected = OracleRoutingNaming.buildTableName("Study Org", "Account");
        assertEquals(expected, resolved);
        assertEquals(expected, mapProperty.get("targetTable"));
    }

    @Test
    void exposesLogicalAndPhysicalTargetTableSeparatelyForOracleDefaults() {
        SalesforceTargetTableResolver.ResolvedTargetTable resolved = SalesforceTargetTableResolver.resolveTargetTable(
                "ETL_USER",
                "Account",
                "Account",
                "Study Org",
                DatabaseVendorStrategies.require(DatabaseVendor.ORACLE)
        );

        assertEquals("Account", resolved.selectedObject());
        assertEquals("Account", resolved.logicalTableName());
        assertEquals(OracleRoutingNaming.buildTableName("Study Org", "Account"), resolved.physicalTableName());
    }

    @Test
    void fallsBackToSelectedObjectForNonOracleWhenTargetTableIsBlank() {
        String resolved = SalesforceTargetTableResolver.resolve(
                "etl_user",
                "Account",
                "   ",
                "Study Org",
                DatabaseVendorStrategies.require(DatabaseVendor.POSTGRESQL)
        );

        assertEquals("Account", resolved);
    }
}
