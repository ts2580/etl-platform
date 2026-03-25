package com.apache.sfdc.common;

import com.etlplatform.common.storage.database.sql.DatabaseVendorStrategies;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

class SalesforceObjectSchemaBuilderTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void oracleBuildSchemaUsesSamePhysicalTableNameRule() throws Exception {
        ArrayNode fields = objectMapper.createArrayNode();
        fields.add(field("Id", "id", 18, "ID"));
        fields.add(field("Name", "string", 255, "Name"));

        SalesforceObjectSchemaBuilder.SchemaResult schema = SalesforceObjectSchemaBuilder.buildSchema(
                "ETL_USER",
                "Account",
                "Yuri Company",
                fields,
                objectMapper,
                DatabaseVendorStrategies.require(com.etlplatform.common.storage.database.DatabaseVendor.ORACLE)
        );

        assertTrue(schema.ddl().contains("\"YURI_COMPANY_ACCOUNT\""));
    }

    @Test
    void oracleDropSqlUsesPrefixedPhysicalTableNameAndCascadePurge() {
        String sql = SalesforceObjectSchemaBuilder.buildDropTableSql(
                "ETL_USER",
                "Account",
                "Yuri Company",
                DatabaseVendorStrategies.require(com.etlplatform.common.storage.database.DatabaseVendor.ORACLE)
        );

        assertTrue(sql.contains("YURI_COMPANY_ACCOUNT"));
        assertTrue(sql.contains("CASCADE CONSTRAINTS PURGE"));
    }

    private ObjectNode field(String name, String type, int length, String label) {
        ObjectNode node = objectMapper.createObjectNode();
        node.put("name", name);
        node.put("type", type);
        node.put("length", length);
        node.put("label", label);
        return node;
    }
}
