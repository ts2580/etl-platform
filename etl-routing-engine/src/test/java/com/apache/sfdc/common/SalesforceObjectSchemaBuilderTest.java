package com.apache.sfdc.common;

import com.etlplatform.common.storage.database.DatabaseVendor;
import com.etlplatform.common.storage.database.OracleRoutingNaming;
import com.etlplatform.common.storage.database.sql.DatabaseVendorStrategies;
import com.etlplatform.common.storage.database.sql.DatabaseVendorStrategy;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
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

        assertTrue(schema.ddl().contains("\"" + OracleRoutingNaming.buildTableName("Yuri Company", "Account") + "\""));
    }

    @Test
    void oracleBuildSchemaKeepsExplicitCustomTargetTable() throws Exception {
        ArrayNode fields = objectMapper.createArrayNode();
        fields.add(field("Id", "id", 18, "ID"));
        fields.add(field("Name", "string", 255, "Name"));

        SalesforceObjectSchemaBuilder.SchemaResult schema = SalesforceObjectSchemaBuilder.buildSchema(
                "ETL_USER",
                "Account",
                "CUSTOM_ACCOUNT_TABLE",
                "Yuri Company",
                fields,
                objectMapper,
                DatabaseVendorStrategies.require(com.etlplatform.common.storage.database.DatabaseVendor.ORACLE)
        );

        assertTrue(schema.ddl().contains("\"CUSTOM_ACCOUNT_TABLE\""));
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

    @Test
    void buildsVendorSpecificSchemaAndDropSqlForAllSupportedDatabases() throws Exception {
        ArrayNode fields = objectMapper.createArrayNode();
        fields.add(field("Id", "id", 18, "ID"));
        fields.add(field("Name", "string", 255, "Name"));
        fields.add(field("Description", "textarea", 32000, "Description"));
        fields.add(field("CreatedDate", "datetime", 0, "Created Date"));

        assertVendorSchema(DatabaseVendor.ORACLE,
                "\"ETL_USER\".\"" + OracleRoutingNaming.buildTableName("Yuri Company", "Account") + "\"",
                "VARCHAR2(18)",
                "CLOB",
                "TIMESTAMP(6)",
                true,
                "CASCADE CONSTRAINTS PURGE");

        assertVendorSchema(DatabaseVendor.POSTGRESQL,
                "\"etl_user\".\"Account\"",
                "VARCHAR(18)",
                "TEXT",
                "TIMESTAMP(6)",
                false,
                "DROP TABLE IF EXISTS \"etl_user\".\"Account\"");

        assertVendorSchema(DatabaseVendor.MARIADB,
                "`etl_user`.`Account`",
                "VARCHAR(18)",
                "TEXT",
                "DATETIME(6)",
                false,
                "DROP TABLE IF EXISTS `etl_user`.`Account`");

        assertVendorSchema(DatabaseVendor.MYSQL,
                "`etl_user`.`Account`",
                "VARCHAR(18)",
                "TEXT",
                "DATETIME(6)",
                false,
                "DROP TABLE IF EXISTS `etl_user`.`Account`");
    }

    private void assertVendorSchema(DatabaseVendor vendor,
                                    String expectedQualifiedName,
                                    String expectedIdType,
                                    String expectedTextareaType,
                                    String expectedDateTimeType,
                                    boolean oracle,
                                    String expectedDropSqlFragment) throws Exception {
        ArrayNode fields = objectMapper.createArrayNode();
        fields.add(field("Id", "id", 18, "ID"));
        fields.add(field("Name", "string", 255, "Name"));
        fields.add(field("Description", "textarea", 32000, "Description"));
        fields.add(field("CreatedDate", "datetime", 0, "Created Date"));

        DatabaseVendorStrategy strategy = DatabaseVendorStrategies.require(vendor);
        SalesforceObjectSchemaBuilder.SchemaResult schema = SalesforceObjectSchemaBuilder.buildSchema(
                "etl_user",
                "Account",
                "Yuri Company",
                fields,
                objectMapper,
                strategy
        );

        assertTrue(schema.ddl().contains(expectedQualifiedName));
        assertTrue(schema.ddl().contains("\"sfid\" " + expectedIdType)
                || schema.ddl().contains("`sfid` " + expectedIdType));
        assertTrue(schema.ddl().contains("\"Description\" " + expectedTextareaType)
                || schema.ddl().contains("`Description` " + expectedTextareaType));
        assertTrue(schema.ddl().contains("\"CreatedDate\" " + expectedDateTimeType)
                || schema.ddl().contains("`CreatedDate` " + expectedDateTimeType));

        if (oracle) {
            assertTrue(schema.ddl().startsWith("BEGIN EXECUTE IMMEDIATE"));
            assertFalse(schema.ddl().contains("IF NOT EXISTS"));
        } else {
            assertTrue(schema.ddl().contains("CREATE TABLE IF NOT EXISTS"));
        }

        String dropSql = SalesforceObjectSchemaBuilder.buildDropTableSql("etl_user", "Account", "Yuri Company", strategy);
        assertTrue(dropSql.contains(expectedDropSqlFragment), dropSql);
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
