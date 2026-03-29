package com.apache.sfdc.common;

import com.etlplatform.common.storage.database.DatabaseVendor;
import com.etlplatform.common.storage.database.sql.DatabaseVendorStrategies;
import com.etlplatform.common.storage.database.sql.SqlParameter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class SalesforceObjectSchemaBuilderTypeMappingRegressionTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void preparedInsertParametersNormalizeTimezonePrecisionAndBooleansForDefaultStrategy() throws Exception {
        SalesforceObjectSchemaBuilder.SchemaResult schemaResult = SalesforceObjectSchemaBuilder.buildSchema(
                "routing",
                "Account",
                objectMapper.readTree("""
                        [
                          {"name":"Id","type":"id","label":"ID","length":18},
                          {"name":"IsDeleted","type":"boolean","label":"Deleted","length":1},
                          {"name":"AnnualRevenue","type":"currency","label":"Revenue","length":18},
                          {"name":"CloseDate","type":"date","label":"Close Date","length":0},
                          {"name":"DailyCutoff","type":"time","label":"Cutoff","length":0},
                          {"name":"LastModifiedDate","type":"datetime","label":"Modified","length":0}
                        ]
                        """)
        );

        ObjectNode record = objectMapper.createObjectNode();
        record.put("Id", "001000000000001AAA");
        record.put("IsDeleted", true);
        record.put("AnnualRevenue", "12345678901234567890.123456");
        record.put("CloseDate", "2026-03-28T09:15:30+09:00");
        record.put("DailyCutoff", "2026-03-28T09:15:30.123456+09:00");
        record.put("LastModifiedDate", "2026-03-28T09:15:30.123456+09:00");

        List<SqlParameter> parameters = SalesforceObjectSchemaBuilder.buildPreparedInsertParameters(
                record,
                schemaResult,
                DatabaseVendorStrategies.defaultStrategy()
        );

        assertEquals(8, parameters.size());
        assertEquals(Boolean.TRUE, parameters.get(1).value());
        assertEquals(Types.BOOLEAN, parameters.get(1).sqlType());
        assertEquals(new BigDecimal("12345678901234567890.123456"), parameters.get(2).value());
        assertEquals(LocalDate.of(2026, 3, 28), parameters.get(3).value());
        assertEquals(Types.DATE, parameters.get(3).sqlType());
        assertEquals(LocalTime.of(0, 15, 30, 123456000), parameters.get(4).value());
        assertEquals(Types.TIME, parameters.get(4).sqlType());
        assertEquals(LocalDateTime.of(2026, 3, 28, 0, 15, 30, 123456000), parameters.get(5).value());
        assertEquals(LocalDateTime.of(2026, 3, 28, 0, 15, 30, 123456000), parameters.get(6).value());
        assertEquals(LocalDateTime.of(2026, 3, 28, 0, 15, 30, 123456000), parameters.get(7).value());
        assertEquals(Types.TIMESTAMP, parameters.get(5).sqlType());
    }

    @Test
    void preparedInsertParametersUseOracleSpecificTypesForBooleanTimeAndTextareaNulls() throws Exception {
        SalesforceObjectSchemaBuilder.SchemaResult schemaResult = SalesforceObjectSchemaBuilder.buildSchema(
                "ETL_USER",
                "Account",
                "Study Org",
                objectMapper.readTree("""
                        [
                          {"name":"Id","type":"id","label":"ID","length":18},
                          {"name":"IsDeleted","type":"boolean","label":"Deleted","length":1},
                          {"name":"Notes","type":"textarea","label":"Notes","length":32000},
                          {"name":"DailyCutoff","type":"time","label":"Cutoff","length":0},
                          {"name":"LastModifiedDate","type":"datetime","label":"Modified","length":0}
                        ]
                        """),
                objectMapper,
                DatabaseVendorStrategies.require(DatabaseVendor.ORACLE)
        );

        ObjectNode record = objectMapper.createObjectNode();
        record.put("Id", "001000000000001AAA");
        record.put("IsDeleted", true);
        record.putNull("Notes");
        record.put("DailyCutoff", "2026-03-28T09:15:30.123456+09:00");
        record.put("LastModifiedDate", 1774628130123L);

        List<SqlParameter> parameters = SalesforceObjectSchemaBuilder.buildPreparedInsertParameters(
                record,
                schemaResult,
                DatabaseVendorStrategies.require(DatabaseVendor.ORACLE)
        );

        assertEquals(7, parameters.size());
        assertEquals(1, parameters.get(1).value());
        assertEquals(Types.NUMERIC, parameters.get(1).sqlType());
        assertEquals(null, parameters.get(2).value());
        assertEquals(Types.CLOB, parameters.get(2).sqlType());
        assertEquals("00:15:30.123456", parameters.get(3).value());
        assertEquals(Types.VARCHAR, parameters.get(3).sqlType());
        assertInstanceOf(LocalDateTime.class, parameters.get(4).value());
        assertEquals(Types.TIMESTAMP, parameters.get(4).sqlType());
        assertInstanceOf(LocalDateTime.class, parameters.get(5).value());
        assertInstanceOf(LocalDateTime.class, parameters.get(6).value());
    }

    @Test
    void preparedInsertParametersKeepNullTypedBindingsAcrossTemporalAndBooleanFields() throws Exception {
        SalesforceObjectSchemaBuilder.SchemaResult schemaResult = SalesforceObjectSchemaBuilder.buildSchema(
                "routing",
                "Account",
                objectMapper.readTree("""
                        [
                          {"name":"Id","type":"id","label":"ID","length":18},
                          {"name":"IsDeleted","type":"boolean","label":"Deleted","length":1},
                          {"name":"CloseDate","type":"date","label":"Close Date","length":0},
                          {"name":"DailyCutoff","type":"time","label":"Cutoff","length":0},
                          {"name":"Description","type":"textarea","label":"Description","length":32000},
                          {"name":"LastModifiedDate","type":"datetime","label":"Modified","length":0}
                        ]
                        """)
        );

        ObjectNode record = objectMapper.createObjectNode();
        record.put("Id", "001000000000001AAA");
        record.putNull("IsDeleted");
        record.putNull("CloseDate");
        record.putNull("DailyCutoff");
        record.putNull("Description");
        record.putNull("LastModifiedDate");

        List<SqlParameter> parameters = SalesforceObjectSchemaBuilder.buildPreparedInsertParameters(
                record,
                schemaResult,
                DatabaseVendorStrategies.defaultStrategy()
        );

        assertEquals(new SqlParameter(null, Types.BOOLEAN), parameters.get(1));
        assertEquals(new SqlParameter(null, Types.DATE), parameters.get(2));
        assertEquals(new SqlParameter(null, Types.TIME), parameters.get(3));
        assertEquals(new SqlParameter(null, Types.VARCHAR), parameters.get(4));
        assertEquals(new SqlParameter(null, Types.TIMESTAMP), parameters.get(5));
        assertEquals(new SqlParameter(null, Types.TIMESTAMP), parameters.get(6));
        assertEquals(new SqlParameter(null, Types.TIMESTAMP), parameters.get(7));
    }
}
