package com.apache.sfdc.common;

import com.etlplatform.common.storage.database.sql.DatabaseVendorStrategies;
import com.etlplatform.common.storage.database.sql.SqlParameter;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SalesforceBulkResultMapperTest {

    private final SalesforceBulkResultMapper mapper = new SalesforceBulkResultMapper();

    @Test
    void mapsCsvRowToInsertRowsAndPreparedParameters() throws Exception {
        SalesforceObjectSchemaBuilder.SchemaResult schemaResult = SalesforceObjectSchemaBuilder.buildSchema(
                "routing",
                "Account",
                new ObjectMapper().readTree("""
                        [
                          {"name":"Id","type":"id","label":"ID","length":18},
                          {"name":"Name","type":"string","label":"Name","length":255},
                          {"name":"AnnualRevenue","type":"double","label":"Revenue","length":18},
                          {"name":"IsDeleted","type":"boolean","label":"Deleted","length":1},
                          {"name":"LastModifiedDate","type":"datetime","label":"Modified","length":0}
                        ]
                        """)
        );

        List<Map<String, String>> rows = List.of(Map.of(
                "Id", "001000000000001AAA",
                "Name", "Acme",
                "AnnualRevenue", "123.45",
                "IsDeleted", "true",
                "LastModifiedDate", "2026-03-28T00:00:00.000+0000"
        ));

        List<String> insertRows = mapper.toInsertRows(rows, schemaResult, DatabaseVendorStrategies.defaultStrategy());
        List<List<SqlParameter>> parameterGroups = mapper.toParameterGroups(rows, schemaResult, DatabaseVendorStrategies.defaultStrategy());

        assertEquals(1, insertRows.size());
        assertEquals(true, insertRows.get(0).contains("'Acme'"));
        assertEquals(1, parameterGroups.size());
        assertEquals("001000000000001AAA", parameterGroups.get(0).get(0).value());
        assertEquals("Acme", parameterGroups.get(0).get(1).value());
        assertEquals("123.45", parameterGroups.get(0).get(2).value().toString());
        assertEquals(true, parameterGroups.get(0).get(3).value());
    }
}
