package com.apache.sfdc.common;

import com.etlplatform.common.storage.database.sql.DatabaseVendorStrategy;
import com.etlplatform.common.storage.database.sql.SqlParameter;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Component
public class SalesforceBulkResultMapper {

    public List<String> toInsertRows(Iterable<Map<String, String>> records,
                                     SalesforceObjectSchemaBuilder.SchemaResult schemaResult,
                                     DatabaseVendorStrategy strategy) {
        List<String> rows = new ArrayList<>();
        for (Map<String, String> record : records) {
            rows.add(SalesforceObjectSchemaBuilder.buildInsertValues(record, schemaResult.fields(), schemaResult.mapType(), strategy));
        }
        return rows;
    }

    public List<List<SqlParameter>> toParameterGroups(Iterable<Map<String, String>> records,
                                                      SalesforceObjectSchemaBuilder.SchemaResult schemaResult,
                                                      DatabaseVendorStrategy strategy) {
        List<List<SqlParameter>> groups = new ArrayList<>();
        for (Map<String, String> record : records) {
            groups.add(SalesforceObjectSchemaBuilder.buildPreparedInsertParameters(record, schemaResult, strategy));
        }
        return groups;
    }
}
