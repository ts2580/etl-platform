package com.apache.sfdc.common;

import com.etlplatform.common.storage.database.DatabaseVendor;
import com.etlplatform.common.storage.database.sql.DatabaseVendorStrategy;

import java.util.Map;

public final class SalesforceTargetTableResolver {

    private SalesforceTargetTableResolver() {
    }

    public static String resolve(String targetSchema,
                                 String selectedObject,
                                 String targetTable,
                                 String orgName,
                                 DatabaseVendorStrategy strategy) {
        return resolveTargetTable(targetSchema, selectedObject, targetTable, orgName, strategy).physicalTableName();
    }

    public static ResolvedTargetTable resolveTargetTable(String targetSchema,
                                                         String selectedObject,
                                                         String targetTable,
                                                         String orgName,
                                                         DatabaseVendorStrategy strategy) {
        if (selectedObject == null || selectedObject.isBlank()) {
            throw new IllegalArgumentException("selectedObject is required");
        }

        String logicalTableName = (targetTable != null && !targetTable.isBlank()) ? targetTable : selectedObject;
        String physicalTableName = shouldUseGeneratedOracleName(selectedObject, logicalTableName, strategy)
                ? SalesforceObjectSchemaBuilder.resolvePhysicalTableName(targetSchema, selectedObject, orgName, strategy)
                : logicalTableName;

        return new ResolvedTargetTable(selectedObject, logicalTableName, physicalTableName);
    }

    public static String resolveAndStore(Map<String, String> mapProperty,
                                         String selectedObject,
                                         DatabaseVendorStrategy strategy) {
        if (mapProperty == null) {
            return resolve(null, selectedObject, null, null, strategy);
        }
        ResolvedTargetTable resolved = resolveTargetTable(
                mapProperty.get("targetSchema"),
                selectedObject,
                mapProperty.get("targetTable"),
                mapProperty.get("orgName"),
                strategy
        );
        mapProperty.put("targetTable", resolved.physicalTableName());
        return resolved.physicalTableName();
    }

    private static boolean shouldUseGeneratedOracleName(String selectedObject,
                                                        String logicalTableName,
                                                        DatabaseVendorStrategy strategy) {
        return strategy != null
                && strategy.vendor() == DatabaseVendor.ORACLE
                && selectedObject.equalsIgnoreCase(logicalTableName);
    }

    public record ResolvedTargetTable(String selectedObject,
                                      String logicalTableName,
                                      String physicalTableName) {
    }
}
