package com.apache.sfdc.common;

import com.apache.sfdc.storage.service.ExternalStorageRoutingJdbcExecutor;
import com.etlplatform.common.storage.database.DatabaseVendor;
import com.etlplatform.common.storage.database.OracleRoutingNaming;
import com.etlplatform.common.storage.database.sql.DatabaseVendorStrategies;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SalesforceInitialLoadServiceImplTest {

    @Mock
    private SalesforceBulkQueryClient bulkQueryClient;

    @Mock
    private SalesforceBulkResultMapper bulkResultMapper;

    @Mock
    private ExternalStorageRoutingJdbcExecutor routingJdbcExecutor;

    @Test
    void keepsExplicitTargetTableForOracleInitialLoad() throws Exception {
        SalesforceInitialLoadServiceImpl service = new SalesforceInitialLoadServiceImpl(
                bulkQueryClient,
                bulkResultMapper,
                routingJdbcExecutor
        );
        ReflectionTestUtils.setField(service, "chunkSize", 2000);
        ReflectionTestUtils.setField(service, "pollIntervalMillis", 1L);
        ReflectionTestUtils.setField(service, "timeoutMillis", 1000L);

        when(routingJdbcExecutor.resolveStrategy(7L)).thenReturn(DatabaseVendorStrategies.require(DatabaseVendor.ORACLE));
        when(routingJdbcExecutor.usesExternalStorage(7L)).thenReturn(false);
        when(bulkQueryClient.createQueryJob(anyString(), anyString(), anyString(), anyString())).thenReturn("750xx0000000001AAA");
        when(bulkQueryClient.fetchResults(anyString(), anyString(), anyString(), anyString(), any(), any()))
                .thenReturn(new SalesforceBulkQueryClient.BulkQueryPage("Id,Name\n001,Acme\n", null, true));
        when(bulkResultMapper.toInsertRows(anyList(), any(), any())).thenReturn(List.of("('001','Acme')"));
        when(routingJdbcExecutor.insert(eq("CDC"), anyString(), anyList(), anyString(), eq(7L))).thenReturn(1);

        Map<String, String> mapProperty = new LinkedHashMap<>();
        mapProperty.put("targetSchema", "config");
        mapProperty.put("targetTable", "CUSTOM_ACCOUNT_TABLE");
        mapProperty.put("orgName", "Study Org");

        SalesforceObjectSchemaBuilder.SchemaResult schemaResult = new SalesforceObjectSchemaBuilder.SchemaResult(
                "ddl",
                Map.of("Name", "string"),
                List.of("Name"),
                "SELECT Name FROM Account",
                "SELECT Name FROM Account"
        );

        service.load("CDC", mapProperty, "token", "https://example.salesforce.com", "61.0", "Account", schemaResult, 7L);

        ArgumentCaptor<String> upperQueryCaptor = ArgumentCaptor.forClass(String.class);
        verify(routingJdbcExecutor).insert(eq("CDC"), upperQueryCaptor.capture(), anyList(), anyString(), eq(7L));
        assertTrue(upperQueryCaptor.getValue().contains("CUSTOM_ACCOUNT_TABLE"), upperQueryCaptor.getValue());
    }

    @Test
    void fallsBackToOraclePhysicalTableNameWhenTargetTableMatchesSelectedObject() throws Exception {
        SalesforceInitialLoadServiceImpl service = new SalesforceInitialLoadServiceImpl(
                bulkQueryClient,
                bulkResultMapper,
                routingJdbcExecutor
        );
        ReflectionTestUtils.setField(service, "chunkSize", 2000);
        ReflectionTestUtils.setField(service, "pollIntervalMillis", 1L);
        ReflectionTestUtils.setField(service, "timeoutMillis", 1000L);

        when(routingJdbcExecutor.resolveStrategy(9L)).thenReturn(DatabaseVendorStrategies.require(DatabaseVendor.ORACLE));
        when(routingJdbcExecutor.usesExternalStorage(9L)).thenReturn(false);
        when(bulkQueryClient.createQueryJob(anyString(), anyString(), anyString(), anyString())).thenReturn("750xx0000000002AAA");
        when(bulkQueryClient.fetchResults(anyString(), anyString(), anyString(), anyString(), any(), any()))
                .thenReturn(new SalesforceBulkQueryClient.BulkQueryPage("Id,Name\n001,Acme\n", null, true));
        when(bulkResultMapper.toInsertRows(anyList(), any(), any())).thenReturn(List.of("('001','Acme')"));
        when(routingJdbcExecutor.insert(eq("STREAMING"), anyString(), anyList(), anyString(), eq(9L))).thenReturn(1);

        Map<String, String> mapProperty = new LinkedHashMap<>();
        mapProperty.put("targetSchema", "config");
        mapProperty.put("targetTable", "Account");
        mapProperty.put("orgName", "Study Org");

        SalesforceObjectSchemaBuilder.SchemaResult schemaResult = new SalesforceObjectSchemaBuilder.SchemaResult(
                "ddl",
                Map.of("Name", "string"),
                List.of("Name"),
                "SELECT Name FROM Account",
                "SELECT Name FROM Account"
        );

        service.load("STREAMING", mapProperty, "token", "https://example.salesforce.com", "61.0", "Account", schemaResult, 9L);

        String expectedPhysicalTable = OracleRoutingNaming.buildTableName("Study Org", "Account");
        ArgumentCaptor<String> upperQueryCaptor = ArgumentCaptor.forClass(String.class);
        verify(routingJdbcExecutor).insert(eq("STREAMING"), upperQueryCaptor.capture(), anyList(), anyString(), eq(9L));
        assertTrue(upperQueryCaptor.getValue().contains(expectedPhysicalTable), upperQueryCaptor.getValue());
        assertTrue(expectedPhysicalTable.equals(mapProperty.get("targetTable")), mapProperty.toString());
    }
}
