package com.apache.sfdc.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

class SalesforceCompoundAddressMappingTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final SalesforceCdcPayloadMapper mapper = new SalesforceCdcPayloadMapper();
    private final SalesforceRecordMutationProcessor processor = new SalesforceRecordMutationProcessor();

    @Test
    void contactCompoundAddressFieldsAreExpandedIntoUpdateSql() throws Exception {
        String payload = """
                {
                  "ChangeEventHeader": {
                    "entityName": "Contact",
                    "recordIds": ["003WU00001MN6sfYAD"],
                    "changeType": "UPDATE",
                    "commitTimestamp": 1773848371000,
                    "changedFields": ["0x02000000", "3-0x1F", "4-0x1F"],
                    "nulledFields": []
                  },
                  "Name": {
                    "Salutation": "님",
                    "FirstName": "이",
                    "LastName": "유리유리"
                  },
                  "OtherAddress": {
                    "Street": "용마산로 252",
                    "City": "서울",
                    "State": "남한",
                    "PostalCode": "02252",
                    "Country": "한국",
                    "Latitude": null,
                    "Longitude": null,
                    "GeocodeAccuracy": null
                  },
                  "MailingAddress": {
                    "Street": "용마산로 252",
                    "City": "서울",
                    "State": "남한",
                    "PostalCode": "02252",
                    "Country": "한국",
                    "Latitude": null,
                    "Longitude": null,
                    "GeocodeAccuracy": null
                  },
                  "LastModifiedDate": 1773848371000
                }
                """;

        Map<String, Object> mapType = new LinkedHashMap<>();
        mapType.put("Name", "string");
        mapType.put("FirstName", "string");
        mapType.put("LastName", "string");
        mapType.put("OtherAddress", "string");
        mapType.put("OtherStreet", "string");
        mapType.put("OtherCity", "string");
        mapType.put("OtherState", "string");
        mapType.put("OtherPostalCode", "string");
        mapType.put("OtherCountry", "string");
        mapType.put("OtherLatitude", "double");
        mapType.put("OtherLongitude", "double");
        mapType.put("OtherGeocodeAccuracy", "string");
        mapType.put("MailingAddress", "string");
        mapType.put("MailingStreet", "string");
        mapType.put("MailingCity", "string");
        mapType.put("MailingState", "string");
        mapType.put("MailingPostalCode", "string");
        mapType.put("MailingCountry", "string");
        mapType.put("MailingLatitude", "double");
        mapType.put("MailingLongitude", "double");
        mapType.put("MailingGeocodeAccuracy", "string");
        mapType.put("LastModifiedDate", "datetime");

        Optional<SalesforceRecordMutation> optionalMutation = mapper.map(objectMapper.readTree(payload), mapType);
        assertTrue(optionalMutation.isPresent());

        SalesforceRecordMutation mutation = optionalMutation.get();
        assertTrue(mutation.targetFields().contains("Name"));
        assertTrue(mutation.targetFields().contains("FirstName"));
        assertTrue(mutation.targetFields().contains("LastName"));
        assertTrue(mutation.targetFields().contains("OtherAddress"));
        assertTrue(mutation.targetFields().contains("OtherStreet"));
        assertTrue(mutation.targetFields().contains("MailingAddress"));
        assertTrue(mutation.targetFields().contains("MailingStreet"));

        CapturingRepository repository = new CapturingRepository();
        SalesforceRecordMutationProcessor.MutationResult result = processor.apply(
                "org_studyorg",
                "Contact",
                mapType,
                mutation,
                repository,
                "test"
        );

        assertEquals(1, result.updated());
        String sql = repository.lastUpdateSql;
        assertNotNull(sql);
        assertTrue(sql.contains("`Name` = '님 유리유리 이'"), sql);
        assertTrue(sql.contains("`FirstName` = '이'"), sql);
        assertTrue(sql.contains("`LastName` = '유리유리'"), sql);
        assertTrue(sql.contains("`OtherAddress` = '한국 남한 서울 용마산로 252 02252'"), sql);
        assertTrue(sql.contains("`OtherStreet` = '용마산로 252'"), sql);
        assertTrue(sql.contains("`OtherCity` = '서울'"), sql);
        assertTrue(sql.contains("`OtherState` = '남한'"), sql);
        assertTrue(sql.contains("`OtherPostalCode` = '02252'"), sql);
        assertTrue(sql.contains("`OtherCountry` = '한국'"), sql);
        assertTrue(sql.contains("`MailingAddress` = '한국 남한 서울 용마산로 252 02252'"), sql);
        assertTrue(sql.contains("`MailingStreet` = '용마산로 252'"), sql);
        assertTrue(sql.contains("`MailingCity` = '서울'"), sql);
        assertTrue(sql.contains("`MailingState` = '남한'"), sql);
        assertTrue(sql.contains("`MailingPostalCode` = '02252'"), sql);
        assertTrue(sql.contains("`MailingCountry` = '한국'"), sql);
    }


    @Test
    void contactNamePartialUpdateDoesNotClearUnrelatedNameParts() throws Exception {
        String payload = """
                {
                  "ChangeEventHeader": {
                    "entityName": "Contact",
                    "recordIds": ["003WU00001MN6sfYAD"],
                    "changeType": "UPDATE",
                    "commitTimestamp": 1773849000000,
                    "changedFields": ["FirstName"],
                    "nulledFields": []
                  },
                  "Name": {
                    "FirstName": "성진!"
                  },
                  "LastModifiedDate": 1773849000000
                }
                """;

        Map<String, Object> mapType = new java.util.LinkedHashMap<>();
        mapType.put("Name", "string");
        mapType.put("FirstName", "string");
        mapType.put("LastName", "string");
        mapType.put("LastModifiedDate", "datetime");

        Optional<SalesforceRecordMutation> optionalMutation = mapper.map(objectMapper.readTree(payload), mapType);
        assertTrue(optionalMutation.isPresent());

        SalesforceRecordMutation mutation = optionalMutation.get();
        assertTrue(mutation.targetFields().contains("FirstName"));
        assertTrue(mutation.targetFields().contains("Name"));
        assertFalse(mutation.targetFields().contains("LastName"));

        CapturingRepository repository = new CapturingRepository();
        SalesforceRecordMutationProcessor.MutationResult result = processor.apply(
                "org_studyorg",
                "Contact",
                mapType,
                mutation,
                repository,
                "test"
        );

        assertEquals(1, result.updated());
        String sql = repository.lastUpdateSql;
        assertNotNull(sql);
        assertTrue(sql.contains("`FirstName` = '성진!'"), sql);
        assertFalse(sql.contains("`LastName` ="), sql);
    }

    @Test
    void contactCompoundChangeFieldNameOnlyWithFirstNameDoesNotClearLastName() throws Exception {
        String payload = """
                {
                  "ChangeEventHeader": {
                    "entityName": "Contact",
                    "recordIds": ["003WU00001MN6sfYAD"],
                    "changeType": "UPDATE",
                    "commitTimestamp": 1773849100000,
                    "changedFields": ["Name"],
                    "nulledFields": []
                  },
                  "Name": {
                    "FirstName": "성진!"
                  },
                  "LastModifiedDate": 1773849100000
                }
                """;

        Map<String, Object> mapType = new java.util.LinkedHashMap<>();
        mapType.put("Name", "string");
        mapType.put("FirstName", "string");
        mapType.put("LastName", "string");
        mapType.put("LastModifiedDate", "datetime");

        Optional<SalesforceRecordMutation> optionalMutation = mapper.map(objectMapper.readTree(payload), mapType);
        assertTrue(optionalMutation.isPresent());

        SalesforceRecordMutation mutation = optionalMutation.get();
        assertTrue(mutation.targetFields().contains("FirstName"));
        assertFalse(mutation.targetFields().contains("LastName"));

        CapturingRepository repository = new CapturingRepository();
        SalesforceRecordMutationProcessor.MutationResult result = processor.apply(
                "org_studyorg",
                "Contact",
                mapType,
                mutation,
                repository,
                "test"
        );

        assertEquals(1, result.updated());
        String sql = repository.lastUpdateSql;
        assertNotNull(sql);
        assertTrue(sql.contains("`FirstName` = '성진!'") );
        assertFalse(sql.contains("`LastName` ="));
    }

    @Test
    void contactCompoundChangeFieldNameOnlyWithLastNameDoesNotClearFirstName() throws Exception {
        String payload = """
                {
                  "ChangeEventHeader": {
                    "entityName": "Contact",
                    "recordIds": ["003WU00001MN6sfYAD"],
                    "changeType": "UPDATE",
                    "commitTimestamp": 1773849101000,
                    "changedFields": ["Name"],
                    "nulledFields": []
                  },
                  "Name": {
                    "LastName": "성"
                  },
                  "LastModifiedDate": 1773849101000
                }
                """;

        Map<String, Object> mapType = new java.util.LinkedHashMap<>();
        mapType.put("Name", "string");
        mapType.put("FirstName", "string");
        mapType.put("LastName", "string");
        mapType.put("LastModifiedDate", "datetime");

        Optional<SalesforceRecordMutation> optionalMutation = mapper.map(objectMapper.readTree(payload), mapType);
        assertTrue(optionalMutation.isPresent());

        SalesforceRecordMutation mutation = optionalMutation.get();
        assertTrue(mutation.targetFields().contains("LastName"));
        assertFalse(mutation.targetFields().contains("FirstName"));

        CapturingRepository repository = new CapturingRepository();
        SalesforceRecordMutationProcessor.MutationResult result = processor.apply(
                "org_studyorg",
                "Contact",
                mapType,
                mutation,
                repository,
                "test"
        );

        assertEquals(1, result.updated());
        String sql = repository.lastUpdateSql;
        assertNotNull(sql);
        assertTrue(sql.contains("`LastName` = '성'"));
        assertFalse(sql.contains("`FirstName` ="));
    }

    @Test
    void accountCompoundAddressFieldsAreExpandedIntoUpdateSql() throws Exception {
        String payload = """
                {
                  "ChangeEventHeader": {
                    "entityName": "Account",
                    "recordIds": ["001WU00001V6kqfYAB"],
                    "changeType": "UPDATE",
                    "commitTimestamp": 1773849233000,
                    "changedFields": ["0x400002", "4-0x1F", "5-0x1F"],
                    "nulledFields": []
                  },
                  "BillingAddress": {
                    "Street": "3",
                    "City": "2",
                    "State": "1",
                    "PostalCode": "4",
                    "Country": "5",
                    "Latitude": null,
                    "Longitude": null,
                    "GeocodeAccuracy": null
                  },
                  "ShippingAddress": {
                    "Street": "8",
                    "City": "7",
                    "State": "6",
                    "PostalCode": "9",
                    "Country": "10",
                    "Latitude": null,
                    "Longitude": null,
                    "GeocodeAccuracy": null
                  },
                  "LastModifiedDate": 1773849233000
                }
                """;

        Map<String, Object> mapType = new LinkedHashMap<>();
        mapType.put("BillingAddress", "string");
        mapType.put("BillingStreet", "string");
        mapType.put("BillingCity", "string");
        mapType.put("BillingState", "string");
        mapType.put("BillingPostalCode", "string");
        mapType.put("BillingCountry", "string");
        mapType.put("BillingLatitude", "double");
        mapType.put("BillingLongitude", "double");
        mapType.put("BillingGeocodeAccuracy", "string");
        mapType.put("ShippingAddress", "string");
        mapType.put("ShippingStreet", "string");
        mapType.put("ShippingCity", "string");
        mapType.put("ShippingState", "string");
        mapType.put("ShippingPostalCode", "string");
        mapType.put("ShippingCountry", "string");
        mapType.put("ShippingLatitude", "double");
        mapType.put("ShippingLongitude", "double");
        mapType.put("ShippingGeocodeAccuracy", "string");
        mapType.put("LastModifiedDate", "datetime");

        Optional<SalesforceRecordMutation> optionalMutation = mapper.map(objectMapper.readTree(payload), mapType);
        assertTrue(optionalMutation.isPresent());

        SalesforceRecordMutation mutation = optionalMutation.get();
        assertTrue(mutation.targetFields().contains("BillingAddress"));
        assertTrue(mutation.targetFields().contains("BillingStreet"));
        assertTrue(mutation.targetFields().contains("ShippingAddress"));
        assertTrue(mutation.targetFields().contains("ShippingStreet"));

        CapturingRepository repository = new CapturingRepository();
        SalesforceRecordMutationProcessor.MutationResult result = processor.apply(
                "org_studyorg",
                "Account",
                mapType,
                mutation,
                repository,
                "test"
        );

        assertEquals(1, result.updated());
        String sql = repository.lastUpdateSql;
        assertNotNull(sql);
        assertTrue(sql.contains("`BillingAddress` = '5 1 2 3 4'"), sql);
        assertTrue(sql.contains("`BillingStreet` = '3'"), sql);
        assertTrue(sql.contains("`BillingCity` = '2'"), sql);
        assertTrue(sql.contains("`BillingState` = '1'"), sql);
        assertTrue(sql.contains("`BillingPostalCode` = '4'"), sql);
        assertTrue(sql.contains("`BillingCountry` = '5'"), sql);
        assertTrue(sql.contains("`ShippingAddress` = '10 6 7 8 9'"), sql);
        assertTrue(sql.contains("`ShippingStreet` = '8'"), sql);
        assertTrue(sql.contains("`ShippingCity` = '7'"), sql);
        assertTrue(sql.contains("`ShippingState` = '6'"), sql);
        assertTrue(sql.contains("`ShippingPostalCode` = '9'"), sql);
        assertTrue(sql.contains("`ShippingCountry` = '10'"), sql);
    }

    private static class CapturingRepository implements SalesforceMutationRepositoryPort {
        private String lastUpdateSql;

        @Override
        public int insertObject(String upperQuery, List<String> listUnderQuery, String tailQuery) {
            return 0;
        }

        @Override
        public int updateObject(StringBuilder strUpdate) {
            this.lastUpdateSql = strUpdate.toString();
            return 1;
        }

        @Override
        public int deleteObject(StringBuilder strDelete) {
            return 0;
        }
    }
}
