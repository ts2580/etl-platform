package com.apache.sfdc.common;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SalesforceHttpErrorHelperTest {

    @Test
    void truncateBodyNormalizesWhitespaceAndLimitsLength() {
        String input = " line1\n\nline2\t" + "x".repeat(1100);

        String truncated = SalesforceHttpErrorHelper.truncateBody(input);

        assertTrue(truncated.startsWith("line1 line2 "));
        assertTrue(truncated.endsWith("..."));
        assertTrue(truncated.length() <= 1003);
    }

    @Test
    void contextOmitsBlankValuesAndStripsQueryFromInstanceUrl() {
        Map<String, Object> context = SalesforceHttpErrorHelper.context(
                "STREAMING",
                "Account",
                "org-1",
                "https://example.my.salesforce.com/services/data?v=1&access_token=secret",
                42L
        );

        assertEquals("STREAMING", context.get("protocol"));
        assertEquals("Account", context.get("selectedObject"));
        assertEquals("org-1", context.get("orgKey"));
        assertEquals("https://example.my.salesforce.com/services/data", context.get("instanceUrl"));
        assertEquals(42L, context.get("targetStorageId"));
    }

    @Test
    void formatContextSkipsBlankEntries() {
        String formatted = SalesforceHttpErrorHelper.formatContext(Map.of(
                "protocol", "CDC",
                "selectedObject", "Contact",
                "blank", "   "
        ));

        assertTrue(formatted.contains("protocol=CDC"));
        assertTrue(formatted.contains("selectedObject=Contact"));
        assertFalse(formatted.contains("blank="));
    }
}
