package com.etlplatform.common.storage.database;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled("Manual verification test")
class DatabaseConnectionTestSupportManualTest {

    @Test
    void mysqlConnectionTestShouldNotAppendDatabaseName() {
        DatabaseStorageRegistrationRequest request = new DatabaseStorageRegistrationRequest();
        request.setName("manual-mysql-test");
        request.setVendor(DatabaseVendor.MYSQL);
        request.setAuthMethod(DatabaseAuthMethod.PASSWORD);
        request.setJdbcUrl("www.sfdevhub.com");
        request.setPort(13306);
        request.setUsername("etl");
        request.setPassword(System.getenv("MYSQL_TEST_PASSWORD"));
        request.setDatabaseName("etl_sfdc");

        new DatabaseConnectionTestSupport().test(request);
    }
}
