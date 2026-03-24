package com.etlplatform.common.storage.database.sql;

import com.etlplatform.common.storage.database.DatabaseVendor;

public final class MariaDbDatabaseVendorStrategy extends MySqlDatabaseVendorStrategy {

    @Override
    public DatabaseVendor vendor() {
        return DatabaseVendor.MARIADB;
    }
}
