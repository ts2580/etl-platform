package com.etlplatform.common.storage.database.sql;

import com.etlplatform.common.error.AppException;
import com.etlplatform.common.storage.database.DatabaseVendor;

import java.util.EnumMap;
import java.util.Map;

public final class DatabaseVendorStrategies {

    private static final DatabaseVendorStrategy DEFAULT = new MariaDbDatabaseVendorStrategy();
    private static final Map<DatabaseVendor, DatabaseVendorStrategy> STRATEGIES = strategies();

    private DatabaseVendorStrategies() {
    }

    public static DatabaseVendorStrategy defaultStrategy() {
        return DEFAULT;
    }

    public static DatabaseVendorStrategy require(DatabaseVendor vendor) {
        if (vendor == null) {
            return DEFAULT;
        }
        DatabaseVendorStrategy strategy = STRATEGIES.get(vendor);
        if (strategy == null) {
            throw new AppException("외부 DB vendor 전략이 아직 구현되지 않았어요. vendor=" + vendor);
        }
        return strategy;
    }

    private static Map<DatabaseVendor, DatabaseVendorStrategy> strategies() {
        Map<DatabaseVendor, DatabaseVendorStrategy> strategies = new EnumMap<>(DatabaseVendor.class);
        DatabaseVendorStrategy mariaDb = DEFAULT;
        strategies.put(DatabaseVendor.MARIADB, mariaDb);
        strategies.put(DatabaseVendor.MYSQL, new MySqlDatabaseVendorStrategy());
        strategies.put(DatabaseVendor.POSTGRESQL, new PostgreSqlDatabaseVendorStrategy());
        strategies.put(DatabaseVendor.ORACLE, new OracleDatabaseVendorStrategy());
        return strategies;
    }
}
