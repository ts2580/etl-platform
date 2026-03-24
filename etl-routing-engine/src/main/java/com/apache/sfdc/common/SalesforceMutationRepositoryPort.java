package com.apache.sfdc.common;

import com.etlplatform.common.storage.database.sql.BoundBatchSql;
import com.etlplatform.common.storage.database.sql.BoundSql;
import com.etlplatform.common.storage.database.sql.DatabaseVendorStrategies;
import com.etlplatform.common.storage.database.sql.DatabaseVendorStrategy;

import java.util.List;

public interface SalesforceMutationRepositoryPort {
    int insertObject(String upperQuery, List<String> listUnderQuery, String tailQuery);

    int updateObject(StringBuilder strUpdate);

    int deleteObject(StringBuilder strDelete);

    default boolean supportsBoundStatements() {
        return false;
    }

    default DatabaseVendorStrategy vendorStrategy() {
        return DatabaseVendorStrategies.defaultStrategy();
    }

    default int insertObject(BoundBatchSql batchSql) {
        throw new UnsupportedOperationException("Bound insert is not supported");
    }

    default int updateObject(BoundSql boundSql) {
        throw new UnsupportedOperationException("Bound update is not supported");
    }

    default int deleteObject(BoundSql boundSql) {
        throw new UnsupportedOperationException("Bound delete is not supported");
    }
}
