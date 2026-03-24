package com.etlplatform.common.storage.database.sql;

import java.util.List;

public record BoundBatchSql(String sql, List<List<SqlParameter>> parameterGroups) {
}
