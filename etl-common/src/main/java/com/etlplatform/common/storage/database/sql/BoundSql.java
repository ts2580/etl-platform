package com.etlplatform.common.storage.database.sql;

import java.util.List;

public record BoundSql(String sql, List<SqlParameter> parameters) {
}
