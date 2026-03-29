package com.apache.sfdc.storage.service;

import com.etlplatform.common.storage.database.sql.SqlParameter;
import com.etlplatform.common.storage.database.DatabaseVendor;

import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.math.BigDecimal;
import java.util.List;

public final class RoutingPreparedStatementBinder {

    private RoutingPreparedStatementBinder() {
        // no-op
    }

    public static void bind(PreparedStatement statement, List<SqlParameter> parameters, DatabaseVendor vendor) throws Exception {
        for (int i = 0; i < parameters.size(); i++) {
            SqlParameter parameter = parameters.get(i);
            int index = i + 1;
            bindParameter(statement, index, parameter, vendor);
        }
    }

    private static void bindParameter(PreparedStatement statement, int index, SqlParameter parameter, DatabaseVendor vendor) throws Exception {
        Object value = parameter.value();
        int sqlType = parameter.sqlType();

        if (value == null) {
            bindNull(statement, index, sqlType, vendor);
            return;
        }

        if (vendor == DatabaseVendor.ORACLE) {
            bindOracleParameter(statement, index, value, sqlType);
            return;
        }

        statement.setObject(index, value, sqlType);
    }

    private static void bindNull(PreparedStatement statement, int index, int sqlType, DatabaseVendor vendor) throws Exception {
        if (vendor == DatabaseVendor.ORACLE) {
            switch (sqlType) {
                case Types.CLOB -> statement.setNull(index, Types.VARCHAR);
                case Types.TIMESTAMP -> statement.setNull(index, Types.TIMESTAMP);
                case Types.DATE -> statement.setNull(index, Types.DATE);
                case Types.TIME -> statement.setNull(index, Types.VARCHAR);
                case Types.DOUBLE, Types.FLOAT, Types.REAL -> statement.setNull(index, Types.DOUBLE);
                case Types.NUMERIC, Types.DECIMAL, Types.INTEGER, Types.SMALLINT, Types.TINYINT -> statement.setNull(index, Types.NUMERIC);
                default -> statement.setNull(index, Types.VARCHAR);
            }
            return;
        }
        statement.setNull(index, sqlType);
    }

    private static void bindOracleParameter(PreparedStatement statement, int index, Object value, int sqlType) throws Exception {
        switch (sqlType) {
            case Types.TIMESTAMP -> {
                if (value instanceof LocalDateTime localDateTime) {
                    statement.setTimestamp(index, Timestamp.valueOf(localDateTime));
                } else if (value instanceof Timestamp timestamp) {
                    statement.setTimestamp(index, timestamp);
                } else {
                    statement.setTimestamp(index, Timestamp.valueOf(LocalDateTime.parse(String.valueOf(value).replace(' ', 'T'))));
                }
            }
            case Types.DATE -> {
                if (value instanceof LocalDate localDate) {
                    statement.setDate(index, java.sql.Date.valueOf(localDate));
                } else if (value instanceof java.sql.Date date) {
                    statement.setDate(index, date);
                } else {
                    statement.setDate(index, java.sql.Date.valueOf(String.valueOf(value)));
                }
            }
            case Types.TIME -> {
                if (value instanceof LocalTime localTime) {
                    statement.setString(index, localTime.toString());
                } else {
                    statement.setString(index, String.valueOf(value));
                }
            }
            case Types.CLOB -> statement.setString(index, String.valueOf(value));
            case Types.DOUBLE, Types.FLOAT, Types.REAL -> {
                if (value instanceof BigDecimal bigDecimal) {
                    statement.setDouble(index, bigDecimal.doubleValue());
                } else if (value instanceof Number number) {
                    statement.setDouble(index, number.doubleValue());
                } else {
                    statement.setDouble(index, Double.parseDouble(String.valueOf(value)));
                }
            }
            case Types.NUMERIC, Types.DECIMAL -> {
                if (value instanceof BigDecimal bigDecimal) {
                    statement.setBigDecimal(index, bigDecimal);
                } else if (value instanceof Integer integer) {
                    statement.setInt(index, integer);
                } else if (value instanceof Long longValue) {
                    statement.setLong(index, longValue);
                } else if (value instanceof Number number) {
                    statement.setBigDecimal(index, new BigDecimal(number.toString()));
                } else {
                    statement.setBigDecimal(index, new BigDecimal(String.valueOf(value)));
                }
            }
            case Types.INTEGER, Types.SMALLINT, Types.TINYINT -> {
                if (value instanceof Number number) {
                    statement.setInt(index, number.intValue());
                } else {
                    statement.setInt(index, Integer.parseInt(String.valueOf(value)));
                }
            }
            case Types.VARCHAR, Types.CHAR, Types.LONGVARCHAR -> statement.setString(index, String.valueOf(value));
            default -> statement.setObject(index, value, sqlType);
        }
    }
}
