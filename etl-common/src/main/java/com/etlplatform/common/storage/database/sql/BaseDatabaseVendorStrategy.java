package com.etlplatform.common.storage.database.sql;

import com.etlplatform.common.error.AppException;

import java.math.BigDecimal;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;

public abstract class BaseDatabaseVendorStrategy implements DatabaseVendorStrategy {

    protected static final DateTimeFormatter DATETIME_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    protected static final DateTimeFormatter DATETIME_FRACTION_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
    protected static final DateTimeFormatter TIME_FORMAT = DateTimeFormatter.ofPattern("HH:mm:ss");
    protected static final DateTimeFormatter TIME_FRACTION_FORMAT = DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSS");

    @Override
    public SqlParameter bindValue(Object value, String sfType) {
        if (value == null) {
            return new SqlParameter(null, sqlType(sfType));
        }

        return switch (sfType) {
            case "double", "percent", "currency" -> new SqlParameter(parseDecimal(value, sfType), sqlType(sfType));
            case "int" -> new SqlParameter(parseInteger(value), sqlType(sfType));
            case "boolean" -> bindBoolean(parseBoolean(value));
            case "datetime" -> bindDateTime(parseDateTime(value));
            case "date" -> bindDate(parseDate(value));
            case "time" -> bindTime(parseTime(value));
            default -> new SqlParameter(String.valueOf(value), Types.VARCHAR);
        };
    }

    @Override
    public String renderLiteral(Object value, String sfType) {
        if (value == null) {
            return "null";
        }

        return switch (sfType) {
            case "double", "percent", "currency" -> parseDecimal(value, sfType).toPlainString();
            case "int" -> String.valueOf(parseInteger(value));
            case "boolean" -> renderBooleanLiteral(parseBoolean(value));
            case "datetime" -> renderDateTimeLiteral(parseDateTime(value));
            case "date" -> renderDateLiteral(parseDate(value));
            case "time" -> renderTimeLiteral(parseTime(value));
            default -> quoteString(String.valueOf(value));
        };
    }

    @Override
    public String columnCommentClause(String comment) {
        return "";
    }

    @Override
    public List<String> afterCreateTableStatements(String schemaName, String tableName, List<ColumnDefinition> columns) {
        return List.of();
    }

    protected SqlParameter bindBoolean(Boolean value) {
        return new SqlParameter(value, Types.BOOLEAN);
    }

    protected SqlParameter bindDateTime(LocalDateTime value) {
        return new SqlParameter(value, Types.TIMESTAMP);
    }

    protected SqlParameter bindDate(LocalDate value) {
        return new SqlParameter(value, Types.DATE);
    }

    protected SqlParameter bindTime(LocalTime value) {
        return new SqlParameter(value, Types.TIME);
    }

    protected String renderBooleanLiteral(Boolean value) {
        return value ? "TRUE" : "FALSE";
    }

    protected String renderDateTimeLiteral(LocalDateTime value) {
        return quoteString(formatDateTime(value));
    }

    protected String renderDateLiteral(LocalDate value) {
        return quoteString(value.toString());
    }

    protected String renderTimeLiteral(LocalTime value) {
        return quoteString(formatTime(value));
    }

    protected int sqlType(String sfType) {
        return switch (sfType) {
            case "double", "percent", "currency" -> Types.DOUBLE;
            case "int" -> Types.INTEGER;
            case "boolean" -> Types.BOOLEAN;
            case "datetime" -> Types.TIMESTAMP;
            case "date" -> Types.DATE;
            case "time" -> Types.TIME;
            default -> Types.VARCHAR;
        };
    }

    protected BigDecimal parseDecimal(Object value, String sfType) {
        try {
            if (value instanceof Number number) {
                return new BigDecimal(number.toString());
            }
            return new BigDecimal(String.valueOf(value).trim());
        } catch (NumberFormatException ex) {
            throw new AppException("Invalid numeric value for type " + sfType + ": " + value, ex);
        }
    }

    protected Integer parseInteger(Object value) {
        try {
            if (value instanceof Number number) {
                return number.intValue();
            }
            return Integer.valueOf(String.valueOf(value).trim());
        } catch (NumberFormatException ex) {
            throw new AppException("Invalid integer value: " + value, ex);
        }
    }

    protected Boolean parseBoolean(Object value) {
        if (value instanceof Boolean bool) {
            return bool;
        }
        String raw = String.valueOf(value).trim();
        if ("true".equalsIgnoreCase(raw) || "1".equals(raw)) {
            return Boolean.TRUE;
        }
        if ("false".equalsIgnoreCase(raw) || "0".equals(raw)) {
            return Boolean.FALSE;
        }
        throw new AppException("Invalid boolean value: " + value);
    }

    protected LocalDateTime parseDateTime(Object value) {
        if (value instanceof LocalDateTime localDateTime) {
            return trimToMicros(localDateTime);
        }
        if (value instanceof Number number) {
            return fromEpoch(number.longValue());
        }

        String raw = normalizeTemporalText(value);
        if (raw.isBlank()) {
            return null;
        }
        if (raw.matches("^\\d{13}$")) {
            return fromEpoch(Long.parseLong(raw));
        }
        if (raw.matches("^\\d{10}$")) {
            return fromEpoch(Long.parseLong(raw) * 1000L);
        }
        try {
            return trimToMicros(OffsetDateTime.parse(raw).withOffsetSameInstant(ZoneOffset.UTC).toLocalDateTime());
        } catch (DateTimeParseException ignored) {
        }
        try {
            return trimToMicros(Instant.parse(raw).atOffset(ZoneOffset.UTC).toLocalDateTime());
        } catch (DateTimeParseException ignored) {
        }
        try {
            return trimToMicros(LocalDateTime.parse(raw.replace(' ', 'T')));
        } catch (DateTimeParseException ex) {
            throw new AppException("Invalid datetime value: " + value, ex);
        }
    }

    protected LocalDate parseDate(Object value) {
        if (value instanceof LocalDate localDate) {
            return localDate;
        }
        if (value instanceof Number number) {
            return fromEpoch(number.longValue()).toLocalDate();
        }

        String raw = normalizeTemporalText(value);
        if (raw.isBlank()) {
            return null;
        }
        if (raw.matches("^\\d{13}$")) {
            return fromEpoch(Long.parseLong(raw)).toLocalDate();
        }
        if (raw.matches("^\\d{10}$")) {
            return fromEpoch(Long.parseLong(raw) * 1000L).toLocalDate();
        }
        try {
            return LocalDate.parse(raw);
        } catch (DateTimeParseException ex) {
            return parseDateTime(raw).toLocalDate();
        }
    }

    protected LocalTime parseTime(Object value) {
        if (value instanceof LocalTime localTime) {
            return trimToMicros(localTime);
        }

        String raw = normalizeTemporalText(value);
        if (raw.isBlank()) {
            return null;
        }
        try {
            return trimToMicros(OffsetDateTime.parse(raw).withOffsetSameInstant(ZoneOffset.UTC).toLocalTime());
        } catch (DateTimeParseException ignored) {
        }
        try {
            return trimToMicros(LocalTime.parse(raw));
        } catch (DateTimeParseException ex) {
            try {
                return trimToMicros(parseDateTime(raw).toLocalTime());
            } catch (AppException nested) {
                throw new AppException("Invalid time value: " + value, ex);
            }
        }
    }

    protected LocalDateTime fromEpoch(long epochValue) {
        long millis = String.valueOf(Math.abs(epochValue)).length() <= 10 ? epochValue * 1000L : epochValue;
        return trimToMicros(LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneOffset.UTC));
    }

    protected String formatDateTime(LocalDateTime value) {
        return value.getNano() == 0 ? value.format(DATETIME_FORMAT) : value.format(DATETIME_FRACTION_FORMAT);
    }

    protected String formatTime(LocalTime value) {
        return value.getNano() == 0 ? value.format(TIME_FORMAT) : value.format(TIME_FRACTION_FORMAT);
    }

    protected String normalizeTemporalText(Object value) {
        return String.valueOf(value)
                .trim()
                .replace(".000+0000", "")
                .replace(' ', 'T');
    }

    protected LocalDateTime trimToMicros(LocalDateTime value) {
        return value.withNano((value.getNano() / 1000) * 1000);
    }

    protected LocalTime trimToMicros(LocalTime value) {
        return value.withNano((value.getNano() / 1000) * 1000);
    }

    protected String quoteString(String value) {
        return "'" + value.replace("'", "''") + "'";
    }

    protected String normalizeComment(String comment) {
        return comment == null ? "" : comment.replace("'", "''");
    }
}
