package com.apache.sfdc.storage.service;

import com.apache.sfdc.pubsub.repository.PubSubRepository;
import com.apache.sfdc.storage.model.repository.ExternalDatabaseStorageRoutingRepository;
import com.apache.sfdc.streaming.repository.StreamingRepository;
import com.etlplatform.common.error.AppException;
import com.etlplatform.common.storage.database.DatabaseAuthMethod;
import com.etlplatform.common.storage.database.DatabaseCertificateMetaCodec;
import com.etlplatform.common.storage.database.DatabaseJdbcSupport;
import com.etlplatform.common.storage.database.DatabaseVendor;
import com.etlplatform.common.storage.database.ExternalDatabaseStorageDefinition;
import com.etlplatform.common.storage.database.sql.BoundBatchSql;
import com.etlplatform.common.storage.database.sql.BoundSql;
import com.etlplatform.common.storage.database.sql.DatabaseVendorStrategies;
import com.etlplatform.common.storage.database.sql.DatabaseVendorStrategy;
import com.etlplatform.common.storage.database.sql.SqlParameter;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Component
@RequiredArgsConstructor
@Slf4j
@ConditionalOnProperty(name = "app.db.enabled", havingValue = "true")
public class ExternalStorageRoutingJdbcExecutor {

    private final StreamingRepository streamingRepository;
    private final PubSubRepository pubSubRepository;
    private final ExternalDatabaseStorageRoutingRepository externalStorageRepository;
    private final DatabaseCredentialEncryptor credentialEncryptor;
    private final DatabaseCertificateMetaCodec certificateMetaCodec = new DatabaseCertificateMetaCodec();
    private final ConcurrentMap<Long, RoutedStorageContext> routedContexts = new ConcurrentHashMap<>();

    @Value("${app.routing.external-storage.user-key:}")
    private String legacyExternalStorageUserKey;

    public DatabaseVendorStrategy resolveStrategy(Long targetStorageId) {
        if (targetStorageId == null) {
            return DatabaseVendorStrategies.defaultStrategy();
        }
        return routedContexts.computeIfAbsent(targetStorageId, this::createContext).strategy();
    }

    public boolean usesExternalStorage(Long targetStorageId) {
        return targetStorageId != null;
    }

    public void executeDdl(String routingProtocol, String ddl, Long targetStorageId, String targetSchema) {
        if (targetStorageId == null) {
            defaultRepository(routingProtocol).setTable(ddl);
            return;
        }

        RoutedStorageContext context = routedContexts.computeIfAbsent(targetStorageId, this::createContext);
        try (Connection connection = context.dataSource().getConnection();
             Statement statement = connection.createStatement()) {
            ensureLogicalSchemaExistsIfNeeded(statement, context, targetSchema);
            for (String ddlStatement : splitDdlStatements(ddl, context.strategy().vendor())) {
                statement.execute(ddlStatement);
            }
        } catch (Exception e) {
            invalidate(targetStorageId, context);
            throw new AppException("외부 DB 저장소로 DDL 실행에 실패했어요. storageId=" + targetStorageId, e);
        }
    }

    public int insert(String routingProtocol, String upperQuery, List<String> rows, String tailQuery, Long targetStorageId) {
        if (targetStorageId == null) {
            return defaultRepository(routingProtocol).insertObject(upperQuery, rows, tailQuery);
        }
        if (rows == null || rows.isEmpty()) {
            return 0;
        }
        String sql = upperQuery + "\n" + String.join(",", rows) + ((tailQuery == null || tailQuery.isBlank()) ? "" : "\n" + tailQuery);
        return withStatement(targetStorageId, statement -> statement.executeUpdate(sql));
    }

    public int insert(BoundBatchSql batchSql, Long targetStorageId) {
        if (targetStorageId == null) {
            throw new AppException("Bound insert는 외부 DB 저장소 경로에서만 지원돼요.");
        }
        DatabaseVendor vendor = resolveStrategy(targetStorageId).vendor();
        return withPreparedStatement(targetStorageId, batchSql.sql(), batchSql.parameterGroups(),
                statement -> executeBatch(statement, batchSql.parameterGroups(), vendor));
    }

    public int update(String routingProtocol, StringBuilder sql, Long targetStorageId) {
        if (targetStorageId == null) {
            return defaultRepository(routingProtocol).updateObject(sql);
        }
        return withStatement(targetStorageId, statement -> statement.executeUpdate(sql.toString()));
    }

    public int update(BoundSql boundSql, Long targetStorageId) {
        if (targetStorageId == null) {
            throw new AppException("Bound update는 외부 DB 저장소 경로에서만 지원돼요.");
        }
        DatabaseVendor vendor = resolveStrategy(targetStorageId).vendor();
        return withPreparedStatement(targetStorageId, boundSql.sql(), boundSql.parameters(), statement -> {
            RoutingPreparedStatementBinder.bind(statement, boundSql.parameters(), vendor);
            return statement.executeUpdate();
        });
    }

    public int delete(String routingProtocol, StringBuilder sql, Long targetStorageId) {
        if (targetStorageId == null) {
            return defaultRepository(routingProtocol).deleteObject(sql);
        }
        return withStatement(targetStorageId, statement -> statement.executeUpdate(sql.toString()));
    }

    public int delete(BoundSql boundSql, Long targetStorageId) {
        if (targetStorageId == null) {
            throw new AppException("Bound delete는 외부 DB 저장소 경로에서만 지원돼요.");
        }
        DatabaseVendor vendor = resolveStrategy(targetStorageId).vendor();
        return withPreparedStatement(targetStorageId, boundSql.sql(), boundSql.parameters(), statement -> {
            RoutingPreparedStatementBinder.bind(statement, boundSql.parameters(), vendor);
            return statement.executeUpdate();
        });
    }

    private RoutingRepositoryDelegate defaultRepository(String routingProtocol) {
        if ("CDC".equalsIgnoreCase(routingProtocol)) {
            return new RoutingRepositoryDelegate() {
                @Override
                public void setTable(String ddl) { pubSubRepository.setTable(ddl); }
                @Override
                public int insertObject(String upperQuery, List<String> rows, String tailQuery) { return pubSubRepository.insertObject(upperQuery, rows, tailQuery); }
                @Override
                public int updateObject(StringBuilder sql) { return pubSubRepository.updateObject(sql); }
                @Override
                public int deleteObject(StringBuilder sql) { return pubSubRepository.deleteObject(sql); }
            };
        }
        return new RoutingRepositoryDelegate() {
            @Override
            public void setTable(String ddl) { streamingRepository.setTable(ddl); }
            @Override
            public int insertObject(String upperQuery, List<String> rows, String tailQuery) { return streamingRepository.insertObject(upperQuery, rows, tailQuery); }
            @Override
            public int updateObject(StringBuilder sql) { return streamingRepository.updateObject(sql); }
            @Override
            public int deleteObject(StringBuilder sql) { return streamingRepository.deleteObject(sql); }
        };
    }

    private <T> T withStatement(Long targetStorageId, SqlFunction<Statement, T> callback) {
        RoutedStorageContext context = routedContexts.computeIfAbsent(targetStorageId, this::createContext);
        try (Connection connection = context.dataSource().getConnection();
             Statement statement = connection.createStatement()) {
            return callback.apply(statement);
        } catch (Exception e) {
            invalidate(targetStorageId, context);
            throw new AppException("외부 DB 저장소로 SQL 실행에 실패했어요. storageId=" + targetStorageId, e);
        }
    }

    private <T> T withPreparedStatement(Long targetStorageId, String sql, Object bindDebugPayload, SqlFunction<PreparedStatement, T> callback) {
        RoutedStorageContext context = routedContexts.computeIfAbsent(targetStorageId, this::createContext);
        try (Connection connection = context.dataSource().getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            return callback.apply(statement);
        } catch (Exception e) {
            logBindFailureIfNeeded(context, sql, bindDebugPayload, e);
            invalidate(targetStorageId, context);
            throw new AppException("외부 DB 저장소로 SQL 실행에 실패했어요. storageId=" + targetStorageId, e);
        }
    }

    private int executeBatch(PreparedStatement statement, List<List<SqlParameter>> parameterGroups, DatabaseVendor vendor) throws Exception {
        int total = 0;
        for (List<SqlParameter> parameterGroup : parameterGroups) {
            RoutingPreparedStatementBinder.bind(statement, parameterGroup, vendor);
            statement.addBatch();
        }
        for (int affected : statement.executeBatch()) {
            total += Math.max(affected, 0);
        }
        return total;
    }


    private void logBindFailureIfNeeded(RoutedStorageContext context, String sql, Object bindDebugPayload, Exception e) {
        if (!log.isDebugEnabled() || context == null || context.strategy().vendor() != DatabaseVendor.ORACLE) {
            return;
        }

        String payloadSummary;
        if (bindDebugPayload instanceof List<?> list && !list.isEmpty() && list.get(0) instanceof List<?>) {
            @SuppressWarnings("unchecked")
            List<List<SqlParameter>> groups = (List<List<SqlParameter>>) bindDebugPayload;
            payloadSummary = formatParameterGroups(groups);
        } else if (bindDebugPayload instanceof List<?> list) {
            @SuppressWarnings("unchecked")
            List<SqlParameter> parameters = (List<SqlParameter>) list;
            payloadSummary = formatParameters(parameters);
        } else {
            payloadSummary = String.valueOf(bindDebugPayload);
        }

        log.debug("[ORACLE-BIND-FAILURE] storageId={}, storageName={}, vendor={}, sql=\n{}\nparameters={}\nrootCause={}",
                context.storage().getStorageId(),
                context.storage().getName(),
                context.strategy().vendor(),
                sql,
                payloadSummary,
                e.toString());
    }

    private String formatParameterGroups(List<List<SqlParameter>> groups) {
        StringBuilder builder = new StringBuilder();
        int maxGroups = Math.min(groups.size(), 3);
        builder.append("groupCount=").append(groups.size()).append(", sampleGroups=[");
        for (int i = 0; i < maxGroups; i++) {
            if (i > 0) {
                builder.append(", ");
            }
            builder.append("#").append(i + 1).append(": ").append(formatParameters(groups.get(i)));
        }
        if (groups.size() > maxGroups) {
            builder.append(", ...");
        }
        builder.append("]");
        return builder.toString();
    }

    private String formatParameters(List<SqlParameter> parameters) {
        StringBuilder builder = new StringBuilder("[");
        for (int i = 0; i < parameters.size(); i++) {
            if (i > 0) {
                builder.append(", ");
            }
            SqlParameter parameter = parameters.get(i);
            Object value = parameter != null ? parameter.value() : null;
            builder.append("{")
                    .append("idx=").append(i + 1)
                    .append(", sqlType=").append(parameter != null ? parameter.sqlType() : "null")
                    .append(", valueClass=").append(value != null ? value.getClass().getName() : "null")
                    .append(", value=").append(truncateValue(value))
                    .append("}");
        }
        builder.append("]");
        return builder.toString();
    }

    private String truncateValue(Object value) {
        if (value == null) {
            return "null";
        }
        String text = String.valueOf(value).replaceAll("\\s+", " ").trim();
        return text.length() > 200 ? text.substring(0, 200) + "..." : text;
    }

    private RoutedStorageContext createContext(Long targetStorageId) {
        ExternalDatabaseStorageDefinition storage = externalStorageRepository.findByStorageId(targetStorageId);
        if (storage == null) {
            throw new AppException("외부 DB 저장소를 찾을 수 없어요. storageId=" + targetStorageId);
        }
        if (Boolean.FALSE.equals(storage.getEnabled())) {
            throw new AppException("비활성화된 외부 DB 저장소입니다. storageId=" + targetStorageId);
        }
        if (storage.getJdbcUrl() == null || storage.getJdbcUrl().isBlank()) {
            throw new AppException("외부 DB 저장소 JDBC URL이 비어 있습니다. storageId=" + targetStorageId);
        }

        DatabaseJdbcSupport.loadDriver(storage.getVendor());
        Properties properties = buildConnectionProperties(storage);

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(storage.getJdbcUrl());
        if (storage.getUsername() != null && !storage.getUsername().isBlank()) {
            config.setUsername(storage.getUsername());
        }
        if (properties.containsKey("password") && !properties.containsKey("dataSource.password")) {
            config.setPassword(properties.getProperty("password"));
        }
        config.setPoolName("routing-storage-" + targetStorageId);
        config.setMaximumPoolSize(3);
        config.setMinimumIdle(0);
        config.setConnectionTimeout(5000);
        config.setValidationTimeout(3000);
        config.setInitializationFailTimeout(5000);
        config.setDriverClassName(DatabaseJdbcSupport.resolveDriverClassName(storage.getVendor()));

        Properties dsProps = new Properties();
        for (String key : properties.stringPropertyNames()) {
            if ("user".equals(key) || "password".equals(key)) {
                continue;
            }
            dsProps.setProperty(key, properties.getProperty(key));
        }
        config.setDataSourceProperties(dsProps);

        HikariDataSource dataSource = new HikariDataSource(config);
        DatabaseVendorStrategy strategy = DatabaseVendorStrategies.require(storage.getVendor());
        log.info("Created routed datasource. storageId={}, name={}, vendor={}, status={}",
                storage.getStorageId(), storage.getName(), storage.getVendor(), storage.getConnectionStatus());
        return new RoutedStorageContext(storage, strategy, dataSource);
    }

    private Properties buildConnectionProperties(ExternalDatabaseStorageDefinition storage) {
        try {
            return DatabaseJdbcSupport.buildStoredConnectionProperties(
                    storage,
                    credentialEncryptor,
                    certificateMetaCodec,
                    resolveDecryptionKey());
        } catch (AppException e) {
            if (storage.getAuthMethod() == DatabaseAuthMethod.PASSWORD || storage.getAuthMethod() == DatabaseAuthMethod.CERTIFICATE) {
                String message = e.getMessage();
                if (message != null && (message.contains("APP_DB_CREDENTIAL_KEY") || message.contains("복호화 키") || message.contains("요청 키"))) {
                    throw new AppException("자격증명 복호화에 실패했어요. APP_DB_CREDENTIAL_KEY를 우선 확인하고, 레거시 호환이 필요하면 app.routing.external-storage.user-key 설정도 확인해 주세요.", e);
                }
            }
            throw e;
        }
    }

    private String resolveDecryptionKey() {
        String appDbCredentialKey = normalize(System.getenv("APP_DB_CREDENTIAL_KEY"));
        if (appDbCredentialKey == null) {
            appDbCredentialKey = normalize(System.getProperty("APP_DB_CREDENTIAL_KEY"));
        }
        if (appDbCredentialKey != null) {
            validateKeyLength(appDbCredentialKey, "APP_DB_CREDENTIAL_KEY");
            return appDbCredentialKey;
        }

        String legacyKey = normalize(legacyExternalStorageUserKey);
        if (legacyKey != null) {
            validateKeyLength(legacyKey, "app.routing.external-storage.user-key");
            return legacyKey;
        }
        return null;
    }

    private void validateKeyLength(String key, String source) {
        if (key.length() < 8) {
            throw new AppException(source + "는 8자 이상이어야 해요.");
        }
    }

    private String normalize(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }

    private void invalidate(Long targetStorageId, RoutedStorageContext context) {
        routedContexts.remove(targetStorageId);
        closeQuietly(context.dataSource());
    }

    private void closeQuietly(DataSource dataSource) {
        if (dataSource instanceof HikariDataSource hikariDataSource) {
            try {
                hikariDataSource.close();
            } catch (Exception ignored) {
            }
        }
    }

    private void ensureLogicalSchemaExistsIfNeeded(Statement statement, RoutedStorageContext context, String targetSchema) throws Exception {
        if (statement == null || context == null) {
            return;
        }
        DatabaseVendor vendor = context.strategy().vendor();
        String schemaName = normalize(targetSchema);
        if (schemaName == null && context.storage() != null) {
            schemaName = normalize(context.storage().getSchemaName());
        }
        if (schemaName == null) {
            return;
        }

        if (vendor == DatabaseVendor.MARIADB || vendor == DatabaseVendor.MYSQL) {
            String sql = "CREATE DATABASE IF NOT EXISTS `" + schemaName.replace("`", "``") + "` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci";
            statement.execute(sql);
            return;
        }

        if (vendor == DatabaseVendor.POSTGRESQL) {
            String sql = "CREATE SCHEMA IF NOT EXISTS \"" + schemaName.replace("\"", "\"\"") + "\"";
            statement.execute(sql);
        }
    }

    private List<String> splitDdlStatements(String ddl, DatabaseVendor vendor) {
        return RoutingDdlSplitter.split(ddl, vendor);
    }

    @FunctionalInterface
    private interface SqlFunction<T, R> {
        R apply(T value) throws Exception;
    }

    private interface RoutingRepositoryDelegate {
        void setTable(String ddl);
        int insertObject(String upperQuery, List<String> rows, String tailQuery);
        int updateObject(StringBuilder sql);
        int deleteObject(StringBuilder sql);
    }

    private record RoutedStorageContext(ExternalDatabaseStorageDefinition storage,
                                        DatabaseVendorStrategy strategy,
                                        HikariDataSource dataSource) {
    }
}
