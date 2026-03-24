package com.apache.sfdc.storage.service;

import com.apache.sfdc.pubsub.repository.PubSubRepository;
import com.apache.sfdc.storage.model.repository.ExternalDatabaseStorageRoutingRepository;
import com.apache.sfdc.streaming.repository.StreamingRepository;
import com.etlplatform.common.error.AppException;
import com.etlplatform.common.storage.database.DatabaseAuthMethod;
import com.etlplatform.common.storage.database.DatabaseCertificateMetaCodec;
import com.etlplatform.common.storage.database.DatabaseJdbcSupport;
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

    public void executeDdl(String routingProtocol, String ddl, Long targetStorageId) {
        if (targetStorageId == null) {
            defaultRepository(routingProtocol).setTable(ddl);
            return;
        }
        withStatement(targetStorageId, statement -> {
            for (String ddlStatement : splitDdlStatements(ddl)) {
                statement.execute(ddlStatement);
            }
            return null;
        });
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
        return withPreparedStatement(targetStorageId, batchSql.sql(), statement -> executeBatch(statement, batchSql.parameterGroups()));
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
        return withPreparedStatement(targetStorageId, boundSql.sql(), statement -> {
            bind(statement, boundSql.parameters());
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
        return withPreparedStatement(targetStorageId, boundSql.sql(), statement -> {
            bind(statement, boundSql.parameters());
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

    private <T> T withPreparedStatement(Long targetStorageId, String sql, SqlFunction<PreparedStatement, T> callback) {
        RoutedStorageContext context = routedContexts.computeIfAbsent(targetStorageId, this::createContext);
        try (Connection connection = context.dataSource().getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            return callback.apply(statement);
        } catch (Exception e) {
            invalidate(targetStorageId, context);
            throw new AppException("외부 DB 저장소로 SQL 실행에 실패했어요. storageId=" + targetStorageId, e);
        }
    }

    private int executeBatch(PreparedStatement statement, List<List<SqlParameter>> parameterGroups) throws Exception {
        int total = 0;
        for (List<SqlParameter> parameterGroup : parameterGroups) {
            bind(statement, parameterGroup);
            statement.addBatch();
        }
        for (int affected : statement.executeBatch()) {
            total += Math.max(affected, 0);
        }
        return total;
    }

    private void bind(PreparedStatement statement, List<SqlParameter> parameters) throws Exception {
        for (int i = 0; i < parameters.size(); i++) {
            SqlParameter parameter = parameters.get(i);
            int index = i + 1;
            if (parameter.value() == null) {
                statement.setNull(index, parameter.sqlType());
            } else {
                statement.setObject(index, parameter.value(), parameter.sqlType());
            }
        }
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

    private List<String> splitDdlStatements(String ddl) {
        if (ddl == null || ddl.isBlank()) {
            return List.of();
        }
        List<String> statements = new java.util.ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inString = false;
        for (int i = 0; i < ddl.length(); i++) {
            char ch = ddl.charAt(i);
            if (ch == '\'') {
                inString = !inString;
            }
            if (ch == ';' && !inString) {
                String statement = current.toString().trim();
                if (!statement.isBlank()) {
                    statements.add(statement);
                }
                current.setLength(0);
                continue;
            }
            current.append(ch);
        }
        String tail = current.toString().trim();
        if (!tail.isBlank()) {
            statements.add(tail);
        }
        return statements;
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
