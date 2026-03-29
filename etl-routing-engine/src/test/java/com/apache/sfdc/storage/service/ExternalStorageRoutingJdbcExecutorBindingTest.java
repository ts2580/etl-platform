package com.apache.sfdc.storage.service;

import com.apache.sfdc.pubsub.repository.PubSubRepository;
import com.apache.sfdc.storage.model.repository.ExternalDatabaseStorageRoutingRepository;
import com.apache.sfdc.streaming.repository.StreamingRepository;
import com.etlplatform.common.storage.database.DatabaseVendor;
import com.etlplatform.common.storage.database.ExternalDatabaseStorageDefinition;
import com.etlplatform.common.storage.database.sql.BoundBatchSql;
import com.etlplatform.common.storage.database.sql.BoundSql;
import com.etlplatform.common.storage.database.sql.DatabaseVendorStrategies;
import com.etlplatform.common.storage.database.sql.DatabaseVendorStrategy;
import com.etlplatform.common.storage.database.sql.SqlParameter;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.lang.reflect.Constructor;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ExternalStorageRoutingJdbcExecutorBindingTest {

    @Mock
    private StreamingRepository streamingRepository;
    @Mock
    private PubSubRepository pubSubRepository;
    @Mock
    private ExternalDatabaseStorageRoutingRepository externalStorageRepository;
    @Mock
    private DatabaseCredentialEncryptor credentialEncryptor;
    @Mock
    private HikariDataSource dataSource;
    @Mock
    private Connection connection;
    @Mock
    private PreparedStatement preparedStatement;
    @Mock
    private ExternalDatabaseStorageDefinition storage;

    @Test
    void oracleUpdateBindsVendorSpecificJdbcMethodsForNullTimestampBigDecimalClobAndNumeric() throws Exception {
        ExternalStorageRoutingJdbcExecutor executor = newExecutorWithContext(101L, DatabaseVendor.ORACLE);
        when(preparedStatement.executeUpdate()).thenReturn(1);

        LocalDateTime timestamp = LocalDateTime.of(2026, 3, 28, 0, 15, 30, 123456000);
        BoundSql boundSql = new BoundSql(
                "update T set C1=?, C2=?, C3=?, C4=?, C5=?, C6=?",
                List.of(
                        new SqlParameter(null, Types.CLOB),
                        new SqlParameter(timestamp, Types.TIMESTAMP),
                        new SqlParameter(new BigDecimal("12345678901234567890.123456"), Types.DECIMAL),
                        new SqlParameter("very long note", Types.CLOB),
                        new SqlParameter(1, Types.NUMERIC),
                        new SqlParameter("00:15:30.123456", Types.TIME)
                )
        );

        int updated = executor.update(boundSql, 101L);

        assertEquals(1, updated);
        verify(connection).prepareStatement("update T set C1=?, C2=?, C3=?, C4=?, C5=?, C6=?");
        verify(preparedStatement).setNull(1, Types.VARCHAR);
        verify(preparedStatement).setTimestamp(2, Timestamp.valueOf(timestamp));
        verify(preparedStatement).setBigDecimal(3, new BigDecimal("12345678901234567890.123456"));
        verify(preparedStatement).setString(4, "very long note");
        verify(preparedStatement).setInt(5, 1);
        verify(preparedStatement).setString(6, "00:15:30.123456");
        verify(preparedStatement, never()).setObject(2, timestamp, Types.TIMESTAMP);
        verify(preparedStatement).executeUpdate();
    }

    @Test
    void oracleBatchInsertParsesTimestampStringsAndUsesNumericNullMappings() throws Exception {
        ExternalStorageRoutingJdbcExecutor executor = newExecutorWithContext(202L, DatabaseVendor.ORACLE);
        when(preparedStatement.executeBatch()).thenReturn(new int[]{1, 1});

        BoundBatchSql batchSql = new BoundBatchSql(
                "insert into T (C1, C2, C3) values (?, ?, ?)",
                List.of(
                        List.of(
                                new SqlParameter("2026-03-28 00:15:30.123456", Types.TIMESTAMP),
                                new SqlParameter(null, Types.NUMERIC),
                                new SqlParameter("alpha", Types.VARCHAR)
                        ),
                        List.of(
                                new SqlParameter("2026-03-29 00:15:30.654321", Types.TIMESTAMP),
                                new SqlParameter(99L, Types.NUMERIC),
                                new SqlParameter(null, Types.TIME)
                        )
                )
        );

        int inserted = executor.insert(batchSql, 202L);

        assertEquals(2, inserted);
        var inOrder = inOrder(preparedStatement);
        inOrder.verify(preparedStatement).setTimestamp(1, Timestamp.valueOf(LocalDateTime.of(2026, 3, 28, 0, 15, 30, 123456000)));
        inOrder.verify(preparedStatement).setNull(2, Types.NUMERIC);
        inOrder.verify(preparedStatement).setString(3, "alpha");
        inOrder.verify(preparedStatement).addBatch();
        inOrder.verify(preparedStatement).setTimestamp(1, Timestamp.valueOf(LocalDateTime.of(2026, 3, 29, 0, 15, 30, 654321000)));
        inOrder.verify(preparedStatement).setLong(2, 99L);
        inOrder.verify(preparedStatement).setNull(3, Types.VARCHAR);
        inOrder.verify(preparedStatement).addBatch();
        verify(preparedStatement, times(2)).addBatch();
        verify(preparedStatement).executeBatch();
    }

    @Test
    void nonOracleUpdateUsesGenericSetObjectAndOriginalNullSqlTypes() throws Exception {
        ExternalStorageRoutingJdbcExecutor executor = newExecutorWithContext(303L, DatabaseVendor.POSTGRESQL);
        when(preparedStatement.executeUpdate()).thenReturn(1);

        LocalDateTime timestamp = LocalDateTime.of(2026, 3, 28, 0, 15, 30, 123456000);
        BoundSql boundSql = new BoundSql(
                "update T set C1=?, C2=?, C3=?",
                List.of(
                        new SqlParameter(timestamp, Types.TIMESTAMP),
                        new SqlParameter(new BigDecimal("42.10"), Types.DECIMAL),
                        new SqlParameter(null, Types.TIME)
                )
        );

        int updated = executor.update(boundSql, 303L);

        assertEquals(1, updated);
        verify(preparedStatement).setObject(1, timestamp, Types.TIMESTAMP);
        verify(preparedStatement).setObject(2, new BigDecimal("42.10"), Types.DECIMAL);
        verify(preparedStatement).setNull(3, Types.TIME);
        verify(preparedStatement, never()).setTimestamp(1, Timestamp.valueOf(timestamp));
    }

    private ExternalStorageRoutingJdbcExecutor newExecutorWithContext(Long storageId, DatabaseVendor vendor) throws Exception {
        ExternalStorageRoutingJdbcExecutor executor = new ExternalStorageRoutingJdbcExecutor(
                streamingRepository,
                pubSubRepository,
                externalStorageRepository,
                credentialEncryptor
        );

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);

        @SuppressWarnings("unchecked")
        Map<Long, Object> routedContexts = (Map<Long, Object>) ReflectionTestUtils.getField(executor, "routedContexts");
        routedContexts.put(storageId, newRoutedContext(storage, DatabaseVendorStrategies.require(vendor), dataSource));
        return executor;
    }

    private Object newRoutedContext(ExternalDatabaseStorageDefinition storage,
                                    DatabaseVendorStrategy strategy,
                                    HikariDataSource dataSource) throws Exception {
        Class<?> contextClass = Class.forName("com.apache.sfdc.storage.service.ExternalStorageRoutingJdbcExecutor$RoutedStorageContext");
        Constructor<?> constructor = contextClass.getDeclaredConstructor(
                ExternalDatabaseStorageDefinition.class,
                DatabaseVendorStrategy.class,
                HikariDataSource.class
        );
        constructor.setAccessible(true);
        return constructor.newInstance(storage, strategy, dataSource);
    }
}
