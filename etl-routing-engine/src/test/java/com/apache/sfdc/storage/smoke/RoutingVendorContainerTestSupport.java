package com.apache.sfdc.storage.smoke;

import com.apache.sfdc.common.SalesforceMutationRepositoryPort;
import com.apache.sfdc.common.SalesforceObjectSchemaBuilder;
import com.apache.sfdc.pubsub.repository.PubSubRepository;
import com.apache.sfdc.storage.model.repository.ExternalDatabaseStorageRoutingRepository;
import com.apache.sfdc.storage.service.DatabaseCredentialEncryptor;
import com.apache.sfdc.storage.service.ExternalStorageRoutingJdbcExecutor;
import com.apache.sfdc.streaming.repository.StreamingRepository;
import com.etlplatform.common.storage.database.DatabaseAuthMethod;
import com.etlplatform.common.storage.database.ExternalDatabaseStorageDefinition;
import com.etlplatform.common.storage.database.sql.BoundBatchSql;
import com.etlplatform.common.storage.database.sql.BoundSql;
import com.etlplatform.common.storage.database.sql.DatabaseVendorStrategies;
import com.etlplatform.common.storage.database.sql.DatabaseVendorStrategy;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.util.ReflectionTestUtils;
import org.testcontainers.containers.JdbcDatabaseContainer;

import java.lang.reflect.Constructor;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.Mockito.mock;

final class RoutingVendorContainerTestSupport {

    private static final AtomicLong STORAGE_ID_SEQUENCE = new AtomicLong(9000L);

    private RoutingVendorContainerTestSupport() {
    }

    static RunningVendorDatabase start(VendorContainerDescriptor descriptor) throws Exception {
        JdbcDatabaseContainer<?> container = descriptor.containerFactory().get();
        container.start();
        String targetSchema = descriptor.resolveTargetSchema(container);

        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(container.getJdbcUrl());
        hikariConfig.setUsername(container.getUsername());
        hikariConfig.setPassword(container.getPassword());
        hikariConfig.setDriverClassName(container.getDriverClassName());
        hikariConfig.setMaximumPoolSize(2);
        hikariConfig.setMinimumIdle(0);
        hikariConfig.setInitializationFailTimeout(15000);

        HikariDataSource dataSource = new HikariDataSource(hikariConfig);
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        DatabaseVendorStrategy strategy = DatabaseVendorStrategies.require(descriptor.vendor());

        ExternalStorageRoutingJdbcExecutor executor = new ExternalStorageRoutingJdbcExecutor(
                mock(StreamingRepository.class),
                mock(PubSubRepository.class),
                mock(ExternalDatabaseStorageRoutingRepository.class),
                mock(DatabaseCredentialEncryptor.class)
        );

        ExternalDatabaseStorageDefinition storage = new ExternalDatabaseStorageDefinition();
        storage.setStorageId(STORAGE_ID_SEQUENCE.incrementAndGet());
        storage.setName("testcontainers-" + descriptor.id());
        storage.setVendor(descriptor.vendor());
        storage.setJdbcUrl(container.getJdbcUrl());
        storage.setUsername(container.getUsername());
        storage.setAuthMethod(DatabaseAuthMethod.PASSWORD);
        storage.setSchemaName(targetSchema);
        storage.setEnabled(true);
        storage.setConnectionStatus("CONNECTED");

        @SuppressWarnings("unchecked")
        Map<Long, Object> routedContexts = (Map<Long, Object>) ReflectionTestUtils.getField(executor, "routedContexts");
        routedContexts.put(storage.getStorageId(), newRoutedContext(storage, strategy, dataSource));

        return new RunningVendorDatabase(descriptor, container, dataSource, jdbcTemplate, executor, storage, strategy, targetSchema);
    }

    private static Object newRoutedContext(ExternalDatabaseStorageDefinition storage,
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

    record RunningVendorDatabase(
            VendorContainerDescriptor descriptor,
            JdbcDatabaseContainer<?> container,
            HikariDataSource dataSource,
            JdbcTemplate jdbcTemplate,
            ExternalStorageRoutingJdbcExecutor executor,
            ExternalDatabaseStorageDefinition storage,
            DatabaseVendorStrategy strategy,
            String targetSchema
    ) implements AutoCloseable {

        String qualifiedTableName(String logicalTableName) {
            return qualifiedTableName(logicalTableName, null);
        }

        String qualifiedTableName(String logicalTableName, String orgName) {
            return SalesforceObjectSchemaBuilder.qualifiedName(targetSchema, logicalTableName, orgName, strategy);
        }

        String qualifiedPhysicalTableName(String physicalTableName) {
            return strategy.qualifyTableName(targetSchema, physicalTableName);
        }

        void recreateTable(String logicalTableName, SalesforceObjectSchemaBuilder.SchemaResult schemaResult) {
            recreateTable(logicalTableName, null, schemaResult);
        }

        void recreateTable(String logicalTableName, String orgName, SalesforceObjectSchemaBuilder.SchemaResult schemaResult) {
            dropTableIfExists(logicalTableName, orgName);
            executor.executeDdl("CDC", schemaResult.ddl(), storage.getStorageId(), targetSchema);
        }

        SalesforceMutationRepositoryPort repositoryPort() {
            Long storageId = storage.getStorageId();
            return new SalesforceMutationRepositoryPort() {
                @Override
                public int insertObject(String upperQuery, java.util.List<String> listUnderQuery, String tailQuery) {
                    return executor.insert("CDC", upperQuery, listUnderQuery, tailQuery, storageId);
                }

                @Override
                public int updateObject(StringBuilder strUpdate) {
                    return executor.update("CDC", strUpdate, storageId);
                }

                @Override
                public int deleteObject(StringBuilder strDelete) {
                    return executor.delete("CDC", strDelete, storageId);
                }

                @Override
                public boolean supportsBoundStatements() {
                    return true;
                }

                @Override
                public DatabaseVendorStrategy vendorStrategy() {
                    return strategy;
                }

                @Override
                public int insertObject(BoundBatchSql batchSql) {
                    return executor.insert(batchSql, storageId);
                }

                @Override
                public int updateObject(BoundSql boundSql) {
                    return executor.update(boundSql, storageId);
                }

                @Override
                public int deleteObject(BoundSql boundSql) {
                    return executor.delete(boundSql, storageId);
                }
            };
        }

        PersistedRow fetchRow(String logicalTableName, String sfid) {
            return fetchRow(logicalTableName, null, sfid);
        }

        PersistedRow fetchRow(String logicalTableName, String orgName, String sfid) {
            return fetchRowFromQualifiedTable(qualifiedTableName(logicalTableName, orgName), sfid);
        }

        PersistedRow fetchPhysicalRow(String physicalTableName, String sfid) {
            return fetchRowFromQualifiedTable(qualifiedPhysicalTableName(physicalTableName), sfid);
        }

        private PersistedRow fetchRowFromQualifiedTable(String qualifiedTableName, String sfid) {
            String sql = "select "
                    + strategy.quoteIdentifier("sfid") + " as sfid, "
                    + strategy.quoteIdentifier("Name") + " as name_value, "
                    + strategy.quoteIdentifier("IsDeleted") + " as deleted_value, "
                    + strategy.quoteIdentifier("AnnualRevenue") + " as revenue_value, "
                    + strategy.quoteIdentifier("CloseDate") + " as close_date_value, "
                    + strategy.quoteIdentifier("DailyCutoff") + " as cutoff_value, "
                    + strategy.quoteIdentifier("Description") + " as description_value, "
                    + strategy.quoteIdentifier("_oc_last_modified_at") + " as modified_value, "
                    + strategy.quoteIdentifier("_oc_last_event_at") + " as event_value "
                    + "from " + qualifiedTableName + " where " + strategy.quoteIdentifier("sfid") + " = ?";
            return jdbcTemplate.query(
                    sql,
                    ps -> ps.setString(1, sfid),
                    rs -> rs.next() ? mapRow(rs) : null
            );
        }

        PersistedRow fetchRow(String logicalTableName) {
            return fetchRow(logicalTableName, null, "001000000000001AAA");
        }

        private PersistedRow mapRow(ResultSet rs) throws java.sql.SQLException {
            return new PersistedRow(
                    rs.getString("sfid"),
                    rs.getString("name_value"),
                    rs.getBoolean("deleted_value"),
                    rs.getBigDecimal("revenue_value"),
                    rs.getDate("close_date_value") != null ? rs.getDate("close_date_value").toLocalDate() : null,
                    rs.getString("cutoff_value"),
                    rs.getString("description_value"),
                    rs.getTimestamp("modified_value") != null ? rs.getTimestamp("modified_value").toLocalDateTime() : null,
                    rs.getTimestamp("event_value") != null ? rs.getTimestamp("event_value").toLocalDateTime() : null
            );
        }

        int countRows(String logicalTableName) {
            return countRows(logicalTableName, null);
        }

        int countRows(String logicalTableName, String orgName) {
            Integer count = jdbcTemplate.queryForObject("select count(*) from " + qualifiedTableName(logicalTableName, orgName), Integer.class);
            return count == null ? 0 : count;
        }

        void dropTableIfExists(String logicalTableName) {
            dropTableIfExists(logicalTableName, null);
        }

        void dropTableIfExists(String logicalTableName, String orgName) {
            String ddl = SalesforceObjectSchemaBuilder.buildDropTableSql(targetSchema, logicalTableName, orgName, strategy);
            executor.executeDdl("CDC", ddl + ";", storage.getStorageId(), targetSchema);
        }

        @Override
        public void close() {
            try {
                dataSource.close();
            } finally {
                container.close();
            }
        }
    }

    record PersistedRow(
            String sfid,
            String name,
            boolean deleted,
            BigDecimal annualRevenue,
            LocalDate closeDate,
            String dailyCutoff,
            String description,
            LocalDateTime lastModifiedAt,
            LocalDateTime lastEventAt
    ) {
    }
}
