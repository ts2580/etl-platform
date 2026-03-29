package com.apache.sfdc.storage.smoke;

import com.etlplatform.common.storage.database.DatabaseVendor;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.oracle.OracleContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.Locale;
import java.util.function.Function;
import java.util.function.Supplier;

record VendorContainerDescriptor(
        String id,
        DatabaseVendor vendor,
        Supplier<JdbcDatabaseContainer<?>> containerFactory,
        Function<JdbcDatabaseContainer<?>, String> targetSchemaResolver
) {

    static final String DEFAULT_TARGET_SCHEMA = "routing_vendor_smoke";

    static List<VendorContainerDescriptor> supported() {
        return List.of(
                postgresql(),
                mariadb(),
                mysql(),
                oracleCandidate()
        );
    }

    static VendorContainerDescriptor oracleCandidate() {
        return new VendorContainerDescriptor(
                "oracle",
                DatabaseVendor.ORACLE,
                () -> new OracleContainer(DockerImageName.parse("gvenzl/oracle-free:23-slim-faststart"))
                        .withUsername("routing")
                        .withPassword("routingpass1"),
                container -> container.getUsername().toUpperCase(Locale.ROOT)
        );
    }

    private static VendorContainerDescriptor postgresql() {
        return new VendorContainerDescriptor(
                "postgresql",
                DatabaseVendor.POSTGRESQL,
                () -> new PostgreSQLContainer<>("postgres:16-alpine")
                        .withDatabaseName("routing")
                        .withUsername("test")
                        .withPassword("test"),
                container -> DEFAULT_TARGET_SCHEMA
        );
    }

    private static VendorContainerDescriptor mariadb() {
        return new VendorContainerDescriptor(
                "mariadb",
                DatabaseVendor.MARIADB,
                () -> new MariaDBContainer<>("mariadb:11.4")
                        .withDatabaseName("routing")
                        .withUsername("root")
                        .withPassword("test"),
                container -> DEFAULT_TARGET_SCHEMA
        );
    }

    private static VendorContainerDescriptor mysql() {
        return new VendorContainerDescriptor(
                "mysql",
                DatabaseVendor.MYSQL,
                () -> new MySQLContainer<>("mysql:8.4")
                        .withDatabaseName("routing")
                        .withUsername("root")
                        .withPassword("test"),
                container -> DEFAULT_TARGET_SCHEMA
        );
    }

    String resolveTargetSchema(JdbcDatabaseContainer<?> container) {
        return targetSchemaResolver.apply(container);
    }
}
