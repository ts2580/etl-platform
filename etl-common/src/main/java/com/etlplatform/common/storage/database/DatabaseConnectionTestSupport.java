package com.etlplatform.common.storage.database;

import com.etlplatform.common.error.AppException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DatabaseConnectionTestSupport {

    private static final Logger log = Logger.getLogger(DatabaseConnectionTestSupport.class.getName());

    public DatabaseConnectionTestResult test(DatabaseStorageRegistrationRequest request) {
        DatabaseStorageRegistrationValidator.validate(request);

        try {
            DatabaseJdbcSupport.loadDriver(request.getVendor());
            DriverManager.setLoginTimeout(5);

            DatabaseJdbcMetadata metadata = resolveMetadata(request);
            String jdbcUrl = DatabaseJdbcSupport.buildJdbcUrl(request.getVendor(), metadata);
            Properties properties = buildConnectionProperties(request);

            logConnectionPlan(request, metadata, jdbcUrl, properties);

            try (Connection ignored = DriverManager.getConnection(jdbcUrl, properties)) {
                if (request.getAuthMethod() == DatabaseAuthMethod.CERTIFICATE) {
                    return new DatabaseConnectionTestResult(true, "인증서 인증으로 실제 DB 접속 테스트에 성공했어요.");
                }
                return new DatabaseConnectionTestResult(true, "연결 테스트 성공했어요.");
            }
        } catch (SQLException e) {
            DatabaseJdbcMetadata debugMetadata = safeResolveMetadata(request);
            String debugJdbcUrl = safeBuildJdbcUrl(request, debugMetadata);
            Properties debugProperties = buildConnectionProperties(request);

            String line = "[DB 연결 테스트] 실패. "
                    + DatabaseConnectionLogFormatter.connectionSummary(
                    request.getVendor(),
                    request.getAuthMethod(),
                    request.getJdbcUrl(),
                    request.getPort(),
                    request.getUsername(),
                    debugJdbcUrl,
                    debugMetadata,
                    debugProperties)
                    + ", reason=" + e.getMessage()
                    + ", sqlState=" + e.getSQLState()
                    + ", errorCode=" + e.getErrorCode()
                    + ", causeChain=" + summarizeThrowableChain(e);
            log.log(Level.SEVERE, line);
            log.log(Level.SEVERE, "SQLException detail", e);
            System.err.println(line);
            e.printStackTrace(System.err);
            throw new AppException(buildFailureMessage(debugJdbcUrl, e), e);
        }
    }

    private DatabaseJdbcMetadata resolveMetadata(DatabaseStorageRegistrationRequest request) {
        DatabaseJdbcMetadata metadata = DatabaseJdbcSupport.parseMetadata(request.getVendor(), request.getJdbcUrl(), request.getPort());

        if (request.getVendor() == DatabaseVendor.ORACLE) {
            String schemaOrService = OracleStorageSupport.normalizeBlankToNull(request.getSchemaName());
            String serviceName = OracleStorageSupport.resolveJdbcServiceName(metadata.serviceName(), metadata.sid(), schemaOrService);
            metadata = new DatabaseJdbcMetadata(metadata.host(), metadata.port(), metadata.databaseName(), serviceName, metadata.sid());
        }

        if (request.getVendor() == DatabaseVendor.POSTGRESQL
                && metadata.databaseName() == null) {
            String databaseName = normalizeBlankToNull(request.getDatabaseName());
            if (databaseName != null) {
                metadata = new DatabaseJdbcMetadata(metadata.host(), metadata.port(), databaseName, metadata.serviceName(), metadata.sid());
            }
        }
        return metadata;
    }

    private DatabaseJdbcMetadata safeResolveMetadata(DatabaseStorageRegistrationRequest request) {
        try {
            return resolveMetadata(request);
        } catch (Exception ignore) {
            return null;
        }
    }

    private String safeBuildJdbcUrl(DatabaseStorageRegistrationRequest request, DatabaseJdbcMetadata metadata) {
        try {
            return metadata == null ? null : DatabaseJdbcSupport.buildJdbcUrl(request.getVendor(), metadata);
        } catch (Exception ignore) {
            return null;
        }
    }

    private Properties buildConnectionProperties(DatabaseStorageRegistrationRequest request) {
        if (request.getAuthMethod() == DatabaseAuthMethod.CERTIFICATE) {
            return withNetworkTimeouts(request.getVendor(), DatabaseJdbcSupport.buildCertificateConnectionProperties(request));
        }

        Properties props = new Properties();
        if (request.getUsername() != null) {
            props.setProperty("user", request.getUsername());
        }
        if (request.getPassword() != null) {
            props.setProperty("password", request.getPassword());
        }
        return withNetworkTimeouts(request.getVendor(), props);
    }

    private void logConnectionPlan(DatabaseStorageRegistrationRequest request,
                                   DatabaseJdbcMetadata metadata,
                                   String jdbcUrl,
                                   Properties properties) {
        String line = "[DB 연결 테스트] 계획. "
                + DatabaseConnectionLogFormatter.connectionSummary(
                request.getVendor(),
                request.getAuthMethod(),
                request.getJdbcUrl(),
                request.getPort(),
                request.getUsername(),
                jdbcUrl,
                metadata,
                properties);
        log.info(line);
        System.out.println(line);
    }

    private String buildFailureMessage(String debugJdbcUrl, SQLException e) {
        return "연결 테스트에 실패했어요. jdbcUrl=" + debugJdbcUrl
                + ", reason=" + e.getMessage()
                + ", sqlState=" + e.getSQLState()
                + ", errorCode=" + e.getErrorCode();
    }

    private Properties withNetworkTimeouts(DatabaseVendor vendor, Properties props) {
        Properties copy = new Properties();
        if (props != null) {
            copy.putAll(props);
        }

        switch (vendor) {
            case POSTGRESQL -> {
                copy.putIfAbsent("connectTimeout", "5");
                copy.putIfAbsent("socketTimeout", "5");
                copy.putIfAbsent("loginTimeout", "5");
            }
            case MYSQL, MARIADB -> {
                copy.putIfAbsent("connectTimeout", "5000");
                copy.putIfAbsent("socketTimeout", "5000");
            }
            case ORACLE -> {
                copy.putIfAbsent("oracle.net.CONNECT_TIMEOUT", "5000");
                copy.putIfAbsent("oracle.jdbc.ReadTimeout", "5000");
            }
        }
        return copy;
    }

    private String summarizeThrowableChain(Throwable throwable) {
        List<String> chain = new ArrayList<>();
        Throwable current = throwable;
        while (current != null) {
            chain.add(current.getClass().getSimpleName() + "(" + current.getMessage() + ")");
            current = current.getCause();
        }
        return String.join(" -> ", chain);
    }

    private String normalizeBlankToNull(String value) {
        return value == null || value.isBlank() ? null : value;
    }
}
