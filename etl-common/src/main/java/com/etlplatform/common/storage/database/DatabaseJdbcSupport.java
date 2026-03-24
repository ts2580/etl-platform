package com.etlplatform.common.storage.database;

import com.etlplatform.common.error.AppException;

import java.net.URI;
import java.util.Locale;
import java.util.Properties;

public final class DatabaseJdbcSupport {

    private static final int DEFAULT_MARIADB_PORT = 3306;
    private static final int DEFAULT_MYSQL_PORT = 3306;
    private static final int DEFAULT_POSTGRESQL_PORT = 5432;
    private static final int DEFAULT_ORACLE_PORT = 1521;

    private DatabaseJdbcSupport() {
    }

    public static DatabaseJdbcMetadata parseMetadata(String jdbcUrl) {
        if (jdbcUrl == null || jdbcUrl.isBlank()) {
            throw new AppException("JDBC URL이 비어 있습니다.");
        }
        if (!jdbcUrl.startsWith("jdbc:")) {
            throw new AppException("JDBC URL 형식이 아닙니다.");
        }
        String remain = jdbcUrl.substring("jdbc:".length());
        int sep = remain.indexOf(':');
        if (sep < 0) {
            throw new AppException("JDBC URL에서 DB 종류를 읽을 수 없습니다.");
        }
        String vendorToken = remain.substring(0, sep);
        DatabaseVendor vendor = DatabaseVendor.valueOf(vendorToken.toUpperCase(Locale.ROOT));
        return parseMetadata(vendor, jdbcUrl, null);
    }

    public static DatabaseJdbcMetadata parseMetadata(DatabaseVendor vendor, String rawUrl) {
        return parseMetadata(vendor, rawUrl, null);
    }

    public static DatabaseJdbcMetadata parseMetadata(DatabaseVendor vendor, String rawUrl, Integer overridePort) {
        if (rawUrl == null || rawUrl.isBlank()) {
            throw new AppException("연결 URL이 비어 있습니다.");
        }

        String normalized = sanitizeInputUrl(rawUrl);
        DatabaseJdbcMetadata parsed = switch (vendor) {
            case ORACLE -> parseOracleMetadata(normalized);
            case MARIADB, MYSQL, POSTGRESQL -> parseTcpMetadata(vendor, normalized);
        };

        int port = overridePort != null ? overridePort : parsed.port();
        return new DatabaseJdbcMetadata(parsed.host(), port, parsed.databaseName(), parsed.serviceName(), parsed.sid());
    }

    public static String buildJdbcUrl(DatabaseVendor vendor, String rawUrl) {
        return buildJdbcUrl(vendor, parseMetadata(vendor, rawUrl));
    }

    public static String buildJdbcUrl(DatabaseVendor vendor, DatabaseJdbcMetadata metadata) {
        if (metadata == null) {
            throw new AppException("연결 메타데이터가 없습니다.");
        }

        String host = normalizeBlankToNull(metadata.host());
        Integer port = metadata.port() == null ? defaultPort(vendor) : metadata.port();
        if (host == null) {
            throw new AppException("호스트가 비어 있습니다.");
        }

        return switch (vendor) {
            case MARIADB -> buildTcpJdbcUrl("mariadb", host, port, metadata.databaseName());
            case MYSQL -> buildTcpJdbcUrl("mysql", host, port, metadata.databaseName());
            case POSTGRESQL -> buildTcpJdbcUrl("postgresql", host, port, metadata.databaseName());
            case ORACLE -> buildOracleJdbcUrl(host, port, metadata.serviceName(), metadata.sid());
        };
    }

    public static Properties buildCertificateConnectionProperties(DatabaseStorageRegistrationRequest request) {
        DatabaseCertificateAuthRequest certificateAuth = request.getCertificateAuth();
        Properties props = new Properties();

        String normalizedSslMode = normalizeSslMode(certificateAuth.getSslMode());
        props.setProperty("ssl", "true");

        String trustStoreType = resolveStoreType(certificateAuth.getTrustStorePath());
        String keyStoreType = resolveStoreType(certificateAuth.getKeyStorePath());

        props.setProperty("javax.net.ssl.trustStore", certificateAuth.getTrustStorePath());
        props.setProperty("javax.net.ssl.trustStorePassword", certificateAuth.getTrustStorePassword());
        if (trustStoreType != null) {
            props.setProperty("javax.net.ssl.trustStoreType", trustStoreType);
        }

        if (certificateAuth.getKeyAlias() != null && !certificateAuth.getKeyAlias().isBlank()) {
            props.setProperty("javax.net.ssl.keyStoreAlias", certificateAuth.getKeyAlias());
        }
        props.setProperty("javax.net.ssl.keyStore", certificateAuth.getKeyStorePath());
        props.setProperty("javax.net.ssl.keyStorePassword", certificateAuth.getKeyStorePassword());
        if (keyStoreType != null) {
            props.setProperty("javax.net.ssl.keyStoreType", keyStoreType);
        }

        if (request.getUsername() != null && !request.getUsername().isBlank()) {
            props.setProperty("user", request.getUsername());
        }
        if (request.getPassword() != null && !request.getPassword().isBlank()) {
            props.setProperty("password", request.getPassword());
        }

        applyVendorProperties(props, request.getVendor(), normalizedSslMode);
        return props;
    }

    public static Properties buildStoredConnectionProperties(ExternalDatabaseStorageDefinition storage,
                                                             DatabaseCredentialEncryptor encryptor,
                                                             DatabaseCertificateMetaCodec metaCodec,
                                                             String userKey) {
        Properties props = new Properties();
        if (storage.getUsername() != null && !storage.getUsername().isBlank()) {
            props.setProperty("user", storage.getUsername());
        }

        if (storage.getAuthMethod() == DatabaseAuthMethod.CERTIFICATE) {
            applyStoredCertificateProperties(props, storage, encryptor, metaCodec, userKey);
            return props;
        }

        if (storage.getPasswordEncrypted() != null && !storage.getPasswordEncrypted().isBlank()) {
            props.setProperty("password", encryptor.decryptToString(storage.getPasswordEncrypted(), userKey).trim());
        }
        return props;
    }

    public static void loadDriver(DatabaseVendor vendor) {
        try {
            Class.forName(resolveDriverClassName(vendor));
        } catch (ClassNotFoundException e) {
            throw new AppException("선택한 DB 드라이버가 현재 서버에 준비되지 않았어요: " + vendor, e);
        }
    }

    public static String resolveDriverClassName(DatabaseVendor vendor) {
        return switch (vendor) {
            case MARIADB -> "org.mariadb.jdbc.Driver";
            case MYSQL -> "com.mysql.cj.jdbc.Driver";
            case ORACLE -> "oracle.jdbc.OracleDriver";
            case POSTGRESQL -> "org.postgresql.Driver";
        };
    }

    public static String normalizeSslMode(String sslMode) {
        if (sslMode == null || sslMode.isBlank()) {
            return "VERIFY_FULL";
        }
        String normalized = sslMode.trim().toLowerCase(Locale.ROOT);
        return switch (normalized) {
            case "require", "required", "verify-ca", "verify_ca", "verify-identity", "verify_identity",
                    "verify-full", "verify_full", "verifyfull", "requirefull", "strict", "strict-ssl",
                    "strict_ssl", "allow" -> normalized.toUpperCase(Locale.ROOT).replace('_', '-');
            case "disable", "disabled", "false", "off", "0" -> "DISABLED";
            default -> sslMode;
        };
    }

    public static boolean shouldVerifyServerCertificate(String sslMode) {
        if (sslMode == null) {
            return false;
        }
        String normalized = sslMode.toUpperCase(Locale.ROOT);
        return normalized.contains("VERIFY")
                || "REQUIRED".equals(normalized)
                || "VERIFY_CA".equals(normalized)
                || "VERIFY_FULL".equals(normalized);
    }

    public static String resolveStoreType(String path) {
        if (path == null || path.isBlank()) {
            return "JKS";
        }
        String lower = path.toLowerCase(Locale.ROOT);
        if (lower.endsWith(".p12") || lower.endsWith(".pfx")) {
            return "PKCS12";
        }
        if (lower.endsWith(".jceks")) {
            return "JCEKS";
        }
        return "JKS";
    }

    private static DatabaseJdbcMetadata parseTcpMetadata(DatabaseVendor vendor, String rawUrl) {
        String target = stripJdbcPrefix(rawUrl, vendor);
        target = stripScheme(target);
        URI uri = parseHostPortUri(target);

        String host = normalizeBlankToNull(uri.getHost());
        Integer port = uri.getPort() > 0 ? uri.getPort() : defaultPort(vendor);
        String databaseName = extractPathFirstSegment(uri.getRawPath());

        if (host == null) {
            throw new AppException("호스트를 추출할 수 없습니다. 입력: " + rawUrl);
        }

        return new DatabaseJdbcMetadata(host, port, databaseName, null, null);
    }

    private static DatabaseJdbcMetadata parseOracleMetadata(String rawUrl) {
        String target = stripJdbcAndOraclePrefix(rawUrl);
        target = stripScheme(target);

        String addressPart;
        String serviceName = null;
        String sid = null;

        if (target.contains("/")) {
            String[] split = target.split("/", 2);
            addressPart = split[0];
            String token = extractPathFirstSegment("/" + split[1]);
            serviceName = normalizeBlankToNull(token);
        } else if (target.contains(":")) {
            int sidStart = target.lastIndexOf(':');
            if (sidStart <= 0 || sidStart + 1 >= target.length()) {
                throw new AppException("Oracle SID/서비스명 형식을 확인해 주세요. 입력: " + rawUrl);
            }
            addressPart = target.substring(0, sidStart);
            sid = normalizeBlankToNull(target.substring(sidStart + 1));
        } else {
            // host만 입력되는 경우: 스키마/서비스명은 별도 입력값(schemaName)에서 받는다고 보고 host/port만 파싱
            addressPart = target;
            serviceName = null;
            sid = null;
        }

        URI uri = parseHostPortUri(addressPart);
        String host = normalizeBlankToNull(uri.getHost());
        Integer port = uri.getPort() > 0 ? uri.getPort() : DEFAULT_ORACLE_PORT;

        if (host == null) {
            throw new AppException("호스트를 추출할 수 없습니다. 입력: " + rawUrl);
        }

        return new DatabaseJdbcMetadata(host, port, null, serviceName, sid);
    }

    private static URI parseHostPortUri(String target) {
        try {
            String candidate = target.startsWith("//") ? "jdbc:" + target : "jdbc://" + target;
            URI uri = URI.create(candidate);
            if (uri.getHost() == null || uri.getHost().isBlank()) {
                throw new AppException("주소 파싱에 실패했어요. 입력: " + target);
            }
            return uri;
        } catch (Exception e) {
            if (e instanceof AppException appException) {
                throw appException;
            }
            throw new AppException("연결 URL 파싱에 실패했어요: " + target, e);
        }
    }

    private static String stripJdbcPrefix(String rawUrl, DatabaseVendor vendor) {
        String target = rawUrl.trim();

        if (target.startsWith("jdbc:")) {
            target = target.substring("jdbc:".length());
        }

        String lower = vendor.name().toLowerCase(Locale.ROOT);
        String[] prefixes = {
                lower + "://",
                lower + ":"
        };

        for (String prefix : prefixes) {
            if (target.startsWith(prefix)) {
                target = target.substring(prefix.length());
                break;
            }
        }

        if (target.startsWith("://")) {
            target = target.substring(3);
        } else if (target.startsWith(":")) {
            target = target.substring(1);
        }
        if (target.startsWith("<") && target.endsWith(">")) {
            target = target.substring(1, target.length() - 1);
        }
        if (target.startsWith("<") && target.contains("|")) {
            int sep = target.lastIndexOf('|');
            target = target.substring(sep + 1, target.length() - 1);
        }
        return target;
    }

    private static String stripJdbcAndOraclePrefix(String rawUrl) {
        String target = rawUrl.trim();
        String[] prefixes = {
                "jdbc:oracle:thin:@//",
                "jdbc:oracle:thin:@",
                "oracle:thin:@//",
                "oracle:thin:@"
        };

        for (String prefix : prefixes) {
            if (target.startsWith(prefix)) {
                target = target.substring(prefix.length());
                break;
            }
        }
        return target;
    }

    private static String stripScheme(String target) {
        String normalized = target;
        if (normalized.startsWith("//")) {
            return normalized;
        }
        int idx = normalized.indexOf("://");
        if (idx >= 0) {
            normalized = normalized.substring(idx + 3);
        }
        return normalized;
    }

    private static String extractPathFirstSegment(String rawPath) {
        if (rawPath == null || rawPath.isBlank() || "/".equals(rawPath.trim())) {
            return null;
        }
        String path = rawPath;
        if (path.startsWith("/")) {
            path = path.substring(1);
        }
        int slash = path.indexOf('/');
        if (slash >= 0) {
            path = path.substring(0, slash);
        }
        return normalizeBlankToNull(path);
    }

    private static int defaultPort(DatabaseVendor vendor) {
        return switch (vendor) {
            case ORACLE -> DEFAULT_ORACLE_PORT;
            case POSTGRESQL -> DEFAULT_POSTGRESQL_PORT;
            case MYSQL -> DEFAULT_MYSQL_PORT;
            case MARIADB -> DEFAULT_MARIADB_PORT;
        };
    }

    private static String buildTcpJdbcUrl(String vendorToken, String host, Integer port, String databaseName) {
        String dbName = normalizeBlankToNull(databaseName);
        if (dbName == null) {
            return "jdbc:" + vendorToken + "://" + host + ":" + port;
        }
        return "jdbc:" + vendorToken + "://" + host + ":" + port + "/" + dbName;
    }

    private static String buildOracleJdbcUrl(String host, Integer port, String serviceName, String sid) {
        if (normalizeBlankToNull(serviceName) != null) {
            return "jdbc:oracle:thin:@//" + host + ":" + port + "/" + serviceName;
        }
        if (normalizeBlankToNull(sid) != null) {
            return "jdbc:oracle:thin:@" + host + ":" + port + ":" + sid;
        }
        return "jdbc:oracle:thin:@//" + host + ":" + port;
    }

    private static String normalizeBlankToNull(String value) {
        return value == null || value.isBlank() ? null : value;
    }

    private static String sanitizeInputUrl(String rawUrl) {
        if (rawUrl == null) {
            return null;
        }

        String normalized = rawUrl.trim();
        if (normalized.isBlank()) {
            return normalized;
        }

        normalized = normalized.replaceAll("\\s", "");

        if (normalized.startsWith("jdbc:")) {
            normalized = normalized.substring("jdbc:".length());
        }

        normalized = unwrapSlackLink(normalized);

        int lt = normalized.indexOf('<');
        int gt = normalized.lastIndexOf('>');
        if (lt >= 0 && gt > lt) {
            normalized = unwrapSlackLink(normalized.substring(lt, gt + 1));
        }

        normalized = normalized.replace("<", "").replace(">", "");

        String lower = normalized.toLowerCase(Locale.ROOT);
        if (lower.startsWith("http://") || lower.startsWith("https://")) {
            try {
                URI uri = URI.create(normalized);
                String authority = uri.getRawAuthority();
                if (authority != null && !authority.isBlank()) {
                    normalized = authority + (uri.getRawPath() == null ? "" : uri.getRawPath());
                }
            } catch (Exception ignore) {
                // keep raw if invalid
            }
        }

        int schemeSep = normalized.indexOf("://");
        if (schemeSep >= 0) {
            normalized = normalized.substring(schemeSep + 3);
        }

        if (normalized.startsWith("//")) {
            normalized = normalized.substring(2);
        }

        int hash = normalized.indexOf('#');
        if (hash >= 0) {
            normalized = normalized.substring(0, hash);
        }

        int query = normalized.indexOf('?');
        if (query >= 0) {
            normalized = normalized.substring(0, query);
        }

        while (normalized.endsWith("/")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }

        return normalized.isBlank() ? null : normalized;
    }

    private static String unwrapSlackLink(String value) {
        String normalized = value;
        if (normalized == null || normalized.isBlank()) {
            return normalized;
        }

        if (normalized.startsWith("<") && normalized.endsWith(">")) {
            normalized = normalized.substring(1, normalized.length() - 1);
        }

        int pipe = normalized.indexOf('|');
        if (pipe >= 0) {
            normalized = normalized.substring(0, pipe);
        }

        return normalized;
    }

    private static void applyStoredCertificateProperties(Properties props,
                                                       ExternalDatabaseStorageDefinition storage,
                                                       DatabaseCredentialEncryptor encryptor,
                                                       DatabaseCertificateMetaCodec metaCodec,
                                                       String userKey) {
        StoredCertificateCredentialMeta meta = metaCodec.parse(storage.getCredentialMetaJson());
        if (meta == null) {
            throw new AppException("인증서 메타데이터가 비어 있습니다. storageId=" + storage.getStorageId());
        }

        props.setProperty("ssl", "true");
        props.setProperty("javax.net.ssl.trustStore", meta.getTrustStorePath());
        props.setProperty("javax.net.ssl.trustStorePassword",
                encryptor.decryptToString(meta.getTrustStorePasswordEncrypted(), userKey));
        props.setProperty("javax.net.ssl.keyStore", meta.getKeyStorePath());
        props.setProperty("javax.net.ssl.keyStorePassword",
                encryptor.decryptToString(meta.getKeyStorePasswordEncrypted(), userKey));

        String trustStoreType = resolveStoreType(meta.getTrustStorePath());
        String keyStoreType = resolveStoreType(meta.getKeyStorePath());
        if (trustStoreType != null) {
            props.setProperty("javax.net.ssl.trustStoreType", trustStoreType);
        }
        if (keyStoreType != null) {
            props.setProperty("javax.net.ssl.keyStoreType", keyStoreType);
        }
        if (meta.getKeyAlias() != null && !meta.getKeyAlias().isBlank()) {
            props.setProperty("javax.net.ssl.keyStoreAlias", meta.getKeyAlias());
        }
        if (storage.getPasswordEncrypted() != null && !storage.getPasswordEncrypted().isBlank()) {
            props.setProperty("password", encryptor.decryptToString(storage.getPasswordEncrypted(), userKey));
        }

        applyVendorProperties(props, storage.getVendor(), normalizeSslMode(meta.getSslMode()));
    }

    private static void applyVendorProperties(Properties props, DatabaseVendor vendor, String sslMode) {
        switch (vendor) {
            case POSTGRESQL -> applyPostgreSqlProperties(props, sslMode);
            case MARIADB, MYSQL -> applyMySqlProperties(props, sslMode);
            case ORACLE -> applyOracleProperties(props, sslMode);
        }
    }

    private static void applyMySqlProperties(Properties props, String sslMode) {
        props.setProperty("useSSL", "true");
        props.setProperty("requireSSL", "true");
        props.setProperty("sslMode", sslMode);
        props.setProperty("verifyServerCertificate", shouldVerifyServerCertificate(sslMode) ? "true" : "false");
        if (shouldVerifyServerCertificate(sslMode)) {
            props.setProperty("trustCertificateKeyStoreType", "JKS");
            props.setProperty("clientCertificateKeyStoreType", "JKS");
        }
    }

    private static void applyPostgreSqlProperties(Properties props, String sslMode) {
        props.setProperty("sslmode", sslMode == null || sslMode.isBlank() ? "VERIFY-FULL" : sslMode);
        props.setProperty("ssl", "true");
    }

    private static void applyOracleProperties(Properties props, String sslMode) {
        props.setProperty("oracle.jdbc.ssl_server_dn_match", "true");
        if (props.getProperty("javax.net.ssl.trustStore") != null) {
            props.setProperty("javax.net.ssl.trustStoreType", resolveStoreType(props.getProperty("javax.net.ssl.trustStore")));
        }
        if (props.getProperty("javax.net.ssl.keyStore") != null) {
            props.setProperty("javax.net.ssl.keyStoreType", resolveStoreType(props.getProperty("javax.net.ssl.keyStore")));
        }
        if (sslMode != null && !sslMode.isBlank()) {
            props.setProperty("oracle.net.ssl_server_cert_dn", sslMode);
        }
    }
}
