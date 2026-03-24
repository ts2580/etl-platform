package com.etlplatform.common.storage.database;

public class DatabaseStorageRegistrationSupport {

    private final DatabaseCredentialEncryptor encryptor;
    private final DatabaseCertificateMetaCodec certificateMetaCodec;

    public DatabaseStorageRegistrationSupport() {
        this(new DatabaseCredentialEncryptor(), new DatabaseCertificateMetaCodec());
    }

    public DatabaseStorageRegistrationSupport(DatabaseCredentialEncryptor encryptor,
                                              DatabaseCertificateMetaCodec certificateMetaCodec) {
        this.encryptor = encryptor;
        this.certificateMetaCodec = certificateMetaCodec;
    }

    public DatabaseStorageRegistrationDraft buildDraft(DatabaseStorageRegistrationRequest request) {
        DatabaseStorageRegistrationValidator.validate(request);

        DatabaseJdbcMetadata originalMetadata = DatabaseJdbcSupport.parseMetadata(request.getVendor(), request.getJdbcUrl(), request.getPort());
        DatabaseJdbcMetadata jdbcMetadata = originalMetadata;

        String resolvedDatabaseName = normalizeBlankToNull(request.getDatabaseName());
        String resolvedSchemaOrServiceName = normalizeBlankToNull(request.getSchemaName());
        String resolvedUsername = normalizeBlankToNull(request.getUsername());

        if (request.getVendor() == DatabaseVendor.ORACLE) {
            String resolvedServiceName = OracleStorageSupport.resolveJdbcServiceName(
                    originalMetadata.serviceName(),
                    originalMetadata.sid(),
                    resolvedSchemaOrServiceName
            );
            jdbcMetadata = new DatabaseJdbcMetadata(
                    originalMetadata.host(),
                    originalMetadata.port(),
                    originalMetadata.databaseName(),
                    resolvedServiceName,
                    originalMetadata.sid()
            );
            resolvedSchemaOrServiceName = OracleStorageSupport.normalizeSchemaName(
                    resolvedSchemaOrServiceName,
                    resolvedUsername,
                    jdbcMetadata.serviceName(),
                    jdbcMetadata.sid()
            );
        } else if (request.getVendor() == DatabaseVendor.POSTGRESQL || request.getVendor() == DatabaseVendor.MYSQL || request.getVendor() == DatabaseVendor.MARIADB) {
            if (resolvedDatabaseName != null && normalizeBlankToNull(originalMetadata.databaseName()) == null) {
                jdbcMetadata = new DatabaseJdbcMetadata(originalMetadata.host(), originalMetadata.port(), resolvedDatabaseName, originalMetadata.serviceName(), originalMetadata.sid());
            }
        }

        String builtJdbcUrl = DatabaseJdbcSupport.buildJdbcUrl(request.getVendor(), jdbcMetadata);
        String username = normalizeBlankToNull(request.getUsername());
        String passwordEncrypted = request.getAuthMethod() == DatabaseAuthMethod.PASSWORD
                ? encryptor.encrypt(request.getPassword(), request.getEncryptionKey())
                : null;
        String credentialMetaJson = certificateMetaCodec.toJson(request.getCertificateAuth(), encryptor, request.getEncryptionKey());

        return new DatabaseStorageRegistrationDraft(
                request.getVendor(),
                builtJdbcUrl,
                request.getAuthMethod(),
                jdbcMetadata.host(),
                jdbcMetadata.port(),
                jdbcMetadata.databaseName(),
                jdbcMetadata.serviceName(),
                jdbcMetadata.sid(),
                username,
                passwordEncrypted,
                resolvedSchemaOrServiceName,
                credentialMetaJson);
    }

    private String normalizeBlankToNull(String value) {
        return value == null || value.isBlank() ? null : value;
    }
}
