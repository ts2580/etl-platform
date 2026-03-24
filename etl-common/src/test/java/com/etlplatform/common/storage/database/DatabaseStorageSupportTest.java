package com.etlplatform.common.storage.database;

import com.etlplatform.common.error.AppException;
import com.etlplatform.common.storage.database.sql.OracleDatabaseVendorStrategy;
import org.junit.jupiter.api.Test;

import java.sql.Types;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DatabaseStorageSupportTest {

    @Test
    void registrationSupportBuildsDraftFromJdbcUrlAndEncryptedCredentials() {
        DatabaseStorageRegistrationRequest request = new DatabaseStorageRegistrationRequest();
        request.setName("warehouse");
        request.setEncryptionKey("super-secret-key");
        request.setVendor(DatabaseVendor.POSTGRESQL);
        request.setAuthMethod(DatabaseAuthMethod.PASSWORD);
        request.setJdbcUrl("jdbc:postgresql://db.example.com:5432/appdb");
        request.setUsername("etl_user");
        request.setPassword("pw-123");
        request.setSchemaName("config");

        DatabaseStorageRegistrationDraft draft = new DatabaseStorageRegistrationSupport().buildDraft(request);

        assertEquals(DatabaseVendor.POSTGRESQL, draft.vendor());
        assertEquals(DatabaseAuthMethod.PASSWORD, draft.authMethod());
        assertEquals("db.example.com", draft.host());
        assertEquals(5432, draft.port());
        assertEquals("etl_user", draft.username());
        assertNotNull(draft.passwordEncrypted());
        assertNull(draft.credentialMetaJson());
    }

    @Test
    void certificateMetaCodecRoundTripsEncryptedPasswords() {
        DatabaseCredentialEncryptor encryptor = new DatabaseCredentialEncryptor();
        DatabaseCertificateMetaCodec codec = new DatabaseCertificateMetaCodec();

        DatabaseCertificateAuthRequest request = new DatabaseCertificateAuthRequest();
        request.setTrustStorePath("/tmp/truststore.p12");
        request.setTrustStorePassword("trust-pass");
        request.setKeyStorePath("/tmp/keystore.jks");
        request.setKeyStorePassword("key-pass");
        request.setKeyAlias("etl");
        request.setSslMode("verify-full");

        String json = codec.toJson(request, encryptor, "super-secret-key");
        StoredCertificateCredentialMeta stored = codec.parse(json);
        DecryptedStoredCertificateCredentialMeta decrypted = codec.decrypt(stored, "super-secret-key", encryptor);

        assertEquals("/tmp/truststore.p12", decrypted.trustStorePath());
        assertEquals("trust-pass", decrypted.trustStorePassword());
        assertEquals("/tmp/keystore.jks", decrypted.keyStorePath());
        assertEquals("key-pass", decrypted.keyStorePassword());
        assertEquals("etl", decrypted.keyAlias());
        assertEquals("verify-full", decrypted.sslMode());
    }

    @Test
    void storedConnectionPropertiesApplyCertificateSettings() {
        DatabaseCredentialEncryptor encryptor = new DatabaseCredentialEncryptor();
        DatabaseCertificateMetaCodec codec = new DatabaseCertificateMetaCodec();

        ExternalDatabaseStorageDefinition storage = new ExternalDatabaseStorageDefinition();
        storage.setStorageId(10L);
        storage.setVendor(DatabaseVendor.MYSQL);
        storage.setAuthMethod(DatabaseAuthMethod.CERTIFICATE);
        storage.setUsername("etl_user");
        storage.setPasswordEncrypted(encryptor.encrypt("pw-123", "super-secret-key"));

        DatabaseCertificateAuthRequest request = new DatabaseCertificateAuthRequest();
        request.setTrustStorePath("/tmp/truststore.jks");
        request.setTrustStorePassword("trust-pass");
        request.setKeyStorePath("/tmp/keystore.p12");
        request.setKeyStorePassword("key-pass");
        request.setKeyAlias("client");
        request.setSslMode("verify-full");
        storage.setCredentialMetaJson(codec.toJson(request, encryptor, "super-secret-key"));

        Properties props = DatabaseJdbcSupport.buildStoredConnectionProperties(
                storage,
                encryptor,
                codec,
                "super-secret-key");

        assertEquals("etl_user", props.getProperty("user"));
        assertEquals("pw-123", props.getProperty("password"));
        assertEquals("/tmp/truststore.jks", props.getProperty("javax.net.ssl.trustStore"));
        assertEquals("trust-pass", props.getProperty("javax.net.ssl.trustStorePassword"));
        assertEquals("JKS", props.getProperty("trustCertificateKeyStoreType"));
        assertEquals("VERIFY-FULL", props.getProperty("sslMode"));
        assertTrue(Boolean.parseBoolean(props.getProperty("useSSL")));
    }

    @Test
    void oracleRegistrationUsesUsernameAsSchemaAndPreservesSid() {
        DatabaseStorageRegistrationRequest request = new DatabaseStorageRegistrationRequest();
        request.setName("oracle-target");
        request.setEncryptionKey("super-secret-key");
        request.setVendor(DatabaseVendor.ORACLE);
        request.setAuthMethod(DatabaseAuthMethod.PASSWORD);
        request.setJdbcUrl("jdbc:oracle:thin:@db.example.com:1521:XE");
        request.setUsername("etl_user");
        request.setPassword("pw-123");
        request.setSchemaName("XE");

        DatabaseStorageRegistrationDraft draft = new DatabaseStorageRegistrationSupport().buildDraft(request);

        assertEquals("jdbc:oracle:thin:@db.example.com:1521:XE", draft.jdbcUrl());
        assertEquals("XE", draft.sid());
        assertNull(draft.serviceName());
        assertEquals("ETL_USER", draft.schemaName());
    }

    @Test
    void oracleValidatorRequiresSchemaContext() {
        DatabaseStorageRegistrationRequest request = new DatabaseStorageRegistrationRequest();
        request.setName("oracle-target");
        request.setVendor(DatabaseVendor.ORACLE);
        request.setAuthMethod(DatabaseAuthMethod.CERTIFICATE);
        request.setJdbcUrl("jdbc:oracle:thin:@//db.example.com:1521/service");
        request.setCertificateAuth(new DatabaseCertificateAuthRequest());

        AppException exception = assertThrows(AppException.class, () -> DatabaseStorageRegistrationValidator.validate(request));
        assertEquals("Oracle은 schemaName 또는 사용자명을 입력해 주세요.", exception.getMessage());
    }

    @Test
    void oracleStrategyBuildsMergeWithExplicitAliasesAndClobBinding() {
        OracleDatabaseVendorStrategy strategy = new OracleDatabaseVendorStrategy();

        String sql = strategy.buildUpsertSql(
                strategy.qualifyTableName("etl_user", "Account"),
                List.of("sfid", "Description", "_sf_last_modified_at", "_sf_last_event_at"),
                List.of("Description"),
                "sfid",
                "_sf_last_modified_at",
                "_sf_last_event_at"
        );

        assertTrue(sql.contains("SELECT ? AS \"sfid\""));
        assertTrue(sql.contains("? AS \"Description\""));
        assertEquals(Types.CLOB, strategy.bindValue("long text", "textarea").sqlType());
    }
}
