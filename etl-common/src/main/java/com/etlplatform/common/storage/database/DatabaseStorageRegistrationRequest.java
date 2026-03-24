package com.etlplatform.common.storage.database;

import com.fasterxml.jackson.annotation.JsonAlias;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

public class DatabaseStorageRegistrationRequest {

    @NotBlank
    private String name;

    private String encryptionKey;

    private String description;

    @NotNull
    private DatabaseVendor vendor;

    @NotNull
    private DatabaseAuthMethod authMethod;

    @NotBlank
    @JsonAlias("url")
    private String jdbcUrl;

    @Positive
    private Integer port;

    private String username;
    private String password;
    private String schemaName;
    private String databaseName;
    private DatabaseCertificateAuthRequest certificateAuth;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getEncryptionKey() {
        return encryptionKey;
    }

    public void setEncryptionKey(String encryptionKey) {
        this.encryptionKey = encryptionKey;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public DatabaseVendor getVendor() {
        return vendor;
    }

    public void setVendor(DatabaseVendor vendor) {
        this.vendor = vendor;
    }

    public DatabaseAuthMethod getAuthMethod() {
        return authMethod;
    }

    public void setAuthMethod(DatabaseAuthMethod authMethod) {
        this.authMethod = authMethod;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public DatabaseCertificateAuthRequest getCertificateAuth() {
        return certificateAuth;
    }

    public void setCertificateAuth(DatabaseCertificateAuthRequest certificateAuth) {
        this.certificateAuth = certificateAuth;
    }
}
