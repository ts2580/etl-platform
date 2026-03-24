package com.etlplatform.common.storage.database;

import java.time.LocalDateTime;

public class ExternalDatabaseStorageDefinition {

    private Long storageId;
    private String name;
    private DatabaseVendor vendor;
    private DatabaseAuthMethod authMethod;
    private String jdbcUrl;
    private String host;
    private Integer port;
    private String databaseName;
    private String serviceName;
    private String sid;
    private String username;
    private String passwordEncrypted;
    private String schemaName;
    private String jdbcOptionsJson;
    private String credentialMetaJson;
    private String connectionStatus;
    private Boolean enabled;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;

    public Long getStorageId() {
        return storageId;
    }

    public void setStorageId(Long storageId) {
        this.storageId = storageId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
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

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public String getSid() {
        return sid;
    }

    public void setSid(String sid) {
        this.sid = sid;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPasswordEncrypted() {
        return passwordEncrypted;
    }

    public void setPasswordEncrypted(String passwordEncrypted) {
        this.passwordEncrypted = passwordEncrypted;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getJdbcOptionsJson() {
        return jdbcOptionsJson;
    }

    public void setJdbcOptionsJson(String jdbcOptionsJson) {
        this.jdbcOptionsJson = jdbcOptionsJson;
    }

    public String getCredentialMetaJson() {
        return credentialMetaJson;
    }

    public void setCredentialMetaJson(String credentialMetaJson) {
        this.credentialMetaJson = credentialMetaJson;
    }

    public String getConnectionStatus() {
        return connectionStatus;
    }

    public void setConnectionStatus(String connectionStatus) {
        this.connectionStatus = connectionStatus;
    }

    public Boolean getEnabled() {
        return enabled;
    }

    public void setEnabled(Boolean enabled) {
        this.enabled = enabled;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }

    public LocalDateTime getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(LocalDateTime updatedAt) {
        this.updatedAt = updatedAt;
    }
}
