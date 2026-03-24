package com.etlplatform.common.storage.database;

public class StoredCertificateCredentialMeta {

    private String trustStorePath;
    private String trustStorePasswordEncrypted;
    private String keyStorePath;
    private String keyStorePasswordEncrypted;
    private String keyAlias;
    private String sslMode;

    public String getTrustStorePath() {
        return trustStorePath;
    }

    public void setTrustStorePath(String trustStorePath) {
        this.trustStorePath = trustStorePath;
    }

    public String getTrustStorePasswordEncrypted() {
        return trustStorePasswordEncrypted;
    }

    public void setTrustStorePasswordEncrypted(String trustStorePasswordEncrypted) {
        this.trustStorePasswordEncrypted = trustStorePasswordEncrypted;
    }

    public String getKeyStorePath() {
        return keyStorePath;
    }

    public void setKeyStorePath(String keyStorePath) {
        this.keyStorePath = keyStorePath;
    }

    public String getKeyStorePasswordEncrypted() {
        return keyStorePasswordEncrypted;
    }

    public void setKeyStorePasswordEncrypted(String keyStorePasswordEncrypted) {
        this.keyStorePasswordEncrypted = keyStorePasswordEncrypted;
    }

    public String getKeyAlias() {
        return keyAlias;
    }

    public void setKeyAlias(String keyAlias) {
        this.keyAlias = keyAlias;
    }

    public String getSslMode() {
        return sslMode;
    }

    public void setSslMode(String sslMode) {
        this.sslMode = sslMode;
    }
}
