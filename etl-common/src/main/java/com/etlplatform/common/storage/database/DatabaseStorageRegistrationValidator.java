package com.etlplatform.common.storage.database;

import com.etlplatform.common.error.AppException;

public final class DatabaseStorageRegistrationValidator {

    private DatabaseStorageRegistrationValidator() {
    }

    public static void validate(DatabaseStorageRegistrationRequest request) {
        if (request.getVendor() == null) {
            throw new AppException("DB 종류를 선택해 주세요.");
        }
        if (request.getAuthMethod() == null) {
            throw new AppException("인증 방식을 선택해 주세요.");
        }
        if (request.getJdbcUrl() == null || request.getJdbcUrl().isBlank()) {
            throw new AppException("URL이 비어 있습니다.");
        }
        if (request.getPort() != null && request.getPort() <= 0) {
            throw new AppException("포트는 1 이상 숫자로 입력해 주세요.");
        }
        if (request.getName() == null || request.getName().isBlank()) {
            throw new AppException("저장소명을 입력해 주세요.");
        }
        if (request.getVendor() == DatabaseVendor.ORACLE
                && !OracleStorageSupport.hasSchemaContext(request.getSchemaName(), request.getUsername())) {
            throw new AppException("Oracle은 schemaName 또는 사용자명을 입력해 주세요.");
        }
        if (request.getAuthMethod() == DatabaseAuthMethod.PASSWORD) {
            if (request.getUsername() == null || request.getUsername().isBlank()) {
                throw new AppException("사용자명을 입력해 주세요.");
            }
            if (request.getPassword() == null || request.getPassword().isBlank()) {
                throw new AppException("비밀번호를 입력해 주세요.");
            }
            return;
        }

        validateCertificateAuth(request.getCertificateAuth());
    }

    public static void validateCertificateAuth(DatabaseCertificateAuthRequest certificateAuth) {
        if (certificateAuth == null) {
            throw new AppException("인증서 연결 정보가 비어 있습니다.");
        }
        if (certificateAuth.getTrustStorePath() == null || certificateAuth.getTrustStorePath().isBlank()) {
            throw new AppException("TrustStore 경로를 입력해 주세요.");
        }
        if (certificateAuth.getTrustStorePassword() == null || certificateAuth.getTrustStorePassword().isBlank()) {
            throw new AppException("TrustStore 비밀번호를 입력해 주세요.");
        }
        if (certificateAuth.getKeyStorePath() == null || certificateAuth.getKeyStorePath().isBlank()) {
            throw new AppException("KeyStore 경로를 입력해 주세요.");
        }
        if (certificateAuth.getKeyStorePassword() == null || certificateAuth.getKeyStorePassword().isBlank()) {
            throw new AppException("KeyStore 비밀번호를 입력해 주세요.");
        }
    }
}
