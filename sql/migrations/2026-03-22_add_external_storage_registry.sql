-- 외부 저장소 등록 레지스트리 추가
-- 대상: main 모듈이 사용하는 config DB
-- 범위: DATABASE 저장소 등록 메타데이터 + DB 전용 상세 설정

CREATE DATABASE IF NOT EXISTS config CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS config.external_storage (
    id BIGINT NOT NULL AUTO_INCREMENT,
    org_key VARCHAR(255) NULL COMMENT '선택된 Salesforce org와 매핑이 필요할 때 사용',
    storage_type VARCHAR(30) NOT NULL COMMENT 'DATABASE, NFS, S3, WEBDAV, SFTP',
    name VARCHAR(100) NOT NULL COMMENT '외부 저장소 표시 이름',
    description VARCHAR(500) NULL COMMENT '설명/운영 메모',
    enabled BOOLEAN NOT NULL DEFAULT TRUE COMMENT '사용 여부',
    connection_status VARCHAR(30) NOT NULL DEFAULT 'PENDING' COMMENT 'PENDING, VERIFIED, FAILED, DISABLED',
    last_tested_at DATETIME NULL COMMENT '마지막 연결 테스트 시각',
    last_test_success BOOLEAN NULL COMMENT '마지막 연결 테스트 성공 여부',
    last_error_message TEXT NULL COMMENT '마지막 연결 실패 메시지',
    created_by VARCHAR(100) NULL COMMENT '등록 사용자',
    updated_by VARCHAR(100) NULL COMMENT '수정 사용자',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_external_storage_name (name),
    KEY idx_external_storage_org_key (org_key),
    KEY idx_external_storage_type (storage_type),
    KEY idx_external_storage_enabled (enabled),
    KEY idx_external_storage_connection_status (connection_status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS config.external_database_storage (
    storage_id BIGINT NOT NULL,
    vendor VARCHAR(30) NOT NULL COMMENT 'MARIADB, MYSQL, ORACLE, POSTGRESQL',
    jdbc_url VARCHAR(2000) NOT NULL COMMENT '초기 등록용 JDBC URL',
    auth_method VARCHAR(30) NOT NULL DEFAULT 'PASSWORD' COMMENT 'PASSWORD, CERTIFICATE',
    host VARCHAR(255) NOT NULL,
    port INT NOT NULL,
    database_name VARCHAR(255) NULL COMMENT 'MariaDB/MySQL/PostgreSQL 기본 DB명',
    service_name VARCHAR(255) NULL COMMENT 'Oracle service name',
    sid VARCHAR(255) NULL COMMENT 'Oracle SID',
    username VARCHAR(255) NOT NULL,
    password_encrypted VARCHAR(2000) NOT NULL COMMENT '암호화된 비밀번호',
    schema_name VARCHAR(255) NULL,
    jdbc_options_json TEXT NULL COMMENT 'JDBC 옵션 JSON 문자열',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (storage_id),
    CONSTRAINT fk_external_database_storage_storage
        FOREIGN KEY (storage_id) REFERENCES config.external_storage(id)
        ON DELETE CASCADE,
    KEY idx_external_database_storage_vendor (vendor),
    KEY idx_external_database_storage_host_port (host, port),
    KEY idx_external_database_storage_database_name (database_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS config.external_storage_connection_history (
    id BIGINT NOT NULL AUTO_INCREMENT,
    storage_id BIGINT NULL,
    event_type VARCHAR(30) NOT NULL COMMENT 'TEST, REGISTER, UPDATE, DISABLE',
    success BOOLEAN NOT NULL,
    message TEXT NULL,
    detail_text LONGTEXT NULL,
    tested_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    CONSTRAINT fk_external_storage_connection_history_storage
        FOREIGN KEY (storage_id) REFERENCES config.external_storage(id)
        ON DELETE CASCADE,
    KEY idx_external_storage_connection_history_storage_id (storage_id),
    KEY idx_external_storage_connection_history_tested_at (tested_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
