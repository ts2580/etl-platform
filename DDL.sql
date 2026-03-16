-- etl-platform base DDL
-- MariaDB/MySQL compatible bootstrap script
--
-- NOTE:
-- 1) MariaDB does not support CREATE OR REPLACE TABLE in the same way views do.
--    So for 'replace' semantics, this script uses DROP TABLE IF EXISTS + CREATE TABLE.
-- 2) Dynamic Salesforce object tables (for streaming / CDC) are created by application runtime
--    across each tenant/purpose schema, so they are not fully enumerated here.

CREATE DATABASE IF NOT EXISTS config CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE DATABASE IF NOT EXISTS mig CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE DATABASE IF NOT EXISTS routing CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- =========================================================
-- config.member
-- main-app local user / signup / login
-- =========================================================
DROP TABLE IF EXISTS config.member;
CREATE TABLE config.member (
    id INT NOT NULL AUTO_INCREMENT,
    name VARCHAR(100) NOT NULL,
    username VARCHAR(100) NOT NULL,
    password VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL,
    description TEXT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_member_username (username),
    UNIQUE KEY uk_member_email (email)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- =========================================================
-- config.salesforce_org_credentials
-- 멀티 org 운영용 refresh key/endpoint 관리
-- =========================================================
DROP TABLE IF EXISTS config.salesforce_org_credentials;
CREATE TABLE config.salesforce_org_credentials (
    id BIGINT NOT NULL AUTO_INCREMENT,
    org_key VARCHAR(255) NOT NULL,
    org_name VARCHAR(255) NOT NULL,
    my_domain VARCHAR(512) NOT NULL,
    schema_name VARCHAR(255) NULL,
    client_id VARCHAR(255),
    client_secret VARCHAR(255),
    access_token TEXT,
    refresh_token TEXT NOT NULL,
    access_token_issued_at DATETIME NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    is_default BOOLEAN NOT NULL DEFAULT FALSE,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_sf_org_credentials_key (org_key),
    KEY idx_sf_org_credentials_active (is_active),
    KEY idx_sf_org_credentials_default (is_default)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- =========================================================
-- routing.routing_registry
-- single source of truth for active/inactive Salesforce routing state
-- STREAMING / CDC 공통 관리용
-- =========================================================
DROP TABLE IF EXISTS routing.routing_registry;
CREATE TABLE routing.routing_registry (
    id BIGINT NOT NULL AUTO_INCREMENT,
    org_key VARCHAR(255) NOT NULL COMMENT 'Salesforce org 식별자(예: orgId 또는 myDomain)',
    org_name VARCHAR(255) NULL COMMENT '표시용 org 이름',
    my_domain VARCHAR(255) NULL COMMENT '예: https://xxx.my.salesforce.com',
    target_schema VARCHAR(255) NOT NULL COMMENT '오브젝트 저장 대상 스키마',
    target_table VARCHAR(255) NOT NULL COMMENT '오브젝트 저장 대상 테이블',
    instance_name VARCHAR(100) NULL COMMENT '예: AP16',
    org_type VARCHAR(100) NULL COMMENT '예: Developer Edition, Enterprise Edition',
    is_sandbox BOOLEAN NOT NULL DEFAULT FALSE COMMENT 'sandbox 여부',
    selected_object VARCHAR(255) NOT NULL COMMENT 'Salesforce object API name',
    object_label VARCHAR(255) NULL COMMENT 'Salesforce object label',
    routing_protocol VARCHAR(20) NOT NULL COMMENT 'STREAMING or CDC',
    routing_endpoint VARCHAR(100) NOT NULL COMMENT '예: /streaming, /pubsub',
    routing_status VARCHAR(30) NOT NULL DEFAULT 'ACTIVE' COMMENT 'ACTIVE, INACTIVE, FAILED, RELEASED',
    source_status VARCHAR(30) NOT NULL DEFAULT 'ACTIVE' COMMENT 'Salesforce 실제 상태와 동기화 결과',
    initial_load_count INT NOT NULL DEFAULT 0 COMMENT '최초 적재 건수',
    last_error_message TEXT NULL COMMENT '마지막 실패 메시지',
    activated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '최초 활성화 시각',
    released_at DATETIME NULL COMMENT '해지 시각',
    last_synced_at DATETIME NULL COMMENT 'Salesforce/엔진과 마지막 동기화 시각',
    created_by VARCHAR(100) NULL COMMENT '등록 사용자 id/username',
    updated_by VARCHAR(100) NULL COMMENT '마지막 수정 사용자 id/username',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_routing_registry_org_object_protocol (org_key, selected_object, routing_protocol),
    KEY idx_routing_registry_status (routing_status),
    KEY idx_routing_registry_protocol (routing_protocol),
    KEY idx_routing_registry_object (selected_object),
    KEY idx_routing_registry_org_key (org_key),
    KEY idx_routing_registry_target_schema (target_schema),
    KEY idx_routing_registry_target_table (target_table)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- =========================================================
-- routing.routing_history
-- 라우팅 등록/재처리/해지/실패 이력
-- =========================================================
DROP TABLE IF EXISTS routing.routing_history;
CREATE TABLE routing.routing_history (
    id BIGINT NOT NULL AUTO_INCREMENT,
    routing_registry_id BIGINT NULL COMMENT 'routing.routing_registry.id',
    org_key VARCHAR(255) NOT NULL,
    selected_object VARCHAR(255) NOT NULL,
    target_schema VARCHAR(255) NULL,
    target_table VARCHAR(255) NULL,
    routing_protocol VARCHAR(20) NOT NULL COMMENT 'STREAMING or CDC',
    event_type VARCHAR(30) NOT NULL COMMENT 'REGISTER, REPROCESS, RELEASE, FAIL',
    event_status VARCHAR(30) NOT NULL COMMENT 'SUCCESS, FAILED, STARTED, SKIPPED',
    event_stage VARCHAR(50) NULL COMMENT '예: CDC_CHANNEL, INITIAL_LOAD, SUBSCRIBE',
    endpoint VARCHAR(100) NULL COMMENT '예: /streaming, /pubsub, /pubsub/slots/deactivate',
    message TEXT NULL COMMENT '요약 메시지',
    detail_text LONGTEXT NULL COMMENT '응답 원문/스택/추가 detail',
    initial_load_count INT NOT NULL DEFAULT 0,
    actor VARCHAR(100) NULL COMMENT '작업 유발 사용자 또는 시스템',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    KEY idx_routing_history_registry_id (routing_registry_id),
    KEY idx_routing_history_org_object (org_key, selected_object),
    KEY idx_routing_history_protocol (routing_protocol),
    KEY idx_routing_history_event_type (event_type),
    KEY idx_routing_history_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- =========================================================
-- routing.ingestion_slot_registry
-- 공통 ingestion slot registry (CDC/Streaming 모두 관리)
-- 기존 코드 호환 + 운영 메타 확장
-- =========================================================
DROP TABLE IF EXISTS routing.ingestion_slot_registry;
CREATE TABLE routing.ingestion_slot_registry (
    id BIGINT NOT NULL AUTO_INCREMENT,
    org_key VARCHAR(255) NULL COMMENT '멀티 org 대응용 식별자',
    selected_object VARCHAR(255) NOT NULL,
    routing_protocol VARCHAR(20) NOT NULL DEFAULT 'CDC',
    routing_registry_id BIGINT NULL COMMENT 'routing.routing_registry.id',
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    activated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deactivated_at DATETIME NULL,
    note VARCHAR(500) NULL COMMENT '운영 메모',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_ingestion_slot_registry_protocol_object (routing_protocol, selected_object),
    KEY idx_ingestion_slot_registry_is_active (is_active),
    KEY idx_ingestion_slot_registry_protocol (routing_protocol),
    KEY idx_ingestion_slot_registry_org_key (org_key),
    KEY idx_ingestion_slot_registry_routing_registry_id (routing_registry_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
