-- routing_registry에 target storage 컬럼 복구 DDL
-- 대상: routing.routing_registry
-- 증상: SQL SELECT에서 target_storage_id/target_storage_name 조회 시 Unknown column 에러 발생

ALTER TABLE routing.routing_registry
    ADD COLUMN IF NOT EXISTS target_storage_id BIGINT NULL COMMENT 'config.external_storage.id'
        AFTER my_domain,
    ADD COLUMN IF NOT EXISTS target_storage_name VARCHAR(255) NULL COMMENT '라우팅 대상 DB 저장소 이름'
        AFTER target_storage_id;

CREATE INDEX IF NOT EXISTS idx_routing_registry_target_storage_id
    ON routing.routing_registry (target_storage_id);
