-- 등록 전 연결 테스트 이력 저장을 위해 storage_id nullable 허용

ALTER TABLE config.external_storage_connection_history
    DROP FOREIGN KEY fk_external_storage_connection_history_storage;

ALTER TABLE config.external_storage_connection_history
    MODIFY COLUMN storage_id BIGINT NULL;

ALTER TABLE config.external_storage_connection_history
    ADD CONSTRAINT fk_external_storage_connection_history_storage
        FOREIGN KEY (storage_id) REFERENCES config.external_storage(id)
        ON DELETE CASCADE;
