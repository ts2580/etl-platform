-- CERTIFICATE 모드 지원: 사용자/비밀번호 컬럼을 CERTIFICATE 방식에서 nullable하게 허용

ALTER TABLE config.external_database_storage
    MODIFY COLUMN username VARCHAR(255) NULL,
    MODIFY COLUMN password_encrypted VARCHAR(2000) NULL;
