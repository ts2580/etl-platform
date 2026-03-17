-- Client Credentials 전환: refresh_token 컬럼 제거
-- 주의: 적용 전 백업 권장

ALTER TABLE config.salesforce_org_credentials
    MODIFY COLUMN client_id VARCHAR(255) NULL,
    MODIFY COLUMN client_secret VARCHAR(255) NULL,
    MODIFY COLUMN access_token TEXT NULL,
    MODIFY COLUMN access_token_issued_at DATETIME NULL;

ALTER TABLE config.salesforce_org_credentials
    DROP COLUMN refresh_token;
