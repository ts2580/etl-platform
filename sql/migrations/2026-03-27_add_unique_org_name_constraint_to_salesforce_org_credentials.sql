-- salesforce_org_credentials.org_name 중복 등록 방지
-- 적용: 메인 모듈 org 등록 시 org_name UNIQUE 제약 추가
-- NOTE: 적용 전 중복 org_name 데이터가 있으면 실패할 수 있습니다.

ALTER TABLE config.salesforce_org_credentials
    ADD UNIQUE INDEX IF NOT EXISTS uk_sf_org_credentials_name (org_name);
