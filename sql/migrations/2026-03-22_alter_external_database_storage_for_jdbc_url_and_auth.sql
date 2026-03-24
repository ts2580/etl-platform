-- 외부 DB 저장소 등록 1차 스펙 보강
-- 입력 단순화: jdbc_url + username + password
-- 향후 확장: auth_method / credential_meta_json

ALTER TABLE config.external_database_storage
    ADD COLUMN jdbc_url VARCHAR(2000) NULL COMMENT '초기 등록용 JDBC URL' AFTER vendor,
    ADD COLUMN auth_method VARCHAR(30) NOT NULL DEFAULT 'PASSWORD' COMMENT 'PASSWORD, CERTIFICATE' AFTER jdbc_url,
    ADD COLUMN credential_meta_json TEXT NULL COMMENT '인증서 기반 로그인 확장용 메타데이터 JSON' AFTER jdbc_options_json;

UPDATE config.external_database_storage
SET jdbc_url = CASE vendor
    WHEN 'MARIADB' THEN CONCAT('jdbc:mariadb://', host, ':', port, '/', COALESCE(database_name, ''))
    WHEN 'MYSQL' THEN CONCAT('jdbc:mysql://', host, ':', port, '/', COALESCE(database_name, ''))
    WHEN 'POSTGRESQL' THEN CONCAT('jdbc:postgresql://', host, ':', port, '/', COALESCE(database_name, ''))
    WHEN 'ORACLE' THEN CASE
        WHEN service_name IS NOT NULL AND service_name <> '' THEN CONCAT('jdbc:oracle:thin:@//', host, ':', port, '/', service_name)
        WHEN sid IS NOT NULL AND sid <> '' THEN CONCAT('jdbc:oracle:thin:@', host, ':', port, ':', sid)
        ELSE NULL
    END
    ELSE NULL
END
WHERE jdbc_url IS NULL;

ALTER TABLE config.external_database_storage
    MODIFY COLUMN jdbc_url VARCHAR(2000) NOT NULL;
