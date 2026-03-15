-- etl-platform base DDL
-- MariaDB/MySQL compatible bootstrap script
--
-- NOTE:
-- 1) MariaDB does not support CREATE OR REPLACE TABLE in the same way views do.
--    So for 'replace' semantics, this script uses DROP TABLE IF EXISTS + CREATE TABLE.
-- 2) Dynamic Salesforce object tables (for streaming / CDC) are created by application runtime
--    under the `config` schema, so they are not fully enumerated here.

CREATE DATABASE IF NOT EXISTS config CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE DATABASE IF NOT EXISTS mig CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

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
-- config.cdc_slot_registry
-- routing-engine CDC slot usage summary
-- =========================================================
DROP TABLE IF EXISTS config.cdc_slot_registry;
CREATE TABLE config.cdc_slot_registry (
    id BIGINT NOT NULL AUTO_INCREMENT,
    selected_object VARCHAR(255) NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_cdc_slot_registry_selected_object (selected_object),
    KEY idx_cdc_slot_registry_is_active (is_active)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- =========================================================
-- mig.account_file
-- file-engine migration source table
-- =========================================================
DROP TABLE IF EXISTS mig.account_file;
CREATE TABLE mig.account_file (
    id BIGINT NOT NULL AUTO_INCREMENT,
    account_number VARCHAR(100) NOT NULL,
    append_file VARCHAR(1024) NULL,
    appnd_file_path VARCHAR(2048) NULL,
    is_mig BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (id),
    KEY idx_account_file_account_number (account_number),
    KEY idx_account_file_is_mig (is_mig)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- =========================================================
-- mig.account_file_map
-- file-engine mapping table
-- =========================================================
DROP TABLE IF EXISTS mig.account_file_map;
CREATE TABLE mig.account_file_map (
    id BIGINT NOT NULL AUTO_INCREMENT,
    account_number VARCHAR(100) NOT NULL,
    sfid VARCHAR(64) NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_account_file_map_account_number (account_number),
    KEY idx_account_file_map_sfid (sfid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- =========================================================
-- optional seed
-- =========================================================
-- INSERT INTO config.member (name, username, password, email, description)
-- VALUES ('admin', 'admin', '$2a$10$replace-with-bcrypt-hash', 'admin@example.com', 'bootstrap admin');

-- =========================================================
-- runtime-generated tables
-- =========================================================
-- routing-engine / main-app dynamically create Salesforce object tables like:
--   config.Account
--   config.Contact
--   config.Lead
-- etc.
-- Those are generated from Salesforce metadata at runtime,
-- so they are intentionally excluded from this static bootstrap DDL.
