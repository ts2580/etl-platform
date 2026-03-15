-- 회원가입/로그인 최소 동작용 DDL
-- 대상: etl-main-app
-- 용도: /user/signup, /user/login 에 필요한 config.member 생성

CREATE DATABASE IF NOT EXISTS config
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS config.member (
  id INT NOT NULL AUTO_INCREMENT,
  name VARCHAR(100) NOT NULL,
  username VARCHAR(100) NOT NULL,
  password VARCHAR(255) NOT NULL,
  email VARCHAR(255) NOT NULL,
  description VARCHAR(500) NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uk_member_username (username),
  UNIQUE KEY uk_member_email (email)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci;

-- 선택: 설정 조회 API(/config/user)가 필요하면 아래 테이블도 생성
-- 현재 코드는 config.user 를 조회만 하고 insert/update 는 보이지 않음
CREATE TABLE IF NOT EXISTS config.user (
  id INT NOT NULL AUTO_INCREMENT,
  name VARCHAR(100) NOT NULL,
  username VARCHAR(100) NOT NULL,
  password VARCHAR(255) NOT NULL,
  email VARCHAR(255) NOT NULL,
  description VARCHAR(500) NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uk_user_username (username),
  UNIQUE KEY uk_user_email (email)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci;

-- 확인용 샘플 데이터가 필요하면 예시
-- 비밀번호는 Spring Security BCrypt 해시를 넣어야 정상 로그인 가능
-- 아래 값은 예시 placeholder 이므로 실제 해시로 교체해서 사용
-- INSERT INTO config.member (name, username, password, email, description)
-- VALUES ('admin', 'admin', '$2a$10$replace_with_bcrypt_hash', 'admin@example.com', '관리자');
