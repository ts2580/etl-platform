# etl-platform

Salesforce ETL 플랫폼을 **단일 Repo + Gradle 멀티모듈**로 관리하기 위한 통합 프로젝트.

## Tech Stack

- **Language**: Java 21 (Toolchain)
- **Build**: Gradle Wrapper (Gradle 8.13), Groovy DSL
- **Framework**: Spring Boot **3.5.6**
- **DB/Integration**
  - MyBatis Spring Boot Starter 3.0.3
  - (모듈별) Spring Data JPA
  - MariaDB / PostgreSQL Driver
  - (모듈별) Spring Data Redis
- **Routing**: Apache Camel 4.6.0 (Salesforce 연동 포함)
- **HTTP Clients**: OkHttp, Apache HttpClient
- **Auth/Security**: Spring Security, Thymeleaf Extras (Spring Security 6)
- **Etc**: Lombok, Gson, json-simple

## Modules

### Libraries (jar)
- `etl-common` : 공통 유틸/헬퍼
- `etl-domain` : 도메인(entity/도메인 로직)
- `etl-api-contract` : DTO/contract
- `etl-infra` : 공통 인프라 client(salesforce/db/storage)

### Apps (deployable bootJar)
- `etl-main-app` : 관리 모듈 
- `etl-routing-engine` : Apache Camel 기반 단방향 라우팅 모듈
- `etl-file-engine` : 파일 업로드/동기화 모듈

## Build Outputs

빌드 후 각 모듈의 `build/libs/` 아래 jar 생성 

- `etl-main-app/build/libs/etl-main-app.jar`
- `etl-routing-engine/build/libs/etl-routing-engine.jar`
- `etl-file-engine/build/libs/etl-file-engine.jar`

## Docker (single image for dev test)

개발 테스트용으로 3개 실행 앱(`etl-main-app`, `etl-routing-engine`, `etl-file-engine`)을 **단일 Docker 이미지**에서 함께 띄울 수 있어요.

### Build

```bash
docker build -t etl-platform:dev .
```

### Run

DB/Redis 없이 화면/기본 엔드포인트만 확인하려면:

```bash
docker run --rm -p 8080:8080 -p 3931:3931 -p 9443:9443 \
  -e APP_DB_ENABLED=false \
  etl-platform:dev
```

기존 `.env`를 써서 실행하려면:

```bash
docker compose -f docker-compose.dev.yml up --build
```

### Notes

- 이미지 하나 안에서 JVM 3개를 동시에 실행해요.
- 개발 테스트/통합 확인용으로는 편하지만, 운영 배포에는 보통 서비스별 이미지 분리가 더 좋아요.
- `APP_DB_ENABLED=true`면 MariaDB/Redis 같은 외부 의존성은 별도로 준비돼 있어야 해요.

## Provenance

이 Repo는 아래 3개 프로젝트를 통합/재구성한 것임

- `etl-main-app`  ← ETL-MAIN-MODULE
  - https://github.com/ts2580/ETL-MAIN-MODULE
- `etl-routing-engine` ← ETL-WORK-MODULE
  - https://github.com/ts2580/ETL-WORK-MODULE
- `etl-file-engine` ← sfdcFileUpload
  - https://github.com/ts2580/sfdcFileUpload