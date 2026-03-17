# Module별 로컬 환경변수 가이드

현재 `dev-etl.sh`는 각 모듈별 env 파일을 **옵션**으로 읽고,
공통 `.env`를 **기본값**으로 먼저 주입해요.

## 파일 우선순위

- 1순위: 모듈별 env (`env/etl-*.env`)
- 2순위: 공통 env (`/home/trstyq/code/etl-platform/.env`)
- 3순위: 현재 쉘 환경변수

즉, 한 파일로 끝내고 싶으면 `.env`만 채우면 돼요.

## 권장: 한 파일로 통합 관리 (공통 .env)

```bash
cd ~/code/etl-platform
cp /dev/null .env  # 새로 만들기
# 아래 예시 키만 넣어서 사용
cat > .env <<'EOF'
APP_DB_ENABLED=true

DB_URL=jdbc:mariadb://localhost:3306/etl
DB_USERNAME=etl_user
DB_PASSWORD=CHANGE_ME

SALESFORCE_CLIENT_ID=
SALESFORCE_CLIENT_SECRET=
SALESFORCE_TOKEN_URL=/services/oauth2/token
SALESFORCE_ALLOWED_ORIGINS=http://localhost:8080

# 모듈별 포트(필요시 변경)
ROUTING_APP_PORT=3931
ROUTING_ENGINE_BASE_URL=http://localhost:3931
FILE_APP_PORT=9443

# routing-engine
REDIS_URL=127.0.0.1
REDIS_PORT=6379
REDIS_PASSWORD=

SALESFORCE_LOGIN_URL=https://login.salesforce.com
SALESFORCE_API_VERSION=60.0

JAVA_TOOL_OPTIONS="-Xms256m -Xmx512m"
EOF

./dev-etl.sh rebuild
```

## 참고
- `env/*.env`, `env/*.env.*`, `.env`는 `.gitignore`에서 커밋 제외 처리됨
- 예외 템플릿 파일(`.env.example`)은 원하면 올려 공유
