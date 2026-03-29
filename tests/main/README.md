# Main Module Test Scripts

`etl-main-app` 모듈 테스트 실행용 템플릿을 **실제 테스트 목록 기반**으로 채웠어요.

현재 포함 클래스는 다음입니다.

- `com.etl.sfdc.storage.service.DatabaseStorageQueryServiceTest`
- `com.etl.sfdc.navigation.controller.NavigationFlowWebMvcTest`
- `com.etl.sfdc.storage.controller.DatabaseStorageSecurityWebMvcTest`

## Scripts

- `main-ultra-smoke.sh` : 빠른 최소 회귀(테스트 2개)
  - `DatabaseStorageQueryServiceTest`
  - `NavigationFlowWebMvcTest`
- `main-smoke-plus.sh` : 실전형 smoke(위 2개 + 보안 스토리지 API)
  - `DatabaseStorageQueryServiceTest`
  - `NavigationFlowWebMvcTest`
  - `DatabaseStorageSecurityWebMvcTest`
- `main-integration.sh` : 통합 분류를 위한 스크립트(현재는 메인에 통합 전용 클래스가 없어 동일 set으로 운영)
  - `DatabaseStorageQueryServiceTest`
  - `NavigationFlowWebMvcTest`
  - `DatabaseStorageSecurityWebMvcTest`
- `main-one.sh` : 특정 테스트/패턴 개별 실행

## Usage

```bash
./tests/main/main-ultra-smoke.sh
./tests/main/main-smoke-plus.sh
./tests/main/main-integration.sh

# 특정 패턴 실행
./tests/main/main-one.sh NavigationFlowWebMvcTest
./tests/main/main-one.sh '*Storage*'
```

### 실행 예시(옵션 추가)

```bash
./tests/main/main-smoke-plus.sh --info
./tests/main/main-integration.sh --stacktrace
./tests/main/main-one.sh DatabaseStorageQueryServiceTest --rerun-tasks
```
