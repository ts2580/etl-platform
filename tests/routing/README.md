# Routing Test Scripts

`etl-routing-engine` 모듈 테스트를 빠르게 반복 실행하기 위한 스크립트 모음입니다.

이 디렉터리의 목적은 아래 두 가지입니다.

1. 자주 사용하는 테스트 실행 명령을 표준화한다.
2. 테스트 실행 수준을 `ultra-smoke -> smoke-plus -> integration`으로 구분해 개발 속도와 신뢰도를 균형 있게 가져간다.

---

## Scripts

### 1) `routing-ultra-smoke.sh`
가장 빠른 최소 검증 세트입니다.

**목적**
- 저장 직후 빠르게 회귀 확인
- 리팩토링 직후 핵심 로직 생존 여부 확인
- 자주 반복 실행하는 로컬 기본 스크립트

**포함 대상 성격**
- 환경 의존성이 거의 없는 테스트
- 순수 로직/파싱/해석/DDL 분해 등 핵심 회귀 포인트
- 실패하면 라우팅 핵심 로직이 깨졌을 가능성이 높은 테스트

**현재 포함 테스트**
- `com.apache.sfdc.common.SalesforceBulkCsvCursorTest`
- `com.apache.sfdc.common.SalesforceBulkResultMapperTest`
- `com.apache.sfdc.common.SalesforceObjectSchemaBuilderTypeMappingRegressionTest`
- `com.apache.sfdc.common.SalesforceRecordMutationProcessorTableResolutionTest`
- `com.apache.sfdc.storage.service.ExternalStorageRoutingJdbcExecutorDdlSplitTest`

**권장 사용 시점**
- 메서드/클래스 수정 직후
- IDE 없이 터미널에서 빠르게 확인할 때
- 커밋 전에 최소한의 생존 확인을 하고 싶을 때

**실행 예시**
```bash
./tests/routing/routing-ultra-smoke.sh
./tests/routing/routing-ultra-smoke.sh --info
./tests/routing/routing-ultra-smoke.sh --stacktrace
```

---

### 2) `routing-smoke-plus.sh`
`ultra-smoke`보다 범위를 넓힌 실전형 smoke 세트입니다.

**목적**
- 핵심 경로가 조금 더 넓게 살아 있는지 확인
- PR 전 / 병합 전 / 기능 마무리 시점 검증
- 너무 무겁지 않으면서도 신뢰도를 높이는 중간 단계

**포함 대상 성격**
- `ultra-smoke`에 포함된 핵심 테스트
- 클라이언트/초기 적재/바인딩 등 라우팅 주요 구성요소 검증
- 완전한 integration까지는 아니지만 실제 장애 감지력이 높은 테스트

**현재 포함 테스트**
- `com.apache.sfdc.common.SalesforceBulkCsvCursorTest`
- `com.apache.sfdc.common.SalesforceBulkQueryClientImplTest`
- `com.apache.sfdc.common.SalesforceBulkResultMapperTest`
- `com.apache.sfdc.common.SalesforceHttpErrorHelperTest`
- `com.apache.sfdc.common.SalesforceInitialLoadServiceImplTest`
- `com.apache.sfdc.common.SalesforceObjectSchemaBuilderTypeMappingRegressionTest`
- `com.apache.sfdc.common.SalesforceRecordMutationProcessorBoundTypeMappingTest`
- `com.apache.sfdc.common.SalesforceRecordMutationProcessorTableResolutionTest`
- `com.apache.sfdc.common.SalesforceTargetTableResolverTest`
- `com.apache.sfdc.storage.service.ExternalStorageRoutingJdbcExecutorBindingTest`
- `com.apache.sfdc.storage.service.ExternalStorageRoutingJdbcExecutorDdlSplitTest`

**권장 사용 시점**
- 기능 단위 수정이 끝났을 때
- PR 올리기 전
- 다른 사람에게 리뷰 요청하기 전
- 라우팅 관련 수정이 여러 파일에 걸쳤을 때

**실행 예시**
```bash
./tests/routing/routing-smoke-plus.sh
./tests/routing/routing-smoke-plus.sh --info
./tests/routing/routing-smoke-plus.sh --stacktrace
./tests/routing/routing-smoke-plus.sh --rerun-tasks
```

---

### 3) `routing-integration.sh`
무겁거나 환경 영향이 큰 통합 성격 테스트를 모아둔 스크립트입니다.

**목적**
- 스프링 컨텍스트/컨테이너/DB/실제 연동 감각이 있는 테스트 실행
- smoke 단계보다 더 현실적인 검증 수행
- 로컬 환경 또는 준비된 테스트 환경에서 최종 확인

**현재 포함 테스트**
- `com.apache.sfdc.common.StudyAccountBulkApiIntegrationTest`
- `com.apache.sfdc.SfdcApplicationTests`
- `com.apache.sfdc.storage.smoke.RoutingVendorContainerSmokeTest`

**권장 사용 시점**
- smoke-plus 통과 후 마지막 확인
- 환경 설정이 맞춰진 상태에서 실제 통합 흐름 검증
- 배포 전 또는 주요 장애 대응 후 재검증

**주의**
- 환경 설정, DB 연결, 컨테이너 상태 등에 따라 실패할 수 있습니다.
- 실패했다고 해서 즉시 코드 결함이라고 단정하지 말고, 환경 의존성 여부를 함께 확인하세요.

**실행 예시**
```bash
./tests/routing/routing-integration.sh
./tests/routing/routing-integration.sh --info
./tests/routing/routing-integration.sh --stacktrace
```

---

### 4) `routing-one.sh`
테스트 클래스 1개 또는 패턴 1개만 골라 실행하는 스크립트입니다.

**목적**
- 특정 테스트 하나만 집중적으로 디버깅
- 실패 재현 시간을 줄이기
- smoke 세트 바깥 테스트를 선택 실행

**사용법**
```bash
./tests/routing/routing-one.sh <TestClassOrPattern> [extra gradle args...]
```

**실행 예시**
```bash
./tests/routing/routing-one.sh StudyAccountBulkApiIntegrationTest
./tests/routing/routing-one.sh com.apache.sfdc.common.SalesforceBulkCsvCursorTest
./tests/routing/routing-one.sh '*TableResolution*'
./tests/routing/routing-one.sh SalesforceInitialLoadServiceImplTest --info
./tests/routing/routing-one.sh com.apache.sfdc.SfdcApplicationTests --stacktrace
```

---

## Recommended Execution Rules

아래 규칙을 기본 운영 가이드로 권장합니다.

### 규칙 1. 작은 수정 직후에는 `ultra-smoke`
다음과 같은 경우에는 우선 `routing-ultra-smoke.sh`를 실행합니다.

- 파싱 로직 수정
- 타입 매핑 수정
- 테이블명 해석 수정
- DDL 문자열 생성/분해 로직 수정
- 단일 클래스/단일 메서드 중심 리팩토링

**권장 이유**
- 가장 빠르게 핵심 회귀를 감지할 수 있습니다.
- 자주 돌려도 부담이 적습니다.

---

### 규칙 2. 라우팅 관련 변경이 넓어지면 `smoke-plus`
다음과 같은 경우에는 `routing-smoke-plus.sh`까지 올려서 확인합니다.

- 여러 클래스에 걸친 수정
- 초기 적재 흐름 영향 가능성 있음
- HTTP 클라이언트/예외 처리/바인딩 로직 수정
- PR 생성 직전
- 코드 리뷰 요청 직전

**권장 이유**
- ultra-smoke보다 실제 장애 검출력이 높습니다.
- integration보다 가볍고 반복 실행이 쉽습니다.

---

### 규칙 3. 환경 영향이 있는 검증은 `integration`
다음과 같은 경우에는 `routing-integration.sh`를 사용합니다.

- smoke-plus는 통과했지만 실제 흐름 확인이 필요할 때
- DB/컨테이너/스프링 컨텍스트 수준 이슈를 확인할 때
- 배포 전 최종 검증
- 장애 수정 후 실제 통합 경로 재확인

**권장 이유**
- smoke만으로 놓칠 수 있는 환경/연동 이슈를 잡을 수 있습니다.
- 다만 비용이 크므로 필요할 때만 실행하는 편이 효율적입니다.

---

### 규칙 4. 실패한 테스트는 `routing-one.sh`로 좁혀서 본다
세트 스크립트에서 실패가 나면 전체를 반복 실행하기보다, 먼저 실패한 테스트를 `routing-one.sh`로 좁혀서 디버깅합니다.

**예시**
```bash
./tests/routing/routing-one.sh SalesforceInitialLoadServiceImplTest --info
./tests/routing/routing-one.sh '*Bulk*' --stacktrace
```

**권장 이유**
- 재현 시간이 짧아집니다.
- 로그 해석이 쉬워집니다.
- 원인 파악 후 다시 smoke 세트로 복귀하기 좋습니다.

---

## Practical Workflow Examples

### 예시 1. 작은 리팩토링
상황:
- `SalesforceRecordMutationProcessor` 내부 로직만 수정함

권장 순서:
```bash
./tests/routing/routing-ultra-smoke.sh
./tests/routing/routing-one.sh '*TableResolution*' --info
```

---

### 예시 2. 라우팅 핵심 경로 수정
상황:
- 테이블 해석, 초기 적재, 에러 처리까지 손댐

권장 순서:
```bash
./tests/routing/routing-ultra-smoke.sh
./tests/routing/routing-smoke-plus.sh
```

필요 시:
```bash
./tests/routing/routing-one.sh SalesforceInitialLoadServiceImplTest --stacktrace
```

---

### 예시 3. 배포 전 점검
상황:
- 기능 수정 완료, 실제 통합 경로도 한번 보고 싶음

권장 순서:
```bash
./tests/routing/routing-ultra-smoke.sh
./tests/routing/routing-smoke-plus.sh
./tests/routing/routing-integration.sh
```

---

### 예시 4. integration 실패 후 좁혀보기
상황:
- `routing-integration.sh`에서 실패함

권장 순서:
```bash
./tests/routing/routing-one.sh StudyAccountBulkApiIntegrationTest --info
./tests/routing/routing-one.sh com.apache.sfdc.SfdcApplicationTests --stacktrace
```

환경 변수/DB 연결/테스트 컨테이너 상태를 함께 확인합니다.

---

## Design Principles For Future Maintenance

스크립트를 유지보수할 때는 아래 원칙을 권장합니다.

### `ultra-smoke`에 넣을 테스트 기준
- 빠르다
- 환경 의존성이 낮다
- 핵심 로직 회귀를 잘 잡는다
- 자주 돌려도 부담이 없다

### `smoke-plus`에 넣을 테스트 기준
- ultra보다는 넓게 검증한다
- 장애 감지 가치가 높다
- 너무 무겁지는 않다
- PR 전 검증 세트로 적합하다

### `integration`에 넣을 테스트 기준
- 스프링 컨텍스트/DB/컨테이너/실연동과 결합된다
- 느릴 수 있다
- 환경 설정에 민감하다
- 최종 검증 가치가 높다

---

## Notes

- 모든 스크립트는 프로젝트 루트에서 `./gradlew :etl-routing-engine:test ...` 형태로 실행됩니다.
- 각 스크립트는 프로젝트 어느 위치에서 호출하더라도 내부에서 루트로 이동한 뒤 실행하도록 작성했습니다.
- 추가 Gradle 옵션이 필요하면 스크립트 뒤에 그대로 붙이면 됩니다.
  - 예: `--info`, `--stacktrace`, `--rerun-tasks`

---

## Quick Reference

```bash
# 가장 빠른 최소 검증
./tests/routing/routing-ultra-smoke.sh

# PR 전 실전형 smoke
./tests/routing/routing-smoke-plus.sh

# 특정 테스트 1개/패턴만 실행
./tests/routing/routing-one.sh StudyAccountBulkApiIntegrationTest
./tests/routing/routing-one.sh '*Bulk*' --info

# 통합 성격 테스트 실행
./tests/routing/routing-integration.sh
```
