# File Module Test Scripts

`etl-file-engine` 모듈 테스트 실행을 위한 템플릿 모음입니다.

현재는 **템플릿 상태**라서 실제 테스트 클래스는 TODO로 남겨두었어요.
필요한 클래스명만 채우면 바로 사용 가능합니다.

## Scripts

- `file-ultra-smoke.sh` : 가장 빠른 최소 회귀 집합 템플릿
- `file-smoke-plus.sh` : 범위를 조금 넓힌 smoke 템플릿
- `file-integration.sh` : 통합 성격 테스트 템플릿
- `file-one.sh` : 특정 테스트/패턴 개별 실행

## Usage

```bash
./tests/file/file-ultra-smoke.sh
./tests/file/file-smoke-plus.sh
./tests/file/file-integration.sh

# 특정 패턴 실행
./tests/file/file-one.sh MyTest
./tests/file/file-one.sh '*Table*'
```

### 실행 예시(옵션 추가)

```bash
./tests/file/file-ultra-smoke.sh --info
./tests/file/file-smoke-plus.sh --stacktrace
./tests/file/file-one.sh SomeTest --info --rerun-tasks
```

## Tip

각 스크립트의 `TESTS=( ... )` 부분에 테스트 클래스명을 채운 뒤,
원하면 템플릿 주석을 지워 깔끔하게 쓰시면 됩니다.
