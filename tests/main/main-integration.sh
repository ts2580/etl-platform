#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$ROOT_DIR"

MODULE=":etl-main-app"
LEVEL="integration"

# Current main-module state: 통합 전용 전용 클래스가 없어 기본 전체 테스트(task-level)로 운영
TESTS=(
  "com.etl.sfdc.storage.service.DatabaseStorageQueryServiceTest"
  "com.etl.sfdc.navigation.controller.NavigationFlowWebMvcTest"
  "com.etl.sfdc.storage.controller.DatabaseStorageSecurityWebMvcTest"
)

CMD=("./gradlew" "${MODULE}:test")
for TEST_CLASS in "${TESTS[@]}"; do
  CMD+=("--tests" "$TEST_CLASS")
done

if [[ $# -gt 0 ]]; then
  CMD+=("$@")
fi

printf '==> Running main %s tests (%d classes)\n' "$LEVEL" "${#TESTS[@]}"
printf '   %s\n' "${TESTS[@]}"
printf '\n==> Command: '
printf '%q ' "${CMD[@]}"
printf '\n\n'

exec "${CMD[@]}"
