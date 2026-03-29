#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$ROOT_DIR"

MODULE=":etl-main-app"
LEVEL="smoke-plus"

# Broader smoke set: 핵심 보안/스토리지 API 흐름까지 포함
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
