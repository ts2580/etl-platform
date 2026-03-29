#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$ROOT_DIR"

if [[ -f "$ROOT_DIR/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "$ROOT_DIR/.env"
  set +a
fi

MODULE=":etl-routing-engine"

TESTS=(
  "com.apache.sfdc.common.StudyAccountBulkApiIntegrationTest"
  "com.apache.sfdc.SfdcApplicationTests"
  "com.apache.sfdc.storage.smoke.RoutingVendorContainerSmokeTest"
)

EXTRA_ARGS=("$@")
RESULTS=()
FAILED=0

printf '==> Running routing integration tests sequentially (%d classes)\n' "${#TESTS[@]}"
printf '   %s\n' "${TESTS[@]}"
printf '\n'

for test_class in "${TESTS[@]}"; do
  CMD=("./gradlew" "${MODULE}:test" "--tests" "$test_class")
  if [[ ${#EXTRA_ARGS[@]} -gt 0 ]]; then
    CMD+=("${EXTRA_ARGS[@]}")
  fi

  printf '==> Running: %s\n' "$test_class"
  printf '==> Command: '
  printf '%q ' "${CMD[@]}"
  printf '\n\n'

  if "${CMD[@]}"; then
    RESULTS+=("PASS|$test_class")
  else
    RESULTS+=("FAIL|$test_class")
    FAILED=1
  fi

  printf '\n'
done

printf '==> Summary\n'
for result in "${RESULTS[@]}"; do
  IFS='|' read -r status test_class <<< "$result"
  printf ' - %s %s\n' "$status" "$test_class"
done

exit $FAILED
