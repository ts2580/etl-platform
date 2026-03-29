#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  cat <<'EOF'
Usage:
  ./tests/routing/routing-one.sh <TestClassOrPattern> [extra gradle args...]

Examples:
  ./tests/routing/routing-one.sh StudyAccountBulkApiIntegrationTest
  ./tests/routing/routing-one.sh com.apache.sfdc.common.SalesforceBulkCsvCursorTest
  ./tests/routing/routing-one.sh '*TableResolution*'
  ./tests/routing/routing-one.sh StudyAccountBulkApiIntegrationTest --info
EOF
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$ROOT_DIR"

MODULE=":etl-routing-engine"
TEST_PATTERN="$1"
shift || true

CMD=("./gradlew" "${MODULE}:test" "--tests" "$TEST_PATTERN")
if [[ $# -gt 0 ]]; then
  CMD+=("$@")
fi

printf '==> Running routing test pattern: %s\n' "$TEST_PATTERN"
printf '==> Command: '
printf '%q ' "${CMD[@]}"
printf '\n\n'

exec "${CMD[@]}"
