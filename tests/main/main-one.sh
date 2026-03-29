#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  cat <<'EOF'
Usage:
  ./tests/main/main-one.sh <TestClassOrPattern> [extra gradle args...]

Examples:
  ./tests/main/main-one.sh NavigationFlowWebMvcTest
  ./tests/main/main-one.sh com.etl.sfdc.navigation.controller.NavigationFlowWebMvcTest
  ./tests/main/main-one.sh '*Database*'
EOF
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$ROOT_DIR"

MODULE=":etl-main-app"
TEST_PATTERN="$1"
shift || true

CMD=("./gradlew" "${MODULE}:test" "--tests" "$TEST_PATTERN")
if [[ $# -gt 0 ]]; then
  CMD+=("$@");
fi

printf '==> Running main test pattern: %s\n' "$TEST_PATTERN"
printf '==> Command: '
printf '%q ' "${CMD[@]}"
printf '\n\n'

exec "${CMD[@]}"
