#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  cat <<'EOF'
Usage:
  ./tests/file/file-one.sh <TestClassOrPattern> [extra gradle args...]

Examples:
  ./tests/file/file-one.sh MyFastTest
  ./tests/file/file-one.sh com.example.MyTest
  ./tests/file/file-one.sh '*Controller*'
EOF
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$ROOT_DIR"

MODULE=":etl-file-engine"
TEST_PATTERN="$1"
shift || true

CMD=("./gradlew" "${MODULE}:test" "--tests" "$TEST_PATTERN")
if [[ $# -gt 0 ]]; then
  CMD+=("$@");
fi

printf '==> Running file test pattern: %s\n' "$TEST_PATTERN"
printf '==> Command: '
printf '%q ' "${CMD[@]}"
printf '\n\n'

exec "${CMD[@]}"
