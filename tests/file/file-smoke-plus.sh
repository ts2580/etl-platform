#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$ROOT_DIR"

MODULE=":etl-file-engine"
LEVEL="smoke-plus"

# TODO: Add broader smoke test targets here.
TESTS=(
  # "com.example.SomeSmokeTest"
)

CMD=("./gradlew" "${MODULE}:test")
if (( ${#TESTS[@]} > 0 )); then
  for TEST_CLASS in "${TESTS[@]}"; do
    CMD+=("--tests" "$TEST_CLASS")
  done
fi

if [[ $# -gt 0 ]]; then
  CMD+=("$@")
fi

printf '==> Running file %s tests\n' "$LEVEL"
if (( ${#TESTS[@]} > 0 )); then
  printf '   %s\n' "${TESTS[@]}"
else
  printf '   (predefined tests not set yet; running module default test task)\n'
fi
printf '\n==> Command: '
printf '%q ' "${CMD[@]}"
printf '\n\n'

exec "${CMD[@]}"
