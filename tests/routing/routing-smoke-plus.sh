#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$ROOT_DIR"

MODULE=":etl-routing-engine"

TESTS=(
  "com.apache.sfdc.common.SalesforceBulkCsvCursorTest"
  "com.apache.sfdc.common.SalesforceBulkQueryClientImplTest"
  "com.apache.sfdc.common.SalesforceBulkResultMapperTest"
  "com.apache.sfdc.common.SalesforceHttpErrorHelperTest"
  "com.apache.sfdc.common.SalesforceInitialLoadServiceImplTest"
  "com.apache.sfdc.common.SalesforceObjectSchemaBuilderTypeMappingRegressionTest"
  "com.apache.sfdc.common.SalesforceRecordMutationProcessorBoundTypeMappingTest"
  "com.apache.sfdc.common.SalesforceRecordMutationProcessorTableResolutionTest"
  "com.apache.sfdc.common.SalesforceTargetTableResolverTest"
  "com.apache.sfdc.storage.service.ExternalStorageRoutingJdbcExecutorBindingTest"
  "com.apache.sfdc.storage.service.ExternalStorageRoutingJdbcExecutorDdlSplitTest"
)

CMD=("./gradlew" "${MODULE}:test")
for test_class in "${TESTS[@]}"; do
  CMD+=("--tests" "$test_class")
done

if [[ $# -gt 0 ]]; then
  CMD+=("$@")
fi

printf '==> Running routing smoke-plus tests (%d classes)\n' "${#TESTS[@]}"
printf '   %s\n' "${TESTS[@]}"
printf '\n==> Command: '
printf '%q ' "${CMD[@]}"
printf '\n\n'

exec "${CMD[@]}"
