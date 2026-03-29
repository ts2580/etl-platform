# etl-routing-engine vendor smoke round 1

## Goal

Create a practical, always-runnable first-pass regression smoke set for vendor-sensitive routing behavior without depending on full external DB infrastructure.

## What this smoke suite covers

This round intentionally focuses on the deepest **currently runnable** paths that are most likely to break across Oracle / PostgreSQL / MySQL / MariaDB behavior:

1. **Vendor registration / JDBC parsing / schema expectations**
   - `DatabaseStorageSupportTest`
   - Verifies JDBC parsing, Oracle schema rules, certificate metadata handling, and vendor strategy behavior.

2. **Target table resolution for routing**
   - `SalesforceTargetTableResolverTest`
   - `SalesforceRecordMutationProcessorTableResolutionTest`
   - Verifies Oracle physical table naming vs non-Oracle default table selection.

3. **Salesforce schema -> typed parameter binding**
   - `SalesforceObjectSchemaBuilderTypeMappingRegressionTest`
   - Verifies boolean/date/time/datetime/currency/textarea conversions and Oracle-specific typed bindings.

4. **CDC mutation -> bound SQL behavior**
   - `SalesforceRecordMutationProcessorBoundTypeMappingTest`
   - Verifies JSON fallback, explicit null typing, Oracle create fallback typing, and preserved freshness fields.

5. **External storage JDBC executor vendor behavior**
   - `ExternalStorageRoutingJdbcExecutorBindingTest`
   - `ExternalStorageRoutingJdbcExecutorDdlSplitTest`
   - Verifies Oracle-specific JDBC binding differences, generic non-Oracle binding, and Oracle/non-Oracle DDL splitting rules.

## Why this is the round-1 smoke set

There is runtime support for Oracle/PostgreSQL/MySQL/MariaDB drivers, but no established multi-vendor containerized integration harness in this repo yet.

A real vendor E2E that actually creates tables and writes rows into each external database would be the ideal next step, but would require one of:

- testcontainers-based vendor infrastructure already wired in, or
- stable access to provisioned databases in CI/dev.

Until then, this smoke set gives repeatable regression confidence on the highest-risk vendor-dependent logic already exercised inside the routing engine code path.

## Command

From repo root:

```bash
./gradlew :etl-routing-engine:vendorSmokeTest
```

## Optional environment-backed integration check

`StudyAccountBulkApiIntegrationTest` remains useful for a real environment-backed path:

- Salesforce OAuth client credentials
- Bulk query job execution
- CSV parsing
- DB temp-table insert/cleanup

But it depends on local DB + Salesforce credential state, so it is **not** part of the always-runnable smoke baseline.

## Recommended next step

Add `:etl-routing-engine:vendorContainerSmokeTest` with Testcontainers profiles for:

- PostgreSQL
- MariaDB
- MySQL
- Oracle XE/Free (if licensing/runtime choice is acceptable)

and drive one shared scenario end-to-end:

- build DDL from Salesforce object metadata
- create target table in vendor DB
- execute initial-load insert batch
- execute CDC upsert/update/delete path
- assert final row state and freshness guards
