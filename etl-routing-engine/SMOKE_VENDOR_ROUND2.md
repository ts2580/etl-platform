# Vendor Smoke Round 2 - Testcontainers baseline and Oracle feasibility

## Verified CI split

### 1) Always-runnable baseline (no Docker)
- Gradle task: `:etl-routing-engine:vendorSmokeTest`
- Alias: `:etl-routing-engine:smokeTest`
- Purpose: lightweight smoke/regression coverage that should stay green in normal CI and local dev without Docker.

### 2) Docker-backed live vendor smoke
- Gradle task: `:etl-routing-engine:vendorContainerSmokeTest`
- Alias: `:etl-routing-engine:dockerSmokeTest`
- Purpose: real JDBC/Testcontainers coverage against the currently verified live vendors:
  - PostgreSQL
  - MariaDB
  - MySQL

### Suggested CI baseline
- Required on every PR / normal CI:
  - `./gradlew :etl-routing-engine:smokeTest`
- Required on Docker-capable jobs / scheduled integration lane:
  - `./gradlew :etl-routing-engine:dockerSmokeTest`
- Optional ad-hoc Oracle feasibility rerun while the blocker below remains open:
  - `./gradlew :etl-routing-engine:test --tests com.apache.sfdc.storage.smoke.RoutingVendorContainerSmokeTest --rerun-tasks`
  - then temporarily include `VendorContainerDescriptor.oracleCandidate()` in the parameter source or invoke a focused repro.

## Oracle feasibility result

### What was proven
Oracle **live container startup is feasible now** with Testcontainers using:
- module: `org.testcontainers:oracle-free:1.20.6`
- image: `gvenzl/oracle-free:23-slim-faststart`
- JDBC URL observed during real run: `jdbc:oracle:thin:@localhost:<port>/freepdb1`

The container booted successfully, Hikari connected successfully, and the smoke harness executed far enough to prove the plumbing is real.

### Current blocker
Oracle is **not yet ready to join the default live smoke matrix** because the shared mutation path still emits SQL against logical custom target table names instead of Oracle physical names.

Observed real failure:
- failing path: custom target table scenario inside `RoutingVendorContainerSmokeTest`
- Oracle error: `ORA-00942: table or view "ROUTING"."Account_Custom_Smoke" does not exist`
- reason: Oracle tables in this project use `OracleRoutingNaming.buildTableName(...)`, but this update path still targeted the logical table name rather than the Oracle physical name.

So this is **not** a Testcontainers limitation and **not** a fake/unverified guess. It is a real application-level Oracle naming mismatch exposed by the new live smoke harness.

## Durable extension points added
- `VendorContainerDescriptor` now supports runtime schema resolution via `targetSchemaResolver`
  - needed because Oracle schema/user resolution is runtime-derived
- `VendorContainerDescriptor.oracleCandidate()` keeps the Oracle container definition available without forcing it into green CI
- `RoutingVendorContainerTestSupport` now resolves the effective runtime schema and uses logical→physical table qualification for vendor-specific naming
  - this fixed one Oracle mismatch already (base table lookup)

## Next recommended step
Fix the Oracle custom target table mutation/update path so that any logical target table name is normalized through Oracle physical naming before SQL generation. After that:
1. re-enable Oracle in `VendorContainerDescriptor.supported()`
2. rerun `:etl-routing-engine:dockerSmokeTest`
3. only then promote Oracle into required CI
