# E2E Test Plan: Kafka Connector v4 (SSv2)

## Table of Contents

- [1. Overview](#1-overview)
- [2. Test Dimensions](#2-test-dimensions)
- [3. Test Categories](#3-test-categories)
  - [3.1 Data Ingestion](#31-data-ingestion)
  - [3.2 Error Handling](#32-error-handling)
  - [3.3 Schema Evolution](#33-schema-evolution)
  - [3.4 RECORD_CONTENT Mode](#34-record_content-mode)
  - [3.5 Connector Lifecycle & Resilience](#35-connector-lifecycle--resilience)
  - [3.6 Default Pipe Features](#36-default-pipe-features)
  - [3.7 Load & Stress](#37-load--stress)
- [4. Data Type Compatibility Strategy](#4-data-type-compatibility-strategy)
- [5. Coverage Matrix](#5-coverage-matrix)
- [6. Priority & Sequencing](#6-priority--sequencing)

### Status Legend

| Icon | Meaning |
|------|---------|
| 🟢 | Done -- test exists and passes |
| 🟡 | Needs rework -- test exists but needs dual conversion, un-skip, or xfail |
| 🔴 | Missing -- test must be written |
| ⚫ | Blocked -- should be dual but v3 blocked by SR classloader conflict (**parity gap**) |
| ⚪ | N/A -- not applicable or out of scope |

---

## 1. Overview

### Background

Kafka Connector v4 replaces v3's dual ingestion engines (file-based Snowpipe + SSv1 Streaming) with SSv2 exclusively. The GA strategy requires **functional parity** with v3 in compatibility mode, plus a new high-throughput mode.

### Connector Operating Modes

| Mode | Config | Validation | Schema Evolution | Error Handling | Target Use Case |
|------|--------|-----------|-----------------|----------------|-----------------|
| **Compatibility** | `snowflake.validation=client_side` (default) | Client-side | Client-side `ALTER TABLE` | Sync DLQ / Sync Abort | v3 migration, parity |
| **High-Throughput** | `snowflake.validation=server_side` | None (server) | Server-side (table `ENABLE_SCHEMA_EVOLUTION`) | Async Error Tables | Max throughput (10 GB/s target) |

### PRD Functional Requirements

| FR | Name | Scope |
|----|------|-------|
| FR1 | Client-Side DLQ (`errors.tolerance=all`) -- includes data type validation parity | Compatibility mode |
| FR2 | Client-Side Abort (`errors.tolerance=none`) -- includes data type validation parity | Compatibility mode |
| FR3 | Validation Toggle (`snowflake.validation`) | Mode switch |
| FR4 | Legacy Schema Toggle (`snowflake.enable.schematization`) | Both modes |
| FR5 | Default Pipes Only (`MATCH_BY_COLUMN_NAME`) | Both modes |
| FR6 | Schema Evolution | Both modes (different paths) |
| FR7 | Default Pipe Improvements (Identity, Defaults, Clustering) | Both modes |
| FR8 | Performance & Stability Baselines | High-throughput mode |
| FR9 | v3/v4 DLQ Parity | Compatibility mode -- requires DLQ error messages are byte-for-byte identical between v3 and v4 |
| FR10 | Telemetry & Usage Tracking | Both modes |
| FR11 | Pre-Flight Safety Check | High-throughput mode |

### When Dual Testing is Required

Running every test in dual mode (v3 + v4) doubles CI time. Dual is justified only when **v3 and v4 can produce different results** for the same input. There are two root causes of behavioral divergence:

1. **Data type handling differences between SDKs**: SSv1 and SSv2 may serialize, validate, or store values differently for the same Snowflake type. Known example: SSv1 parses JSON-like strings into native JSON objects in VARIANT columns, while SSv2 stores them as string literals. Similar differences may exist for BINARY encoding, TIMESTAMP precision, or other types. See [Section 4: Data Type Compatibility Strategy](#4-data-type-compatibility-strategy) for the full analysis.

2. **Client-side validation lifecycle**: SSv1 always validates (built into the SDK, cannot be disabled). V4 has a separate `RowValidator` (copied from SSv1's `DataValidationUtil`) that can be toggled. This affects DLQ routing, abort behavior, and schema evolution triggering. Additionally, v3 requires `schematization=true` for schema evolution; v4 does not.

**Dual is required** when a test:
- Asserts on data values stored in Snowflake columns (data type handling may differ between SDKs)
- Exercises client-side validation paths (DLQ, abort, schema evolution structural error detection)

**V4-only is safe** when a test:
- Only checks row counts, no-duplicate invariants, or offset correctness (no data value assertions)
- Exercises Kafka Connect framework behavior (SMTs, tombstone filtering, multi-partition routing)
- Tests features that don't exist in v3 (high-throughput mode, server-side SE)
- Tests connector lifecycle (pause/resume/restart) which is KC framework behavior

**Divergence detection approach**: The solution is generic — all tests use the same pattern. Each test runs v3, v4-compat, and v4-ht as parameterized modes. Assertions encode v3 reference behavior. When v4 diverges, the test handles it inline and logs a `DIVERGENCE` warning with a standardized prefix (`grep DIVERGENCE <test-output>` finds all of them). Each discovered divergence is then triaged as either "fix in production code" or "document as known behavioral gap". See `test_type_compatibility.py` for the implemented pattern.

**Testing gap -- Schema Registry classloader conflict**: Several tests are wired as dual but v3 skips at runtime because the v3 `SnowflakeSinkConnector` JAR conflicts with Confluent Schema Registry classloading. This is a **known testing infrastructure limitation**, not a justification for skipping parity verification. Each affected test is called out with ⚫. Resolution requires a **production code fix** to classloader isolation (being addressed by @sfc-gh-alhuang), not a test-only change. Alternative workarounds: (a) run v3 SR tests in a separate KC worker, or (b) accept the gap with documented risk.

---

## 2. Test Dimensions

Every test scenario can be classified across these independent dimensions:

### Data Format

Only formats used in the **Snowpipe Streaming** ingestion path are in scope. Legacy Snowpipe-only converters (`SnowflakeJsonConverter`, `SnowflakeAvroConverter`) are excluded -- we are migrating `SNOWPIPE_STREAMING` mode, not file-based `SNOWPIPE`.

| Format | Key Converter | Value Converter | Schema Registry | Platform |
|--------|--------------|-----------------|-----------------|----------|
| **JSON (native)** | StringConverter | JsonConverter | No | Any |
| **Avro SR** | StringConverter | AvroConverter (Confluent) | Yes | Confluent |
| **Avro SR (keys+values)** | AvroConverter | AvroConverter | Yes | Confluent |
| **Protobuf SR** | StringConverter | ProtobufConverter (Confluent) | Yes | Confluent |
| **Protobuf (native)** | StringConverter | Custom (raw bytes) | No | Any |
| **String (raw)** | StringConverter | StringConverter | No | Any |
| **Bytes (raw)** | ByteArrayConverter | ByteArrayConverter | No | Any |

### Connector Version

| Value | Meaning | When to use |
|-------|---------|-------------|
| `dual` | Both v4 and v3 via `connector_version` fixture | Tests that assert on data values or exercise client-side validation |
| `dual (v3 blocked)` | Should be dual but v3 cannot run due to SR classloader conflict. **This is a parity gap.** | SR-based tests -- must be resolved or explicitly accepted as risk |
| `v4` | v4 only | Feature has no v3 equivalent, or test only checks row counts / KC framework behavior |

### Architecture

| Value | Config | Behavior |
|-------|--------|----------|
| `client_side` (default) | `snowflake.validation=client_side` | Client-side validation, DLQ, abort |
| `server_side` | `snowflake.validation=server_side` | Server-side only, Error Tables |

### Schematization Mode

| Value | Config | Table Layout |
|-------|--------|-------------|
| `on` (default in v4) | `snowflake.enable.schematization=true` | Flat columns + `RECORD_METADATA` |
| `off` | `snowflake.enable.schematization=false` | `RECORD_CONTENT` + `RECORD_METADATA` (VARIANT) |

### Platform

| Platform | Schema Registry | Notes |
|----------|----------------|-------|
| Apache Kafka | Embedded (limited) | No Confluent SR converters |
| Confluent Platform | Full SR support | Required for Avro SR, Protobuf SR tests |

---

## 3. Test Categories

### 3.1 Data Ingestion

Basic data lands correctly in Snowflake for each format. This is the foundation -- every other category builds on it.

#### 3.1.1 JSON (Compatibility Mode)

| Status | Test | Version | Rationale | File |
|:------:|------|---------|-----------|------|
| 🟢 | String keys + JSON values | dual | Asserts data values in VARIANT header column (confirmed v3/v4 difference) | `test_string_json.py` |
| 🟢 | JSON keys + JSON values | v4 | Row count + metadata only; no data value assertions | `test_json_json.py` |
| 🟢 | JSON without schema + ReplaceField SMT | v4 | SMT runs in KC framework, not SDK-dependent | `test_native_string_json_without_schema.py` |
| 🟢 | Complex SMT chain (ValueToKey + ExtractField + ReplaceField) | v4 | Same -- KC framework SMT processing | `test_native_complex_smt.py` |
| 🟢 | Nullable values after ExtractField SMT | v4 | SMT + tombstone handling, KC framework level | `test_nullable_values_after_smt.py` |
| 🟢 | Snowpipe Streaming multi-partition (3p x 1000) | v4 | Row count + offset uniqueness check only | `test_snowpipe_streaming_string_json.py` |
| 🟢 | Multiple topics -> one table (3 topics x 3 partitions) | v4 | Row count + topic distribution check only | `test_multiple_topic_to_one_table_snowpipe_streaming.py` |
| 🟢 | Tombstone handling (`behavior.on.null.values=IGNORE`) | dual | Asserts data values in VARIANT header column | `test_snowpipe_streaming_string_json_ignore_tombstone.py` |
| 🔴 | Large blob ingestion (20 MiB JSON) | v4 | Row count check; tests SDK buffer limits, not validation. v3 equivalent: `TestLargeBlobSnowpipe` | -- |

#### 3.1.2 Avro (Compatibility Mode)

All Avro SR tests should be dual but v3 is **blocked** by the SR classloader conflict. **This is a parity gap** -- Avro data type handling through the full pipeline has not been verified against v3.

| Status | Test | Version | Rationale | File |
|:------:|------|---------|-----------|------|
| ⚫ | String keys + Avro SR values | dual (v3 blocked) | Parity gap: Avro data type handling not verified against v3 | `test_string_avrosr.py` |
| ⚫ | Avro SR keys + Avro SR values (NaN, Inf) | dual (v3 blocked) | Same gap | `test_avrosr_avrosr.py` |
| ⚫ | Snowpipe Streaming + Avro SR (3p x 1000) | dual (v3 blocked) | Same gap | `test_snowpipe_streaming_string_avro_sr.py` |

#### 3.1.3 Protobuf (Compatibility Mode)

| Status | Test | Version | Rationale | File |
|:------:|------|---------|-----------|------|
| ⚫ | Confluent Protobuf SR (nested types, special floats) | dual (v3 blocked) | Parity gap: Protobuf type handling not verified against v3 | `test_confluent_protobuf_protobuf.py` |
| 🟢 | Native Protobuf (raw bytes, no SR) | v4 | Protobuf deserialization is converter-level, not SDK | `test_native_string_protobuf.py` |

#### 3.1.4 Schema & Type Mapping (Compatibility Mode)

The existing `test_schema_mapping.py` is the beginning of type compatibility testing but has significant gaps. It will be **subsumed by the comprehensive `test_type_compatibility.py`** proposed in [Section 4](#4-data-type-compatibility-strategy). The new test file extends coverage to all Snowflake types, adds negative cases, and runs in dual mode.

| Status | Test | Version | Rationale | File |
|:------:|------|---------|-----------|------|
| 🟡 | Type mapping (JSON): 10 types, positive only | dual | Client-side validation differs between v3 (SSv1 built-in) and v4 (RowValidator copy) -- must verify parity. Currently v4-only with `validation.enabled=false`. Superseded by `test_type_compatibility.py` for comprehensive dual-mode coverage. | `test_schema_mapping.py` |
| 🟢 | Unsupported converter rejection | v4 | Converter rejection is KC framework level | `test_schema_not_supported_converter.py` |

#### 3.1.5 Table Creation

Auto table creation requires the connector to infer column types from the incoming data schema. Table creation itself is converter-independent — testing with a single converter (Avro SR, which provides an explicit schema) is sufficient.

| Status | Test | Version | Rationale | Format | File |
|:------:|------|---------|-----------|--------|------|
| ⚫ | Auto table creation from Avro SR schema | dual (v3 blocked) | Parity gap: v3 blocked by classloader | Avro SR | `test_auto_table_creation.py` |
| ⚫ | Auto table creation with topic2table mapping | dual (v3 blocked) | Same gap | Avro SR | `test_auto_table_creation_topic2table.py` |
| 🔴 | Auto table creation (high-throughput mode) | v4 | No v3 equivalent; verify server-side table creation works | Avro SR | -- |

#### 3.1.6 High-Throughput Mode Ingestion

v4-only: no v3 equivalent. `snowflake.validation=server_side`.

| Status | Test | Version | Format | Notes |
|:------:|------|---------|--------|-------|
| 🔴 | Valid JSON records land correctly (no client validation) | v4 | JSON | FR3 -- verify data arrives without client-side RowValidator |
| 🔴 | Valid Avro SR records land correctly | v4 | Avro SR | FR3 |
| 🔴 | Validation toggle default is `true` (omit toggle, verify compat behavior) | v4 | JSON | FR3 |
| 🔴 | Toggle interaction with DLQ (compat -> client DLQ, HT -> server Error Table) | v4 | JSON | FR3 |

#### 3.1.7 Iceberg Tables

| Status | Test | Version | Rationale | Format | Cloud |
|:------:|------|---------|-----------|--------|-------|
| 🔴 | Iceberg JSON ingestion | v4 | Iceberg is a new feature area; v3 Iceberg was experimental | JSON | AWS |
| 🔴 | Iceberg Avro ingestion | v4 | Same | Avro SR | AWS |
| 🔴 | Iceberg SE JSON | v4 | Same | JSON | AWS |
| 🔴 | Iceberg SE Avro | v4 | Same | Avro SR | AWS |

#### 3.1.8 Pre-Flight Check (FR11)

v4-only: high-throughput mode safety check.

| Status | Test | Scenario | Notes |
|:------:|------|----------|-------|
| 🔴 | No Error Table configured -> startup warning/fail | validation=false, no Error Table | FR11 |
| 🔴 | Error Table configured -> startup succeeds | validation=false, Error Table present | FR11 |

---

### 3.2 Error Handling

Error handling is the highest-risk area for v3/v4 parity. In v3, SSv1 always validates and errors are deterministic. In v4, the `RowValidator` (copied from SSv1's `DataValidationUtil`) is a separate layer that can be toggled. **All compatibility-mode error handling tests must be dual** because they directly exercise client-side validation.

#### 3.2.1 Dead Letter Queue -- `errors.tolerance=all` (FR1, Compatibility Mode)

| Status | Test | Version | Format | Error Type | File |
|:------:|------|---------|--------|-----------|------|
| 🟡 | Invalid JSON -> DLQ | dual | JSON | Deserialization | `test_snowpipe_streaming_string_json_dlq.py` (currently v4-only) |
| 🟡 | Schema mapping error -> DLQ | dual | JSON | Type mismatch | `test_snowpipe_streaming_schema_mapping_dlq.py` (needs `validation.enabled=true` + un-skip) |
| 🔴 | DLQ Kafka headers preserved (v3/v4 byte-for-byte comparison) | dual | JSON | Any | FR9: DLQ error messages must be identical between v3 and v4 |
| 🔴 | DLQ with Avro data | dual (v3 blocked) | Avro SR | Deserialization | FR1 -- **parity gap** due to classloader |
| 🔴 | DLQ with Protobuf data | dual (v3 blocked) | Protobuf SR | Deserialization | FR1 -- **parity gap** due to classloader |
| 🔴 | DLQ with multi-partition topics | dual | JSON | Mixed | FR1 |
| 🔴 | DLQ for each Snowflake type (invalid value in typed column) | dual | JSON | Type mismatch | FR1 -- see [Section 4](#4-data-type-compatibility-strategy) |

#### 3.2.2 Abort -- `errors.tolerance=none` (FR2, Compatibility Mode)

| Status | Test | Version | Format | Error Type | Notes |
|:------:|------|---------|--------|-----------|-------|
| 🔴 | Deserialization error -> task FAILED | dual | JSON | Bad payload | FR2 |
| 🔴 | Schema mismatch -> task FAILED | dual | JSON | Type mismatch | FR2 |
| 🔴 | Abort is synchronous and deterministic (v3/v4 comparison) | dual | JSON | Any | FR2 |

#### 3.2.3 Server-Side Error Handling (High-Throughput Mode)

v4-only: no v3 equivalent.

| Status | Test | Version | Format | Notes |
|:------:|------|---------|--------|-------|
| 🔴 | Invalid records -> SSv2 Error Table (not DLQ) | v4 | JSON | FR3 |
| 🔴 | Error Table + schema mismatch | v4 | JSON | FR3 |

---

### 3.3 Schema Evolution

Schema evolution has two code paths:

- **Compatibility mode** (`validation.enabled=true`): Client-side `ALTER TABLE ADD COLUMN` / `ALTER TABLE DROP NOT NULL`. The connector's `RowValidator` detects structural mismatches (extra columns, missing NOT NULL) and the `SnowflakeSchemaEvolutionService` issues DDL.
- **High-throughput mode** (`validation.enabled=false`): Records go directly to SSv2 SDK. Schema evolution depends on the Snowflake table's `ENABLE_SCHEMA_EVOLUTION = TRUE` property -- the server handles it.

#### 3.3.1 Client-Side Schema Evolution (Compatibility Mode)

**Analysis notes:**
- All `se_*.json` config templates set `snowflake.validation=client_side` but do NOT explicitly set `snowflake.enable.schematization`. The v4 default is `true`, so schematization is implicitly on.
- `test_schema_evolution_streaming.py` uses `snowpipe_streaming_schema_evolution.json` which also does not set validation or schematization explicitly (relying on defaults).
- **Overlap detected**: `test_se_nonnullable_json` and `test_schema_evolution_drop_not_null` test the same scenario (NOT NULL column dropped by SE). `test_se_auto_table_creation_json` and `test_schema_evolution_add_columns` partially overlap (new columns added via SE). These should be deduplicated when the SE test branches are merged.
- **Config_variants gap**: `evo=True, schema=False` combos are all skipped with a TODO. `evo=False, schema=True, valid=False` returns early with no assertions.

Tests are dual when they exercise the client-side validation path (structural error detection triggers SE). Tests that only check row counts after SE can be v4-only.

| Status | Test | Version | Rationale | Format | File |
|:------:|------|---------|-----------|--------|------|
| 🟢 | Add columns (JSON, `{city, age}`) | dual | SE triggers via RowValidator structural error detection | JSON | `test_schema_evolution_streaming.py::test_schema_evolution_add_columns` |
| 🟢 | Multi-wave evolution (wave 1 -> wave 2) | dual | Same path -- structural error triggers ADD COLUMN | JSON | `test_schema_evolution_streaming.py::test_schema_evolution_multi_wave` |
| 🟢 | Happy path (schema matches table) | v4 | No SE triggered, no validation-dependent behavior | JSON | `test_schema_evolution_streaming.py::test_schema_evolution_happy_path` |
| 🟢 | Drop NOT NULL constraint | dual | SE triggers via RowValidator null-in-NOT-NULL detection | JSON | `test_schema_evolution_streaming.py::test_schema_evolution_drop_not_null` |
| 🟢 | Disabled mid-stream (toggle SE off) | v4 | Tests DDL privilege, not validation path | JSON | `test_schema_evolution_streaming.py::test_schema_evolution_disabled_mid_stream` |
| 🟢 | Config matrix (8 combos: `evo x schematization x validation`) | dual | Core validation/SE interaction test | JSON | `test_schema_evolution_streaming.py::test_schema_evolution_config_variants` |
| 🟢 | Avro SR with 2 topics, different schemas | v4 | v3 can't auto-create tables for Avro SR with topic2table.map; pre-created tables cause pipe invalidation on ALTER TABLE | Avro SR | `schema_evolution/test_se_avro_sr.py` |
| 🟢 | Auto table creation + SE (JSON, 2 topics) | dual | SE + auto-create triggers via structural error | JSON | `schema_evolution/test_se_auto_table_creation_json.py` |
| 🟢 | Auto table creation + SE (Avro SR, 2 topics) | v4 | Auto table creation is v4-only; v3 requires pre-existing tables | Avro SR | `schema_evolution/test_se_auto_table_creation_avro_sr.py` |
| 🟢 | Non-nullable columns + SE | dual | SE triggers via null-in-NOT-NULL path | JSON | `schema_evolution/test_se_nonnullable_json.py` |
| 🟢 | Tombstone handling during SE | dual | Asserts data values with SE | JSON | `schema_evolution/test_se_json_ignore_tombstone.py` |
| 🟢 | Random batch sizes (flush timing) | dual | Tests timing-sensitive SE path | JSON | `schema_evolution/test_se_random_row_count.py` |
| 🟢 | Nullable values after SMT + SE | dual | SE structural error path | JSON + SMT | `schema_evolution/test_se_nullable_values_after_smt.py` |
| 🟡 | Drop table recovery | dual | **Compatibility gap**: v4 pipes invalidate after DROP TABLE. Must run dual with v4 `xfail`. Currently runs v3-only. | JSON | `schema_evolution/test_se_drop_table.py` |
| 🟡 | Multi-topic drop table recovery | dual | Same compatibility gap. Must run dual with v4 `xfail`. Currently runs v3-only. | JSON | `schema_evolution/test_se_multi_topic_drop_table.py` |

> **Compatibility gap -- DROP TABLE recovery**: v3 can recover from `DROP TABLE` by recreating the streaming pipe. V4 cannot -- `DROP TABLE` permanently invalidates the pipe. These tests should run in dual mode with v4 expected to fail via `xfail`, explicitly documenting the behavioral difference. Migration documentation must warn customers about this gap.

#### 3.3.2 Server-Side Schema Evolution (High-Throughput Mode)

When `snowflake.validation=server_side`, the connector does not perform client-side validation or DDL. Records go directly to the SSv2 SDK's `channel.appendRow()`. Schema evolution depends entirely on the Snowflake table property `ENABLE_SCHEMA_EVOLUTION = TRUE`.

Note: The connector source has no `MATCH_BY_COLUMN_NAME` or FDN-specific logic. "Server-side SE" means the Snowflake service handles schema mismatches for tables with `ENABLE_SCHEMA_EVOLUTION = TRUE`.

The `test_schema_evolution_config_variants` test already covers `evo=True, schema=True, valid=False` for v4 (server-side SE with schematization on). However, important gaps remain:

| Status | Test | Version | Format | Notes | Suggested File |
|:------:|------|---------|--------|-------|----------------|
| 🟡 | Server-side SE: new columns added (validation off, SE on) | v4 | JSON | config_variants tests this combo but with minimal records | `test_schema_evolution_ht.py` |
| 🔴 | Server-side SE: NOT NULL dropped (validation off, SE on) | v4 | JSON | | `test_schema_evolution_ht.py` |
| 🔴 | Server-side SE: schematization off (validation off, SE on, schema off) | v4 | JSON | config_variants skips this combo (TODO) | `test_schema_evolution_ht.py` |
| 🔴 | Server-side SE with Avro SR | v4 | Avro SR | FR6 | `test_schema_evolution_ht.py` |
| 🔴 | Server-side SE disabled: records rejected to Error Table | v4 | JSON | config_variants returns early with no assertions | `test_schema_evolution_ht.py` |
| 🔴 | Concurrent SE from multiple partitions | v4 | JSON | Tests race condition in ALTER TABLE from multiple tasks | `test_schema_evolution_ht.py` |

---

### 3.4 RECORD_CONTENT Mode

`snowflake.enable.schematization=false` -- data lands in `RECORD_CONTENT` + `RECORD_METADATA` VARIANT columns (FR4).

**RECORD_CONTENT mode tests MUST be dual**: `RECORD_CONTENT` is a VARIANT column, and data type handling between SSv1/SSv2 may differ (see root cause #1 in "When Dual Testing is Required"). The existing tests already have defensive double-decoding (`isinstance(content, str)` check), confirming this risk.

| Status | Test | Version | Rationale | Format | File |
|:------:|------|---------|-----------|--------|------|
| 🟡 | RECORD_CONTENT JSON (StringConverter key, JsonConverter value) | dual | Data type handling may differ in VARIANT columns. Currently v4-only. | JSON (native) | `test_snowpipe_streaming_legacy_string_json.py` |
| 🟡 | RECORD_CONTENT StringConverter (raw string payload) | dual | Same -- raw string in VARIANT. Currently v4-only. | String | `test_snowpipe_streaming_legacy_string_converter.py` |
| 🟡 | RECORD_CONTENT ByteArrayConverter (base64 payload) | dual | Same -- binary encoding in VARIANT. Currently v4-only. | Bytes | `test_snowpipe_streaming_legacy_byte_array_converter.py` |
| 🔴 | RECORD_CONTENT + Avro SR | dual (v3 blocked) | Parity gap: classloader | Avro SR | -- |
| 🔴 | RECORD_CONTENT + SMT (nullable values, ExtractField) | dual | Data values in VARIANT + SMT interaction. v3 equivalent: `TestSnowpipeStreamingNullableValuesAfterSmt` | JSON + SMT | -- |

---

### 3.5 Connector Lifecycle & Resilience

All tests send data in phases, performing disruptive operations between sends. These are v4-only: lifecycle operations (pause/resume/restart/delete) are Kafka Connect framework behavior, not SDK-dependent. The connector's interaction with the KC REST API is identical regardless of SSv1 vs SSv2.

> **Note on ingestion pattern**: Existing lifecycle tests send a batch, perform the disruptive operation, then send another batch. This "phase-based" approach may not sufficiently exercise interleaving — if all data lands within a single flush cycle, the disruption happens in a quiet window. New resilience tests (channel invalidation, backend errors, network partitions) should use a **continuous ingestion** pattern: a background producer sends records throughout the test while disruptions occur, ensuring the connector handles mid-flight interruptions.

#### 3.5.1 Lifecycle Tests (existing)

| Status | Test | Operation Sequence | Version | File |
|:------:|------|-------------------|---------|------|
| 🟢 | Restart (task + connector) | send -> restart -> send -> restart -> send | v4 | `test_kc_restart.py` |
| 🟢 | Delete -> Create (new connector, same name) | send -> delete -> create -> send | v4 | `test_kc_delete_create.py` |
| 🟢 | Delete -> Create + Chaos | send -> delete -> create (with failures) -> send | v4 | `test_kc_delete_create_chaos.py` |
| 🟢 | Delete -> Resume (new connector, inherits offsets) | send -> delete -> resume -> send | v4 | `test_kc_delete_resume.py` |
| 🟢 | Delete -> Resume + Chaos | send -> delete -> resume (with failures) -> send | v4 | `test_kc_delete_resume_chaos.py` |
| 🟢 | Pause -> Create (new connector while paused) | send -> pause -> create -> send | v4 | `test_kc_pause_create.py` |
| 🟢 | Pause -> Create + Chaos | send -> pause -> create (with failures) -> send | v4 | `test_kc_pause_create_chaos.py` |
| 🟢 | Pause -> Resume (same connector) | send -> pause -> resume -> send | v4 | `test_kc_pause_resume.py` |
| 🟢 | Pause -> Resume + Chaos | send -> pause -> resume (with failures) -> send | v4 | `test_kc_pause_resume_chaos.py` |
| 🟢 | Recreate (multiple delete/create cycles) | send -> delete -> recreate -> send x2 | v4 | `test_kc_recreate.py` |
| 🟢 | Recreate + Chaos | multiple cycles with failures | v4 | `test_kc_recreate_chaos.py` |

#### 3.5.2 Fault Injection & Recovery (missing)

These tests should use continuous ingestion (background producer) to exercise mid-flight fault handling.

| Status | Test | Fault Type | Version | Notes |
|:------:|------|-----------|---------|-------|
| 🔴 | Channel invalidation recovery | Server-side channel drop; client must detect and re-open | v4 | Verify no data loss after channel re-open under continuous load |
| 🔴 | Backend 5xx error handling | Simulated server errors during ingestion | v4 | Verify backoff/retry and eventual recovery |
| 🔴 | Backend 429 throttling | Rate-limit responses during ingestion | v4 | Verify connector backs off and resumes without data loss |
| 🔴 | Network partition tolerance | Temporary connectivity loss between KC worker and Snowflake | v4 | Verify connector recovers after partition heals, no duplicate/lost records |

---

### 3.6 Default Pipe Features

FR5 (Default Pipes only) + FR7 (Default Pipe Improvements). These are v4-only (no v3 equivalent) but must be tested in both compatibility and high-throughput modes.

| Status | Test | Feature | Mode | Version | Suggested File |
|:------:|------|---------|------|---------|----------------|
| 🔴 | Auto-Increment (Identity) columns | FR7 | Compatibility | v4 | `test_default_pipe_features.py` |
| 🔴 | Auto-Increment (Identity) columns | FR7 | High-Throughput | v4 | `test_default_pipe_features.py` |
| 🔴 | Default timestamp properties | FR7 | Compatibility | v4 | `test_default_pipe_features.py` |
| 🔴 | Default timestamp properties | FR7 | High-Throughput | v4 | `test_default_pipe_features.py` |
| 🔴 | Pre-clustered tables | FR7 | Compatibility | v4 | `test_default_pipe_features.py` |
| 🔴 | Pre-clustered tables | FR7 | High-Throughput | v4 | `test_default_pipe_features.py` |

---

### 3.7 Load & Stress

> **Scope**: These are CI-level smoke/pressure tests that run in pre-commit. They verify the connector handles moderate load without failures but are not intended to represent production-scale benchmarking. Dedicated load and benchmarking tests exist separately for validating throughput at scale (e.g., 10 GB/s target for high-throughput mode).

| Status | Test | Scale | Version | File |
|:------:|------|-------|---------|------|
| 🟢 | Pressure: 200 topics x 12 partitions x 10K records (24M total) | High | v4 | `test_pressure.py` |
| 🟢 | Pressure + Restart: 10 topics x 3 partitions x 200K records with chaos ops | High | v4 | `test_pressure_restart.py` |

---

## 4. Data Type Compatibility Strategy

This section addresses the critical question: **Does v4 compatibility mode handle every Snowflake data type the same way v3 does?**

### Background: How Type Validation Works

V4's client-side validation (`RowValidator`) uses `DataValidationUtil` -- code **copied from the SSv1 Ingest SDK** into the KC v4 codebase. This copy can drift from the actual SDK. The validation chain is:

1. **Kafka Connect converter** deserializes the message (JsonConverter, AvroConverter, etc.)
2. **KC `KafkaRecordConverter`** transforms Kafka Connect types (Struct/Map) to `Map<String, Object>`
3. **`RowValidator.validateRow()`** (when `validation.enabled=true`) checks each value against the target column's Snowflake type using `DataValidationUtil`
4. **SSv2 SDK `channel.appendRow()`** performs its own validation (SSv2's validation layer)

Parity risk: v3 relied on step 4 only (SSv1's built-in validation). V4 adds step 3 (copied SSv1 code) plus uses SSv2 in step 4. If the copied code drifts from SSv1, or SSv2 validates differently than SSv1, behavior diverges.

### Existing Unit Test Coverage (KC Java Tests)

- **`DataValidationUtilTest.java`**: Copied from SSv1 SDK. Covers DATE, TIME, all TIMESTAMP variants, BIGDECIMAL/FIXED, STRING, VARIANT, ARRAY, OBJECT, BINARY, REAL, BOOLEAN. Both positive and negative cases.
- **`RowValidatorTest.java`**: Structural validation (extra columns, missing NOT NULL, null in NOT NULL). Type rejection for structured OBJECT/ARRAY and collated columns.
- **`SnowflakeColumnTypeMapperTest.java`**: Kafka Connect type -> Snowflake DDL mapping.
- **`ConverterTest.java`**: Kafka Connect record -> Map conversion. Decimal precision, Time/Date/Timestamp logical types, NaN/Infinity.

### Per-Type E2E Coverage and Test Plan

All new E2E type tests go into **`test_type_compatibility.py`** (JSON format, dual mode) and **`test_type_compatibility_avro.py`** (Avro SR format, dual but v3 blocked). Each test covers both **positive** (valid values land correctly) and **negative** (invalid values routed to DLQ) cases for that type.

| Status | Snowflake Type | SSv1 (v3) | V4 RowValidator | SSv2 (v4) | What to Test | Notes |
|:------:|----------------|-----------|-----------------|-----------|-------------|-------|
| 🟢 | NUMBER / INT / BIGINT / SMALLINT / TINYINT / BYTEINT | Supported | FIXED | Supported | Positive: each integer subtype with valid values. Negative: overflow beyond NUMBER(38,0), precision loss for NUMBER(p,s), string in NUMBER column. | `test_type_compatibility.py`: `test_dt_number`, `test_dt_number_with_scale`. v3 passes, v4 passes. DLQ routing not yet verified (infra bug). |
| 🟢 | FLOAT / DOUBLE / REAL | Supported | REAL | Supported | Positive: normal floats, NaN, +/-Infinity. Negative: string in FLOAT column, extreme precision values. | `test_type_compatibility.py`: `test_dt_float`, `test_dt_float_special`. Both modes pass for positive values. NaN/Inf parity confirmed. |
| 🟡 | BOOLEAN | Supported | Supported | Supported | Positive: true/false, 0/1, "true"/"false" coercion. Negative: arbitrary string in BOOLEAN column, number > 1. | `test_type_compatibility.py`: `test_dt_boolean`, `test_dt_boolean_coercion`. **DIVERGENCE**: v4 rejects "yes"/"no" tokens; v4 maps 0->True, 1->False (reversed vs v3). See findings below. |
| 🟢 | VARCHAR / STRING / TEXT / CHAR | Supported | TEXT/CHAR | Supported | Positive: normal strings, Unicode, multi-byte chars, VARCHAR(n) within limit, CHAR(n). Negative: exceeding VARCHAR(n) limit, exceeding 16 MB max. | `test_type_compatibility.py`: `test_dt_varchar`, `test_dt_varchar_length_limit`. Both modes pass. |
| 🟡 | BINARY / VARBINARY | Supported | Supported | **Partial** | Positive: hex strings, byte arrays of various lengths. Negative: exceeding max length, invalid hex. | `test_type_compatibility.py`: `test_dt_binary`. **DIVERGENCE**: v3 ingests 4/4 hex strings; v4 only 2/4 (SNOW-3256183). |
| 🟢 | DATE | Supported | Supported | Supported | Positive: ISO format, epoch days. Negative: invalid dates (Feb 29 non-leap year), out-of-range dates. | `test_type_compatibility.py`: `test_dt_date`. v3 and v4 positive values match. DLQ not yet verified. |
| 🟢 | TIME | Supported | Supported | Supported | Positive: TIME(0) through TIME(9), various formats. Negative: invalid time values (25:00:00), wrong scale. | `test_type_compatibility.py`: `test_dt_time`. v3 and v4 positive values match. DLQ not yet verified. |
| 🟡 | TIMESTAMP_NTZ | Supported | Supported | **Partial** | Positive: ISO format, epoch, various scales. Negative: invalid timestamps, out-of-range. | `test_type_compatibility.py`: `test_dt_timestamp_ntz`, `test_dt_timestamp_ntz_epoch`. **DIVERGENCE**: v4 rejects integer epoch (java.lang.Long). ISO strings work in both modes. |
| 🟢 | TIMESTAMP_LTZ | Supported | Supported | Supported | Positive: timezone-aware values, Instant. Negative: invalid timezone, out-of-range. | `test_type_compatibility.py`: `test_dt_timestamp_ltz`. v3 and v4 positive values match. |
| 🟢 | TIMESTAMP_TZ | Supported | Supported | Supported | Positive: values with offset (+05:30), ZonedDateTime. Negative: invalid offset format. | `test_type_compatibility.py`: `test_dt_timestamp_tz`. v3 and v4 positive values match. |
| 🟡 | VARIANT | Supported | Supported | Supported | Positive: nested JSON objects, arrays, primitives. Negative: exceeding 16 MB size limit, malformed JSON. | `test_type_compatibility.py`: `test_dt_variant`. Positive values match. **DIVERGENCE**: NULL in VARIANT column stored as string `'null'` in v4 vs SQL NULL in v3. |
| 🟢 | ARRAY (unstructured) | Supported | Supported | Supported | Positive: nested arrays, mixed types. Negative: exceeding 16 MB, non-array in ARRAY column. | `test_type_compatibility.py`: `test_dt_array`. Both modes pass. |
| 🟢 | OBJECT (unstructured) | Supported | Supported | Supported | Positive: nested objects, mixed value types. Negative: exceeding 16 MB, non-object in OBJECT column. | `test_type_compatibility.py`: `test_dt_object`. Both modes pass. |
| 🟡 | ARRAY (structured) | Rejected | Expected: Rejected | **Accepted** | Negative: verify RowValidator rejects structured ARRAY for non-Iceberg tables. | `test_unsupported_types.py`: `test_dt_structured_array`. **DIVERGENCE**: v3 rejects, v4 ACCEPTS structured ARRAY(NUMBER) and ingests data. Contradicts ColumnSchema.java analysis. |
| 🟡 | OBJECT (structured) | Rejected | Expected: Rejected | **Accepted** | Negative: verify RowValidator rejects structured OBJECT for non-Iceberg tables. | `test_unsupported_types.py`: `test_dt_structured_object`. **DIVERGENCE**: v3 rejects, v4 ACCEPTS structured OBJECT(name VARCHAR, age NUMBER). |
| 🔴 | Collated VARCHAR | Rejected | Rejected | Rejected | Negative: verify columns with collation are rejected at startup or DLQ. | `RowValidatorTest.java` unit-tests this. |
| 🟢 | NULL (all types) | Supported | Supported | Supported | Positive: NULL in every supported column type. Verify NULL is stored correctly, not coerced. | `test_type_compatibility.py`: `test_dt_null` (12 types parametrized). All pass except VARIANT (see VARIANT note above). |
| 🟡 | Type mismatch (cross-type) | DLQ/error | DLQ/error | DLQ/error | Negative: string in NUMBER, number in BOOLEAN, object in STRING, etc. Verify DLQ routing is identical v3/v4. | `test_type_compatibility.py`: 4 cross-type tests. **BLOCKED**: DLQ infrastructure bug prevents DLQ count verification. Also **DIVERGENCE**: v4 silently drops object->VARCHAR (v3 stringifies and ingests). |
| ⚪ | GEOGRAPHY / GEOMETRY | Not supported | Not supported | Not supported | N/A: not supported in Snowpipe Streaming. | `test_unsupported_types.py`: both modes correctly reject. |
| 🟡 | VECTOR | Not in v3 SDK | Not supported | Supported | Positive: VECTOR(FLOAT, 3) embeddings. | `test_unsupported_types.py`: `test_dt_vector`. v3 skipped, v4 passes. |
| ⚪ | MAP | Iceberg only | Not supported | Iceberg only | Out of scope for non-Iceberg tests. | -- |

### E2E Test Results: Confirmed v3/v4 Divergences

Tests run: 78 (31 failed, 46 passed, 1 skipped). Test files: `test_type_compatibility.py`, `test_unsupported_types.py`. Platform: Apache Kafka 3.7.0. Run time: ~50 min.

**Infrastructure issue**: The batch DLQ reader (`DlqReader`) returns `dlq_count=0` for all tests in both modes. This is a test infrastructure bug -- the bad records are either not routed to the shared DLQ topic by the batch connector, or the Kafka consumer isn't draining properly. DLQ assertions are currently unreliable. **12 of 31 failures are caused by this bug, not by behavioral differences.**

#### Confirmed Behavioral Divergences (v4-compat differs from v3)

| # | Type | Test | v3 Behavior | v4-compat Behavior | Severity | Ticket |
|---|------|------|-------------|-------------------|----------|--------|
| D1 | BINARY | `test_dt_binary` | 4/4 hex strings ingested | 2/4 ingested (server-side cast failure) | High | SNOW-3256183 |
| D2 | BOOLEAN | `test_dt_boolean_coercion` | `[False,True,True,False,True,False]` for `[0,1,"true","false","yes","no"]` | `[True,False,True,False]` -- "yes"/"no" rejected, 0->True/1->False reversed | High | -- |
| D3 | TIMESTAMP_NTZ | `test_dt_timestamp_ntz_epoch` | Integer epoch `1705312800` ingested as `2024-01-15T10:00:00` | 0 rows, 0 DLQ -- epoch integer silently dropped | High | -- |
| D4 | NULL VARIANT | `test_dt_null[variant]` | SQL NULL (`None`) | JSON string `'null'` | Medium | -- |
| D5 | Object->VARCHAR | `test_dt_xtype_object_to_varchar` | 1 row (stringified JSON) | 0 rows, 0 DLQ -- silently dropped | Medium | -- |
| D6 | Structured OBJECT | `test_dt_structured_object` | Rejected (connector error) | **Accepted** -- 1 row ingested | Medium | -- |
| D7 | Structured ARRAY | `test_dt_structured_array` | Rejected (connector error) | **Accepted** -- 1 row ingested | Medium | -- |

**D2 detail**: The boolean coercion test reveals two sub-issues: (a) v4 RowValidator rejects string tokens "yes"/"no" that v3 SSv1 accepts via `DataValidationUtil.validateAndParseBoolean`; (b) v4 maps integer 0->True and 1->False (reversed from v3's 0->False, 1->True). The reversal suggests a bug in the v4 boolean parsing path.

**D6/D7 detail**: Contradicts the ColumnSchema.java analysis (lines 326-349 should reject structured types). The v4 SSv2 SDK appears to accept structured OBJECT/ARRAY columns for non-Iceberg tables, while v3 rejects them. This is the opposite of what was expected.

#### Tests Passing in Both Modes (no divergence)

NUMBER (positive), NUMBER(10,2) (positive), FLOAT (positive + NaN/Inf), VARCHAR, VARCHAR(10), DATE (positive), TIME (positive), TIMESTAMP_NTZ (ISO strings), TIMESTAMP_LTZ (positive), TIMESTAMP_TZ (positive), VARIANT (JSON objects/arrays), OBJECT, ARRAY, NULL (11 of 12 types), GEOGRAPHY (rejected), GEOMETRY (rejected), VECTOR (v4-only, passes).

### Avro-Specific Type Mapping (`test_type_compatibility_avro.py`)

Avro has its own type system. These tests are dual but v3 is blocked by the classloader conflict (⚫ parity gap).

| Status | Avro Types | SF Target Types | What to Test | Notes |
|:------:|-----------|-----------------|-------------|-------|
| ⚫ | int, long, float, double, bytes (decimal) | NUMBER, BIGINT, FLOAT, DOUBLE | Positive: each Avro numeric -> correct SF type. Negative: decimal overflow. | SDK ref: `NumericTypesIT.java` |
| ⚫ | date, time-millis, time-micros, timestamp-millis, timestamp-micros | DATE, TIME, TIMESTAMP_NTZ/LTZ | Positive: each Avro logical type -> correct SF type. Negative: out-of-range. | SDK ref: `DateTimeIT.java` |
| ⚫ | string, bytes, boolean, enum | VARCHAR, BINARY, BOOLEAN, VARCHAR | Positive: each primitive -> correct SF type. Negative: size overflow. | SDK ref: `StringsIT.java`, `BinaryIT.java` |
| ⚫ | record, array, map, union | VARIANT, ARRAY, OBJECT | Positive: complex Avro types -> SF semi-structured. Negative: size overflow. | SDK ref: `SemiStructuredIT.java` |

### Alignment with ingest-java SDK Tests

| SDK Test | Covers | KC E2E Equivalent |
|----------|--------|-------------------|
| `NumericTypesIT.java` | INT, NUMBER(p,s), REAL with all Java input types + out-of-range errors | `test_type_compatibility.py` numeric tests |
| `LogicalTypesIT.java` | BOOLEAN with various input types | `test_type_compatibility.py` boolean tests |
| `DateTimeIT.java` | DATE, TIME(n), TIMESTAMP_NTZ/LTZ/TZ with all Java date/time types | `test_type_compatibility.py` date/time/timestamp tests |
| `StringsIT.java` | VARCHAR, VARCHAR(n), CHAR(n) with length limits, multi-byte chars | `test_type_compatibility.py` string tests |
| `BinaryIT.java` | BINARY with byte arrays and hex strings | `test_type_compatibility.py` binary tests |
| `SemiStructuredIT.java` | VARIANT, OBJECT, ARRAY with all sub-types + size limits | `test_type_compatibility.py` semi-structured tests |
| `NullIT.java` | NULL for every supported type | `test_type_compatibility.py` null test |

The E2E tests do not need to duplicate every SDK test case. They should focus on:
1. **End-to-end pipeline fidelity**: Does the value survive Kafka -> converter -> KC -> SDK -> Snowflake without corruption?
2. **v3/v4 parity**: Do v3 and v4 produce the same Snowflake column values for the same Kafka input?
3. **Boundary behavior**: Do out-of-range/invalid values get routed to DLQ identically in v3 and v4? (FR1)

---

## 5. Coverage Matrix

### By Data Format x Test Category

| Category | JSON (native) | Avro SR | Protobuf SR | Protobuf (native) | String | Bytes |
|----------|:---:|:---:|:---:|:---:|:---:|:---:|
| Basic Ingestion | 🟢 | ⚫ | ⚫ | 🟢 | 🟢 (legacy) | 🟢 (legacy) |
| Type Compatibility | 🟡 | 🔴 | -- | -- | -- | -- |
| Schema Evolution (client) | 🟢 | ⚫ | 🔴 | -- | -- | -- |
| Schema Evolution (server) | 🔴 | 🔴 | -- | -- | -- | -- |
| DLQ (compat) | 🟡 | 🔴 | 🔴 | -- | -- | -- |
| Abort (compat) | 🔴 | -- | -- | -- | -- | -- |
| Error Tables (HT) | 🔴 | -- | -- | -- | -- | -- |
| RECORD_CONTENT Mode | 🟡 | 🔴 | -- | -- | 🟡 | 🟡 |
| Table Creation | -- | ⚫ | -- | -- | -- | -- |
| Resilience (lifecycle) | 🟢 | -- | -- | -- | -- | -- |
| Resilience (fault injection) | 🔴 | -- | -- | -- | -- | -- |
| Load/Stress | 🟢 | -- | -- | -- | -- | -- |

### By Connector Mode x Test Category

| Category | Compatibility (dual where needed) | High-Throughput (v4-only) |
|----------|:---:|:---:|
| Basic Ingestion | 🟢 (2 dual, rest v4-only -- justified) | 🔴 (2 tests) |
| Type Compatibility | 🟡 (tests written, DLQ infra fix needed) | ⚪ |
| Schema Evolution (client) | 🟡 (7 🟢, 2 ⚫, 2 🟡 xfail) | -- |
| Schema Evolution (server) | -- | 🔴 (6 tests) |
| DLQ | 🟡 (1 needs dual, 5 missing) | ⚪ |
| Abort | 🔴 (3 tests) | ⚪ |
| Error Tables | ⚪ | 🔴 (2 tests) |
| Pre-flight Check | ⚪ | 🔴 (2 tests) |
| RECORD_CONTENT Mode | 🟡 (3 need dual, 2 missing) | -- |
| Table Creation | ⚫ (v3 blocked) | 🔴 (1 test) |
| Default Pipe Features | 🔴 (3 tests) | 🔴 (3 tests) |
| Resilience (lifecycle) | 🟢 (v4-only) | -- |
| Resilience (fault injection) | 🔴 (4 tests) | -- |
| Load/Stress | 🟢 (v4-only) | -- |

### Parity Status Summary

| Category | 🟢 Confirmed | ⚫ v3 Blocked | 🟡 Needs Rework | 🔴 New Needed |
|----------|:-:|:-:|:-:|:-:|
| Data Ingestion | 2 | 4 | 0 | 0 |
| Type Compatibility | 10 | 4 | 7 | 1 (collated) |
| Error Handling (DLQ) | 0 | 2 | 1 | 3 + type DLQ |
| Error Handling (Abort) | 0 | 0 | 0 | 3 |
| Schema Evolution | 9 | 0 | 2 | 0 |
| RECORD_CONTENT Mode | 0 | 1 | 3 | 1 |
| Table Creation | 0 | 2 | 0 | 1 |
| Resilience (fault injection) | 0 | 0 | 0 | 4 |

---

## 6. Priority & Sequencing

### P0 -- GA Blockers

Must be complete before GA. Focus on type compatibility and error handling parity.

| # | Status | Test | FR | Category | Work Type |
|---|:------:|------|----|---------|----|
| 1 | 🟡 | `test_type_compatibility.py`: positive type tests for all Snowflake types (dual) | FR1 | Type parity | Written -- 22 tests, 5 divergences found. DLQ assertions blocked by infra bug. |
| 2 | 🟡 | `test_type_compatibility.py`: negative type tests -- DLQ routing for invalid values (dual) | FR1 | Type parity | Written -- 4 cross-type + 6 per-type DLQ tests. DLQ reader infra needs fix. |
| 3 | 🟡 | Convert DLQ test to dual + un-skip schema mapping DLQ | FR1 | DLQ parity | Convert |
| 4 | 🔴 | DLQ Kafka headers preserved (v3/v4 byte-for-byte comparison) | FR9 | DLQ parity | New |
| 5 | 🔴 | Abort: deserialization error -> task FAILED (dual) | FR2 | Abort | New |
| 6 | 🔴 | Abort: schema mismatch -> task FAILED (dual) | FR2 | Abort | New |
| 7 | 🟡 | Convert `test_schema_mapping.py` to dual | FR1 | Type parity | Convert |
| 8 | 🟡 | Convert 3 RECORD_CONTENT mode tests to dual | FR4 | RECORD_CONTENT parity | Convert |
| 9 | 🟡 | Rework DROP TABLE tests: dual with v4 xfail | FR1 | Compat gap | Rework |
| 10 | 🔴 | Validation toggle default = `true` | FR3 | Config | New |
| 11 | 🔴 | Pre-flight check: no Error Table -> startup fail | FR11 | Pre-flight | New |
| 12 | 🔴 | Pre-flight check: Error Table present -> startup OK | FR11 | Pre-flight | New |

### P1 -- GA Completeness

| # | Status | Test | FR | Category | Work Type |
|---|:------:|------|----|---------|----|
| 13 | 🔴 | High-throughput basic ingestion (JSON) | FR3 | HT Mode | New |
| 14 | 🔴 | High-throughput basic ingestion (Avro SR) | FR3 | HT Mode | New |
| 15 | 🔴 | Server-side SE: add columns (validation off, SE on) | FR6 | HT Mode | New |
| 16 | 🔴 | Server-side SE: NOT NULL dropped | FR6 | HT Mode | New |
| 17 | 🔴 | Server-side SE: schematization off (currently skipped combo) | FR6 | HT Mode | New |
| 18 | 🔴 | Server-side SE with Avro SR | FR6 | HT Mode | New |
| 19 | 🔴 | Server-side SE disabled -> Error Table | FR6 | HT Mode | New |
| 20 | 🔴 | Invalid records -> Error Table (high-throughput) | FR3 | HT Mode | New |
| 21 | 🔴 | DLQ with Avro data (dual, v3 blocked) | FR1 | DLQ | New |
| 22 | 🔴 | DLQ with multi-partition topics (dual) | FR1 | DLQ | New |
| 23 | 🔴 | RECORD_CONTENT + Avro SR (dual, v3 blocked) | FR4 | RECORD_CONTENT | New |
| 24 | 🔴 | RECORD_CONTENT + SMT (nullable values) | FR4 | RECORD_CONTENT | New |
| 25 | 🔴 | Auto table creation (high-throughput mode) | FR3 | Table Creation | New |
| 26 | 🔴 | Default Pipe: Identity column (both modes) | FR7 | Default Pipe | New |
| 27 | 🔴 | Default Pipe: Default timestamp (both modes) | FR7 | Default Pipe | New |
| 28 | 🔴 | Iceberg JSON (AWS) | General | Iceberg | New |
| 29 | 🔴 | Iceberg Avro (AWS) | General | Iceberg | New |
| 30 | 🔴 | Iceberg SE JSON (AWS) | FR6 | Iceberg | New |
| 31 | 🔴 | Iceberg SE Avro (AWS) | FR6 | Iceberg | New |
| 32 | 🔴 | Streaming client parameter override | General | Config | New |
| 33 | ⚫ | `test_type_compatibility_avro.py`: Avro type mapping (dual, v3 blocked) | FR1 | Type parity | New |
| 34 | 🔴 | Resolve SR classloader conflict (unblocks ~15 parity gaps) | FR1 | Infra | Infra |
| 35 | 🔴 | Channel invalidation recovery (continuous ingestion) | FR8 | Resilience | New |
| 36 | 🔴 | Backend 5xx / 429 error handling | FR8 | Resilience | New |
| 37 | 🔴 | Network partition tolerance | FR8 | Resilience | New |

### P2 -- Post-GA Polish

| # | Status | Test | FR | Category | Work Type |
|---|:------:|------|----|---------|----|
| 38 | 🔴 | DLQ with Protobuf data (dual, v3 blocked) | FR1 | DLQ | New |
| 39 | 🔴 | Large blob ingestion (20 MiB JSON) | General | Ingestion | New |
| 40 | 🔴 | Default Pipe: Pre-clustered tables (both modes) | FR7 | Default Pipe | New |
| 41 | 🔴 | Concurrent SE from multiple partitions | FR6 | SE | New |
| 42 | 🔴 | Telemetry: validation toggle usage emitted | FR10 | Telemetry | New |
| 43 | 🔴 | Validation toggle interaction with SE (client vs server) | FR3 | Config | New |

### Summary

| Priority | 🔴 New | 🟡 Rework | ⚫ Blocked | Total |
|----------|:------:|:---------:|:---------:|:-----:|
| P0 (GA Blockers) | 7 | 5 | 0 | **12** |
| P1 (GA Completeness) | 23 | 0 | 1 | **24** |
| P2 (Post-GA) | 6 | 0 | 0 | **6** |
| Already covered | -- | -- | -- | **~50 🟢** |
| **Total** | **36** | **5** | **1** | **42** |

### Known Parity Gaps (Classloader Blocked)

These tests need dual verification but v3 is currently blocked by the SR classloader conflict. They are tracked as a single infrastructure item (P1 #34). The fix requires a **production code change** (being addressed by @sfc-gh-alhuang). Until resolved, these represent **unverified parity assumptions**:

| Test Area | Count | Risk |
|-----------|:-----:|------|
| Avro SR ingestion (data type handling) | 3 | SSv1/SSv2 may serialize Avro types differently |
| Protobuf SR ingestion | 1 | Same risk for Protobuf types |
| Avro table creation | 2 | Type inference from Avro schema not verified against v3 |
| Avro type compatibility | 4 | Full Avro type system not verified |
| DLQ with Avro/Protobuf | 2 | Error handling for SR formats not verified |
| RECORD_CONTENT + Avro SR | 1 | VARIANT content with Avro not verified |
| **Total blocked** | **13** | |
