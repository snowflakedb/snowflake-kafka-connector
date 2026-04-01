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

### Status Legend


| Icon | Meaning |
|------|---------|
| 🟢 | Done -- test exists and passes |
| 🟡 | Known divergence -- test documents behavioral difference between v3 and v4, or has a known gap |
| 🔴 | Missing -- test must be written |

### Priority (for 🔴 items)

| Tier | Meaning | Criteria |
|------|---------|----------|
| **P0** | GA blocker | Data correctness risk, no existing coverage, explicit FR requirement |
| **P1** | Should have for GA | Important for launch or fast-follow, partial coverage exists |
| **P2** | Deferred post-GA | Lower risk, high implementation cost, or substantial existing coverage |

---

## 1. Overview

### Background

Kafka Connector v4 replaces v3's dual ingestion engines (file-based Snowpipe + SSv1 Streaming) with SSv2 exclusively. The GA strategy requires **functional parity** with v3 in compatibility mode, plus a new high-throughput mode.

### Connector Operating Modes

| Mode | Config | Validation | Schema Evolution | Error Handling | Target Use Case |
|------|--------|-----------|-----------------|----------------|-----------------|
| **Compatibility** | `snowflake.validation=client_side` | Client-side | Client-side `ALTER TABLE` | Sync DLQ / Sync Abort | v3 migration, parity |
| **High-Throughput** | `snowflake.validation=server_side` (default) | None (server) | Server-side (table `ENABLE_SCHEMA_EVOLUTION`) | Async Error Tables | Max throughput (10 GB/s target) |

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

### Architecture

| Value | Config | Behavior |
|-------|--------|----------|
| `server_side` (default) | `snowflake.validation=server_side` | Server-side only, Error Tables |
| `client_side` | `snowflake.validation=client_side` | Client-side validation, DLQ, abort |

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
| 🟢 | Tombstone handling (`behavior.on.null.values=IGNORE`) | v4 | v4-only; dual coverage pending. Asserts data values in VARIANT header column. | `test_snowpipe_streaming_string_json_ignore_tombstone.py` |
| 🔴 | Large blob ingestion (20 MiB JSON) | v4 | **P2.** Row count check; tests SDK buffer limits, not validation. v3 equivalent: `TestLargeBlobSnowpipe` | -- |

#### 3.1.2 Avro (Compatibility Mode)

These tests were originally ported from v3 with identical assertions. v4 was run against those assertions on Confluent 7.8.0 (2026-03-31) and all passed — confirming v4 produces identical results. v3 itself cannot run in the current infrastructure due to the SR classloader conflict, but parity is indirectly verified through the captured reference assertions.

| Status | Test | Version | Rationale | File |
|:------:|------|---------|-----------|------|
| 🟢 | String keys + Avro SR values | v4 | Assertions capture v3 reference behavior (ported from v3). v4 parity confirmed 2026-03-31. v3 cannot run due to SR classloader conflict. | `test_string_avrosr.py` |
| 🟢 | Avro SR keys + Avro SR values (NaN, Inf) | v4 | Same -- v4 parity confirmed 2026-03-31. | `test_avrosr_avrosr.py` |
| 🟢 | Snowpipe Streaming + Avro SR (3p x 1000) | v4 | Same -- v4 parity confirmed 2026-03-31. | `test_snowpipe_streaming_string_avro_sr.py` |

#### 3.1.3 Protobuf (Compatibility Mode)

| Status | Test | Version | Rationale | File |
|:------:|------|---------|-----------|------|
| 🟢 | Confluent Protobuf SR (nested types, special floats) | v4 | Assertions capture v3 reference behavior (ported from v3). v4 parity confirmed 2026-03-31. V3 cannot run due to SR classloader conflict. | `test_confluent_protobuf_protobuf.py` |
| 🟢 | Native Protobuf (raw bytes, no SR) | v4 | Protobuf deserialization is converter-level, not SDK | `test_native_string_protobuf.py` |

#### 3.1.4 Schema & Type Mapping (Compatibility Mode)

The existing `test_schema_mapping.py` is the beginning of type compatibility testing but has significant gaps. It will be **subsumed by the comprehensive `test_type_compatibility.py`** proposed in [Section 4](#4-data-type-compatibility-strategy). The new test file extends coverage to all Snowflake types, adds negative cases, and runs in dual mode.

| Status | Test | Version | Rationale | File |
|:------:|------|---------|-----------|------|
| 🟢 | Type mapping (JSON): 10 types, positive only | v4 | Superseded by `test_type_compatibility.py` for comprehensive dual-mode coverage. | `test_schema_mapping.py` |
| 🟢 | Unsupported converter rejection | v4 | Converter rejection is KC framework level | `test_schema_not_supported_converter.py` |

#### 3.1.5 Table Creation

Auto table creation requires the connector to infer column types from the incoming data schema. Table creation itself is converter-independent — testing with a single converter (Avro SR, which provides an explicit schema) is sufficient.

> **Note on v3 reference capture**: The v3-first reference capture technique (used for SR tests in PR #1398) is not feasible here because these tests require Confluent Schema Registry, which triggers the v3 classloader conflict. However, these tests only assert table schema and row counts — not data values — so parity risk is low. The table creation DDL is generated by the same converter code in both versions.

| Status | Test | Version | Rationale | Format | File |
|:------:|------|---------|-----------|--------|------|
| 🟢 | Auto table creation from Avro SR schema | v4 | v4-only; v3 blocked by SR classloader. Asserts table schema and row counts only. | Avro SR | `test_auto_table_creation.py` |
| 🟢 | Auto table creation with topic2table mapping | v4 | v4-only; v3 blocked by SR classloader. Asserts table schema and row counts only. | Avro SR | `test_auto_table_creation_topic2table.py` |

#### 3.1.6 High-Throughput Mode Ingestion

`snowflake.validation=server_side`.

| Status | Test | Version | Format | Notes |
|:------:|------|---------|--------|-------|
| 🔴 | Valid JSON records land correctly; verify toggle default is `server_side` when config omitted | v4 | JSON | **P0.** FR3 -- verify data arrives without client-side RowValidator + default toggle behavior |
| 🔴 | Valid Avro SR records land correctly | v4 | Avro SR | **P1.** FR3 |

#### 3.1.7 Iceberg Tables

> **V3 scope note**: V3 (3.2.x) has iceberg support via `snowflake.streaming.iceberg.enabled=true`
> but it was experimental and used custom connector-side code (`IcebergInitService`,
> `IcebergTableStreamingRecordMapper`, `IcebergSchemaEvolutionService`) that was removed in v4.
> V4 delegates iceberg entirely to SSv2 which handles it transparently.  The `v4_config_to_v3`
> migration does not inject `snowflake.streaming.iceberg.enabled=true`, so running these tests
> against v3 would silently write to regular (non-iceberg) tables rather than fail loudly.
> All iceberg tests are therefore v4-only.
>
> **External volume prerequisite**: tests require an AWS external volume named
> `kafka_push_e2e_volume_aws` (override with env var `ICEBERG_EXTERNAL_VOLUME`).

| Status | Test | Version | Rationale | Format | Cloud | File |
|:------:|------|---------|-----------|--------|-------|------|
| 🟢 | Iceberg JSON ingestion (2x2: validation x schematization) | v4 | schema=off: VARIANT bag-of-bits; schema=on: mixed VARIANT+typed table (BIGINT/DOUBLE/TEXT pre-declared, no SE needed); all 4 combos pass | JSON | AWS | `iceberg/test_iceberg_json.py::test_iceberg_json_ingestion` |
| 🟢 | Iceberg Avro ingestion (2x2: validation x schematization) | v4 | Same matrix as JSON but with Avro SR + AvroConverter; verifies typed columns and RECORD_METADATA | Avro SR | AWS | `iceberg/test_iceberg_avro.py::test_iceberg_avro_ingestion` |
| 🟢 | Iceberg SE JSON — add column (client-side) | v4 | Connector issues `ALTER ICEBERG TABLE ADD COLUMN` when RowValidator detects new columns; table starts with CITY+RECORD_METADATA, wave 1 adds AGE, wave 2 adds COUNTRY | JSON | AWS | `iceberg/test_iceberg_se_json.py::test_iceberg_se_add_column` |
| 🟢 | Iceberg SE JSON — multi-wave (client-side) | v4 | Three-wave evolution: city-only → city+age → city+age+country; verifies NULL backfill for pre-existing rows | JSON | AWS | `iceberg/test_iceberg_se_json.py::test_iceberg_se_multi_wave` |
| 🟡 | Iceberg SE JSON — server-side (xfail -- known limitation) | v4 | `ENABLE_SCHEMA_EVOLUTION=TRUE` + `validation=server_side` (HT mode) silently discards typed column additions on ICEBERG_VERSION=3 tables; client-side SE via `ALTER ICEBERG TABLE ADD COLUMN` works correctly; remove xfail once Snowflake server-side SE supports typed columns on iceberg | JSON | AWS | `iceberg/test_iceberg_se_json.py::test_iceberg_se_json_server_side` |
| 🟢 | Iceberg SE Avro — add column (client-side) | v4 | Same as JSON SE but with Avro SR; verifies column additions from evolving Avro schemas | Avro SR | AWS | `iceberg/test_iceberg_se_avro.py::test_iceberg_se_avro_add_column` |

#### 3.1.8 Pre-Flight Check (FR11)

| Status | Test | Scenario | Notes |
|:------:|------|----------|-------|
| 🔴 | Pre-flight: Error Table presence check (positive + negative) | validation=server_side with and without Error Table configured | **P0.** FR11. Parametrize as two scenarios in one test: startup fails/warns without Error Table, succeeds with it. |

#### 3.1.9 Case Sensitivity

| Status | Test | Version | Rationale | File |
|:------:|------|---------|-----------|------|
| 🟢 | Case-sensitive table name handling | dual | Verifies table name case sensitivity across v3/v4 | `compatibility/test_compatibility_case_sensitivity.py::test_compatibility_case_sensitivity_table_name` |
| 🟢 | Case-sensitive ingestion column names | dual | Verifies column name case handling across v3/v4 | `compatibility/test_compatibility_case_sensitivity.py::test_compatibility_case_sensitivity_ingestion_columns` |
| 🟢 | Case sensitivity in schema evolution | dual | Verifies SE handles case-sensitive column names | `compatibility/test_compatibility_case_sensitivity.py::test_case_sensitivity_schema_evolution` |

#### 3.1.10 Migration

| Status | Test | Version | Rationale | File |
|:------:|------|---------|-----------|------|
| 🟢 | v3→v4 migration without duplicates | dual | Verifies seamless migration path | `compatibility/test_migration.py::test_migration_without_duplicates` |
| 🟢 | v3→v4 migration with possible duplicates | dual | Verifies migration handles at-least-once delivery | `compatibility/test_migration.py::test_migration_with_possible_duplicates` |

---

### 3.2 Error Handling

Error handling is the highest-risk area for v3/v4 parity. In v3, SSv1 always validates and errors are deterministic. In v4, the `RowValidator` (copied from SSv1's `DataValidationUtil`) is a separate layer that can be toggled. **All compatibility-mode error handling tests must be dual** because they directly exercise client-side validation.

#### 3.2.1 Dead Letter Queue -- `errors.tolerance=all` (FR1, Compatibility Mode)

| Status | Test | Version | Format | Error Type | File |
|:------:|------|---------|--------|-----------|------|
| 🟢 | Invalid JSON -> DLQ | v4 | JSON | Deserialization | `test_snowpipe_streaming_string_json_dlq.py` -- v4-only; dual conversion pending |
| 🔴 | Schema mapping error -> DLQ | v4 | JSON | Type mismatch | `test_snowpipe_streaming_schema_mapping_dlq.py` -- `@pytest.mark.skip`; broken, not divergent |
| 🔴 | DLQ Kafka headers preserved (v3/v4 byte-for-byte comparison) | dual | JSON | Any | **P0.** FR9: DLQ error messages must be identical between v3 and v4 |
| 🔴 | DLQ with Avro data | dual | Avro SR | Deserialization | **P2.** FR1 -- v3 parity blocked by SR classloader. DLQ routing is format-independent (KC framework level); format-specific differences unlikely. |
| 🔴 | DLQ with Protobuf data | dual | Protobuf SR | Deserialization | **P2.** FR1 -- same reasoning as Avro DLQ. |
| 🔴 | DLQ with multi-partition topics | dual | JSON | Mixed | **P1.** FR1 -- only one test (`test_snowpipe_streaming_string_json_ignore_tombstone.py`) currently exercises multi-partition. |

#### 3.2.2 Abort -- `errors.tolerance=none` (FR2, Compatibility Mode)

| Status | Test | Version | Format | Error Type | Notes |
|:------:|------|---------|--------|-----------|-------|
| 🔴 | Deserialization error -> task FAILED | dual | JSON | Bad payload | **P1.** FR2. Verify v4 aborts identically to v3 on bad payload. Abort mechanism already verified by `ingest_one_type_abort` fixture; gap is v3/v4 parity for this error type. |
| 🔴 | Schema mismatch -> task FAILED | dual | JSON | Type mismatch | **P1.** FR2. Verify v4 aborts identically to v3 on type mismatch. Same — mechanism works, parity not yet verified. |

#### 3.2.3 Error Table Routing (High-Throughput Mode)

When `snowflake.validation=server_side`, invalid records route to SSv2 Error Tables instead of DLQ.

| Status | Test | Version | Format | Notes |
|:------:|------|---------|--------|-------|
| 🔴 | Invalid records -> SSv2 Error Table (not DLQ) | v4 | JSON | **P0.** FR3. Core Error Table verification. |
| 🔴 | Error Table + schema mismatch (extra/missing columns) | v4 | JSON | **P0.** FR3. |
| 🔴 | Compat routes to DLQ while HT routes to Error Table (same bad record, both modes) | v4 | JSON | **P0.** FR3. |
| 🔴 | SE disabled + validation off: rejected records land in Error Table | v4 | JSON | **P0.** FR3. config_variants returns early with no assertions for this combo. |

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
| 🟢 | Config matrix (8 combos: `evo x schematization x validation`) | dual | Core validation/SE interaction test. Has internal `pytest.skip()` for certain v3/v4 combos. `evo=True, schema=False` combos skipped with TODO. | JSON | `test_schema_evolution_streaming.py::test_schema_evolution_config_variants` |
| 🟢 | Avro SR with 2 topics, different schemas | v4 | v3 can't auto-create tables for Avro SR with topic2table.map; pre-created tables cause pipe invalidation on ALTER TABLE | Avro SR | `schema_evolution/test_se_avro_sr.py` |
| 🟢 | Auto table creation + SE (JSON, 2 topics) | dual | SE + auto-create triggers via structural error | JSON | `schema_evolution/test_se_auto_table_creation_json.py` |
| 🟢 | Auto table creation + SE (Avro SR, 2 topics) | v4 | Auto table creation is v4-only; v3 requires pre-existing tables | Avro SR | `schema_evolution/test_se_auto_table_creation_avro_sr.py` |
| 🟢 | Non-nullable columns + SE | dual | SE triggers via null-in-NOT-NULL path | JSON | `schema_evolution/test_se_nonnullable_json.py` |
| 🟢 | Tombstone handling during SE | dual | Asserts data values with SE | JSON | `schema_evolution/test_se_json_ignore_tombstone.py` |
| 🟢 | Random batch sizes (flush timing) | dual | Tests timing-sensitive SE path | JSON | `schema_evolution/test_se_random_row_count.py` |
| 🟢 | Nullable values after SMT + SE | dual | SE structural error path | JSON + SMT | `schema_evolution/test_se_nullable_values_after_smt.py` |

#### 3.3.2 Server-Side Schema Evolution (High-Throughput Mode)

When `snowflake.validation=server_side`, the connector does not perform client-side validation or DDL. Records go directly to the SSv2 SDK's `channel.appendRow()`. Schema evolution depends entirely on the Snowflake table property `ENABLE_SCHEMA_EVOLUTION = TRUE`.

Note: The connector source has no `MATCH_BY_COLUMN_NAME` or FDN-specific logic. "Server-side SE" means the Snowflake service handles schema mismatches for tables with `ENABLE_SCHEMA_EVOLUTION = TRUE`.

The `test_schema_evolution_config_variants` test already covers `evo=True, schema=True, valid=False` for v4 (server-side SE with schematization on). However, important gaps remain:

| Status | Test | Version | Format | Notes | Suggested File |
|:------:|------|---------|--------|-------|----------------|
| 🟢 | Server-side SE: new columns added (validation off, SE on) | v4 | JSON | Minimal coverage; config_variants covers this combo | `test_schema_evolution_ht.py` |
| 🔴 | Server-side SE: NOT NULL dropped + schematization off (parametrized) | v4 | JSON | **P1.** Two scenarios in one parametrized test: NOT NULL drop and schematization=off. config_variants skips the latter (TODO). | `test_schema_evolution_ht.py` |
| 🔴 | Server-side SE with Avro SR | v4 | Avro SR | **P2.** FR6. Avro provides explicit schema; server-side SE may behave differently than JSON. | `test_schema_evolution_ht.py` |
| 🔴 | Concurrent SE from multiple partitions | v4 | JSON | **P1.** Race condition in ALTER TABLE from multiple tasks. Cannot be caught by unit tests. | `test_schema_evolution_ht.py` |

---

### 3.4 RECORD_CONTENT Mode

`snowflake.enable.schematization=false` -- data lands in `RECORD_CONTENT` + `RECORD_METADATA` VARIANT columns (FR4).

`RECORD_CONTENT` is a VARIANT column. Validation mode (`snowflake.validation`) is irrelevant here — the entire payload goes into VARIANT with no type checking. The `snowflake.validation` config was removed from these test templates.

V3 parity was verified by running JSON, String, and ByteArray tests in dual mode (v3 + v4) on Confluent 7.8.0 (2026-03-31) — both versions produce identical results. Tests are now v4-only. Note that v3's own E2E tests (`SnowflakeSinkTaskForStreamingIT.java`) had **no RECORD_CONTENT value assertions** — they only checked row counts and RECORD_METADATA key presence (`offset`, `partition`). Our v4 tests are net-new coverage: field-level content verification, base64 encoding for bytes, and double-encoding edge case handling.

| Status | Test | Version | Rationale | Format | File |
|:------:|------|---------|-----------|--------|------|
| 🟢 | RECORD_CONTENT JSON (StringConverter key, JsonConverter value) | v4 | v3 parity confirmed (dual run 2026-03-31). Assertions capture v3 reference behavior. | JSON (native) | `test_snowpipe_streaming_legacy_string_json.py` |
| 🟢 | RECORD_CONTENT StringConverter (raw string payload) | v4 | v3 parity confirmed (dual run 2026-03-31). Assertions capture v3 reference behavior. | String | `test_snowpipe_streaming_legacy_string_converter.py` |
| 🟢 | RECORD_CONTENT ByteArrayConverter (base64 payload) | v4 | v3 parity confirmed (dual run 2026-03-31). Assertions capture v3 reference behavior. | Bytes | `test_snowpipe_streaming_legacy_byte_array_converter.py` |
| 🟢 | RECORD_CONTENT + Avro SR | v4 | v4 confirmed 2026-03-31. v3 parity cannot be verified: v3's bundled SR classes clash with Confluent 7.8.0 platform SR classes (ServiceConfigurationError). Assertions reflect expected Avro deserialization behavior. | Avro SR | `test_snowpipe_streaming_legacy_avro_sr.py` |
| 🔴 | RECORD_CONTENT + SMT (nullable values, ExtractField) | v4 | **P2.** Data values in VARIANT + SMT interaction. v3 equivalent: `TestSnowpipeStreamingNullableValuesAfterSmt` | JSON + SMT | -- |

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

#### 3.5.2 CREATE OR REPLACE TABLE Recovery

`CREATE OR REPLACE TABLE` mid-stream permanently invalidates v4's streaming channels. In v3, the connector detects the channel invalidation and re-opens a new channel against the recreated table (which already exists from the DDL — the connector does not need to create it).

**v3 recovery flow** (branch `3.2.x`):

1. **Detection**: `SnowflakeSinkServiceV2.insert()` checks `isChannelClosed()` on every record ([`SnowflakeSinkServiceV2.java:349`](src/main/java/com/snowflake/kafka/connector/internal/streaming/SnowflakeSinkServiceV2.java)). `DirectTopicPartitionChannel.isChannelClosed()` delegates to the SSv1 SDK's `channel.isClosed()` ([`DirectTopicPartitionChannel.java:951`](src/main/java/com/snowflake/kafka/connector/internal/streaming/DirectTopicPartitionChannel.java)). SSv1 detects the pipe invalidation caused by the table replacement and returns `true`.
2. **Table check**: `insert()` calls `startPartition()` → `tableActionsOnStartPartition()` → `createTableIfNotExists()` ([`SnowflakeSinkServiceV2.java:676`](src/main/java/com/snowflake/kafka/connector/internal/streaming/SnowflakeSinkServiceV2.java)). Since the table already exists (it was recreated by the DDL), `conn.tableExist()` returns `true` and the method just verifies the RECORD_METADATA column is present (`conn.appendMetaColIfNotExist()`). No table creation happens.
3. **Channel re-open**: `createStreamingChannelForTopicPartition()` opens a **new** SDK channel against the recreated table ([`DirectTopicPartitionChannel.java:842`](src/main/java/com/snowflake/kafka/connector/internal/streaming/DirectTopicPartitionChannel.java)). The new channel has the recreated table's 1-column schema (only RECORD_METADATA). When records arrive, they trigger client-side SE again — columns are re-added, and ingestion resumes from the last committed offset.

**v4 failure mode** (confirmed via log analysis of `test_se_replace_table[v4]` on 2026-03-31):

Wave 1 succeeds normally — client-side SE adds columns, SDK `appendRow()` buffers 100 records, server commits them after ~20s flush latency. After the test verifies 100 rows and runs `CREATE OR REPLACE TABLE`, Wave 2 records are silently lost. The root cause is a **silent channel invalidation**:

1. `CREATE OR REPLACE TABLE` destroys the streaming pipe on the server. The SDK channel object is not notified.
2. Wave 2 records pass client-side validation (the `RowValidator` still has the 4-column schema from Wave 1 SE). `appendRow()` succeeds locally — it only buffers data.
3. When the SDK attempts to flush the buffer, the server-side pipe no longer exists. **The flush silently fails** — no `SFException` is thrown to the connector, no error is logged.
4. `getChannelStatus()` continues returning stale `statusCode=[SUCCESS]` with `rowsInsertedCount=[100]` (from Wave 1). The status never changes.
5. `isChannelClosed()` returns `false` — the SDK channel was never explicitly closed.
6. The v4 recovery trigger (`isChannelClosed() → startPartition() → createTableIfNotExists()`) never fires. The connector sits in PRECOMMIT loops with committed offset stuck at 100 until the test times out (~5 minutes).

The v4 connector has the same `isChannelClosed() → startPartition()` code path as v3 ([`SnowflakeSinkServiceV2.java:396-401`](src/main/java/com/snowflake/kafka/connector/internal/streaming/SnowflakeSinkServiceV2.java)), but it never triggers because the SSv2 SDK does not surface pipe invalidation through `isClosed()` or `appendRow()`. This is a gap in the SDK's error surfacing, not in the connector's recovery logic.

These tests currently exist as v3-only (`@pytest.mark.parametrize("connector_version", ["v3"])`). They must be converted to v4 with `xfail` to document the behavioral gap. The v3 parametrization should be removed — there is no reason to run v3-only tests inside the v4 connector repo.

| Status | Test | Version | Notes | File |
|:------:|------|---------|-------|------|
| 🔴 | Table replacement recovery (single topic, SE re-evolve) | v4 (xfail) | **P1.** Convert from v3-only to v4 + `pytest.mark.xfail(reason="SSv2 SDK silent channel invalidation after CREATE OR REPLACE TABLE")`. Sends records -> evolves schema -> CREATE OR REPLACE TABLE -> sends again. V4 expected to fail at wave 2. | `schema_evolution/test_se_replace_table.py` |
| 🔴 | Table replacement recovery (multi-topic, SE re-evolve) | v4 (xfail) | **P1.** Same conversion. Two topics with different schemas feed one table; CREATE OR REPLACE TABLE should not permanently break ingestion. | `schema_evolution/test_se_multi_topic_replace_table.py` |

#### 3.5.3 Fault Injection & Recovery (missing)

These tests should use continuous ingestion (background producer) to exercise mid-flight fault handling.

| Status | Test | Fault Type | Version | Notes |
|:------:|------|-----------|---------|-------|
| 🔴 | Channel invalidation recovery | Server-side channel drop; client must detect and re-open | v4 | **P2.** Verify no data loss after channel re-open under continuous load. Requires SSv2 server-side channel drop simulation -- hard to reproduce in Docker. |
| 🔴 | Transient server errors (5xx + 429) | Simulated 5xx errors and 429 throttling during ingestion | v4 | **P2.** Verify backoff/retry and eventual recovery. PR #1386 implemented offset-based backoff for 429; unit tests cover retry logic. E2E requires mock proxy. |
| 🔴 | Network partition tolerance | Temporary connectivity loss between KC worker and Snowflake | v4 | **P2.** Verify connector recovers after partition heals, no duplicate/lost records. Requires Docker network manipulation during continuous ingestion -- hard to make deterministic in CI. |

---

### 3.6 Default Pipe Features

FR5 (Default Pipes only) + FR7 (Default Pipe Improvements). Must be tested in both compatibility and high-throughput modes.
| Status | Test | Feature | Mode | Version | Suggested File |
|:------:|------|---------|------|---------|----------------|
| 🔴 | Auto-Increment (Identity) columns | FR7 | Both (parametrized) | v4 | **P0.** `test_default_pipe_features.py`. FR7 PRD requirement. Must verify SSv2 SDK respects IDENTITY columns in both compat and HT modes. |
| 🔴 | Default timestamp properties | FR7 | Both (parametrized) | v4 | **P0.** `test_default_pipe_features.py`. FR7 PRD requirement. Must verify default timestamp handling in both modes. |
| 🔴 | Pre-clustered tables | FR7 | Both (parametrized) | v4 | **P0.** `test_default_pipe_features.py`. FR7 PRD requirement. Must verify ingestion into pre-clustered tables in both modes. |

---

### 3.7 Load & Stress

> **Scope**: These are CI-level smoke/pressure tests that run in pre-commit. They verify the connector handles moderate load without failures but are not intended to represent production-scale benchmarking. Dedicated load and benchmarking tests exist separately for validating throughput at scale (e.g., 10 GB/s target for high-throughput mode).

| Status | Test | Scale | Version | File |
|:------:|------|-------|---------|------|
| 🟢 | Pressure: 200 topics x 12 partitions x 10K records (24M total) | High | v4 | `pressure/test_pressure_init.py` |
| 🟢 | Pressure + Restart: 10 topics x 3 partitions x 200K records with chaos ops | High | v4 | `pressure/test_pressure_restart.py` |

---

## 4. Data Type Compatibility

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

### Per-Type E2E Coverage

All new E2E type tests go into **`test_type_compatibility.py`** (JSON format, dual mode) and **`test_type_compatibility_avro.py`** (Avro SR format, dual but v3 blocked). Each test covers both **positive** (valid values land correctly) and **negative** (invalid values routed to DLQ) cases for that type.
| Status | Snowflake Type | Test Functions | v3 | v4-compat | v4-ht | Notes |
|:------:|----------------|----------------|:--:|:---------:|:-----:|-------|
| 🟢 | NUMBER | `test_number` | Pass | Pass | Pass | Integers, zero, negative, max/min INT, invalid strings/objects. All modes identical. |
| 🟢 | NUMBER(p,s) | `test_number_with_scale` | Pass | Pass | Pass | Decimals, negative, zero, max scale. Invalid text -> DLQ. All modes identical. |
| 🟢 | FLOAT | `test_float` | Pass | Pass | Pass | pi, negative, zero, scientific notation. Invalid text/array -> DLQ. |
| 🟢 | FLOAT (special) | `test_float_special` | Pass | Pass | Pass | NaN, +Infinity, -Infinity as string representations. All modes identical. |
| 🟢 | VARCHAR | `test_varchar` | Pass | Pass | Pass | Normal strings, special chars, 1000-char string. All modes identical. |
| 🟢 | VARCHAR(10) | `test_varchar_length_limit` | Pass | Pass | Pass | At-limit and over-limit strings. Over-limit -> DLQ in all modes. |
| 🟡 | BINARY | `test_binary` | Pass | **Diverges** | **Diverges** | **SNOW-3256183.** SSv1 auto-detects hex encoding (e.g. "48656C6C6F" -> binary). SSv2 expects base64 only — hex strings are either rejected or base64-decoded producing garbled bytes. |
| 🟢 | BOOLEAN | `test_boolean` | Pass | Pass | Pass | true/false literals, invalid objects/arrays -> DLQ. All modes identical. |
| 🟡 | BOOLEAN (coercion) | `test_boolean_coercion` | Pass | **Diverges** | **Diverges** | SSv1 coerces `0`->false, `1`->true. v4 `RowValidator` rejects numeric values for BOOLEAN — only literal `true`/`false` and string tokens accepted. Rejected values silently dropped (not DLQ'd). |
| 🟢 | DATE | `test_date` | Pass | Pass | Pass | ISO dates, epoch, future. Invalid string -> DLQ. All modes identical. |
| 🟢 | TIME | `test_time` | Pass | Pass | Pass | Normal, midnight, end-of-day. Invalid string -> DLQ. All modes identical. |
| 🟢 | TIMESTAMP_NTZ | `test_timestamp_ntz` | Pass | Pass | Pass | ISO timestamps. Invalid string -> DLQ. All modes identical. |
| 🟡 | TIMESTAMP_NTZ (epoch) | `test_timestamp_ntz_epoch` | Pass | Pass | **Diverges** | v4-compat: fixed in PR #1393. v4-ht: server-side applies session timezone to unqualified integer epochs, shifting the stored value. Not fixable in client validator. |
| 🟢 | TIMESTAMP_LTZ | `test_timestamp_ltz` | Pass | Pass | Pass | TZ-aware timestamps, epoch. Invalid -> DLQ. All modes identical. |
| 🟢 | TIMESTAMP_TZ | `test_timestamp_tz` | Pass | Pass | Pass | Timestamps with offset. Invalid -> DLQ. All modes identical. |
| 🟢 | VARIANT | `test_variant` | Pass | Pass | Pass | Objects, arrays, nested, integers, floats, booleans, JSON strings. All modes identical. |
| 🟡 | VARIANT (bare str) | `test_variant_bare_string` | Pass | Pass | **Diverges** | `"hello"` is not valid JSON. v3/v4-compat reject -> DLQ. v4-ht server-side accepts bare scalars and wraps as JSON string. |
| 🟢 | OBJECT | `test_object` | Pass | Pass | Pass | Simple, nested, with arrays, from JSON string. All modes identical. |
| 🟢 | ARRAY | `test_array` | Pass | Pass | Pass | Strings, numbers, objects. All modes identical. |
| 🟡 | ARRAY (JSON str) | `test_array_json_string` | Pass | **Diverges** | **Diverges** | SSv1 parses JSON string `"[1,2,3]"` into native array `[1,2,3]`. SSv2 stores the string literal, producing single-element array `["[1,2,3]"]`. |
| 🟢 | NULL (11 types) | `test_null[COL_*]` | Pass | Pass | Pass | NULL in NUMBER, FLOAT, VARCHAR, BOOLEAN, DATE, TIME, TS_NTZ, TS_LTZ, TS_TZ, OBJECT, ARRAY -- SQL NULL in all modes. |
| 🟡 | NULL (VARIANT) | `test_null[COL_VARIANT]` | Pass | **Diverges** | **Diverges** | SSv1 stores SQL NULL. SSv2 serializes JSON null as the text literal `'null'` instead of SQL NULL. |
| 🟡 | Cross-type mismatch | `test_cross_type_mismatch` | Pass | **Diverges** | **Diverges** | Multiple divergences: v4-compat is stricter (rejects object->VARCHAR via DLQ, silently drops numeric->BOOLEAN). v3 coerces both. v4-ht drops everything without DLQ (no client validation). |
| 🟢 | GEOGRAPHY | `test_dt_geography` | Pass | Pass | Pass | Rejected in all modes (Snowpipe Streaming limitation). Correct error message confirmed. File: `compatibility/test_unsupported_types.py` |
| 🟢 | GEOMETRY | `test_dt_geometry` | Pass | Pass | Pass | Rejected in all modes. Correct error message confirmed. File: `compatibility/test_unsupported_types.py` |
| 🟢 | VECTOR | `test_dt_vector` | Error asserted | Pass | Pass | v3 rejects VECTOR (channel open error, asserted). v4 ingests VECTOR(FLOAT,3) correctly. File: `compatibility/test_unsupported_types.py` |
| 🟡 | Structured OBJECT | `test_dt_structured_object` | **Diverges** | Pass | Pass | SSv1 rejects structured types at channel open. SSv2 accepts them — SDK supports typed OBJECT/ARRAY for non-Iceberg tables. New v4 capability, not a regression. File: `compatibility/test_unsupported_types.py` |
| 🟡 | Structured ARRAY | `test_dt_structured_array` | **Diverges** | Pass | Pass | SSv1 rejects structured types at channel open. SSv2 accepts them — SDK supports typed OBJECT/ARRAY for non-Iceberg tables. New v4 capability, not a regression. File: `compatibility/test_unsupported_types.py` |
| 🔴 | Collated VARCHAR | -- | -- | -- | -- | **P2.** Not yet tested. `RowValidatorTest.java` covers unit level. |

### Avro-Specific Type Mapping (`test_type_compatibility_avro.py`)

Avro has its own type system. These tests are dual but v3 is blocked by the classloader conflict. **File does not exist yet -- must be created.**

| Status | Avro Types | SF Target Types | What to Test | Notes |
|:------:|-----------|-----------------|-------------|-------|
| 🔴 | int, long, float, double, bytes (decimal) | NUMBER, BIGINT, FLOAT, DOUBLE | Positive: each Avro numeric -> correct SF type. Negative: decimal overflow. v3 parity blocked by SR classloader. File does not exist yet. | SDK ref: `NumericTypesIT.java` |
| 🔴 | date, time-millis, time-micros, timestamp-millis, timestamp-micros | DATE, TIME, TIMESTAMP_NTZ/LTZ | Positive: each Avro logical type -> correct SF type. Negative: out-of-range. v3 parity blocked by SR classloader. File does not exist yet. | SDK ref: `DateTimeIT.java` |
| 🔴 | string, bytes, boolean, enum | VARCHAR, BINARY, BOOLEAN, VARCHAR | Positive: each primitive -> correct SF type. Negative: size overflow. v3 parity blocked by SR classloader. File does not exist yet. | SDK ref: `StringsIT.java`, `BinaryIT.java` |
| 🔴 | record, array, map, union | VARIANT, ARRAY, OBJECT | Positive: complex Avro types -> SF semi-structured. Negative: size overflow. v3 parity blocked by SR classloader. File does not exist yet. | SDK ref: `SemiStructuredIT.java` |

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
| 28 | 🟢 | Iceberg JSON (AWS) -- 2x2 matrix; schema=on uses pre-declared mixed VARIANT+typed table | General | Iceberg | Done |
| 29 | 🟢 | Iceberg Avro (AWS) -- 2x2 matrix with Avro SR + AvroConverter | General | Iceberg | Done |
| 30 | 🟢 | Iceberg SE JSON add-column (AWS) -- client-side SE via `ALTER ICEBERG TABLE ADD COLUMN` | FR6 | Iceberg | Done |
| 30b | 🟢 | Iceberg SE JSON multi-wave (AWS) -- three-wave client-side SE | FR6 | Iceberg | Done |
| 30c | 🟡 | Iceberg SE JSON server-side (AWS) -- xfail; server-side SE on iceberg silently drops typed columns | FR6 | Iceberg | Done |
| 31 | 🟢 | Iceberg SE Avro add-column (AWS) -- client-side SE with evolving Avro schemas | FR6 | Iceberg | Done |
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
