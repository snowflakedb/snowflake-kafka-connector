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
- [4. Data Type Compatibility](#4-data-type-compatibility-strategy)

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
| 🟢 | No Error Table configured -> startup warning logged, connector runs | validation=false, no Error Table | `test_error_table_without_error_logging` |
| 🟢 | Error Table configured -> startup succeeds, errors captured | validation=false, Error Table present | `test_error_table_with_error_logging` |

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
| 🟢 | Invalid records -> SSv2 Error Table (not DLQ) | v4 | JSON | `test_error_table_with_error_logging`, `test_error_table_accounting[v4-ht]` |
| 🟢 | Error Table + value validation (VARCHAR overflow) and schema mismatch (missing NOT NULL column) | v4 | JSON | `test_error_table_schema_mismatch` |
| 🟢 | Compat routes to DLQ while HT routes to Error Table (same bad record, both modes) | v4 | JSON | `test_error_table_vs_dlq_routing` |

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

`CREATE OR REPLACE TABLE` mid-stream was causing v4 to silently lose data. The fix adds a per-channel validity probe in `processChannelStatus()` that detects SDK-level invalidation and triggers channel recovery. Both v3 and v4 now pass.

| Status | Test | Version | Notes | File |
|:------:|------|---------|-------|------|
| :white_check_mark: | Table replacement recovery (single topic, SE re-evolve) | v3, v4 | Fixed via preCommit invalidation detection. | `schema_evolution/test_se_replace_table.py` |
| :white_check_mark: | Table replacement recovery (multi-topic, SE re-evolve) | v3, v4 | Same fix. | `schema_evolution/test_se_multi_topic_replace_table.py` |

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
| 🟢 | Auto-Increment (Identity) columns | FR7 | Compatibility | v4 | `test_default_pipe_features.py` |
| 🟢 | Auto-Increment (Identity) columns | FR7 | High-Throughput | v4 | `test_default_pipe_features.py` |
| 🟢 | Default timestamp properties | FR7 | Compatibility | v4 | `test_default_pipe_features.py` |
| 🟢 | Default timestamp properties | FR7 | High-Throughput | v4 | `test_default_pipe_features.py` |
| 🔴 | Pre-clustered tables | FR7 | Compatibility | v4 | `test_default_pipe_features.py` |
| 🔴 | Pre-clustered tables | FR7 | High-Throughput | v4 | `test_default_pipe_features.py` |

---

### 3.7 Load & Stress

> **Scope**: These are CI-level smoke/pressure tests that run in pre-commit. They verify the connector handles moderate load without failures but are not intended to represent production-scale benchmarking. Dedicated load and benchmarking tests exist separately for validating throughput at scale (e.g., 10 GB/s target for high-throughput mode).

| Status | Test | Scale | Version | File |
|:------:|------|-------|---------|------|
| 🟢 | Pressure: 200 topics x 12 partitions x 10K records (24M total) | High | v4 | `pressure/test_pressure_init.py` |
| 🟢 | Pressure + Restart: 10 topics x 3 partitions x 200K records with chaos ops | High | v4 | `pressure/test_pressure_restart.py` |

---

## 4. Data Type Compatibility

**Does v4 compatibility mode handle every Snowflake data type the same way v3 does?**

V4 client-side validation (`RowValidator` + `DataValidationUtil`, code copied from SSv1 SDK) runs before the SSv2 SDK. Server-side mode bypasses client validation entirely. Divergences occur when SSv2 handles a value differently than SSv1 did, and client-side normalization doesn't compensate.

Tests: `test_type_compatibility.py` (JSON, dual mode). Each test covers positive (valid values land correctly) and negative (invalid values routed to DLQ).

| Target Data Type | v3 | v4 Client | v4 Server | Notes |
|---|:---:|:---:|:---:|---|
| NUMBER | 🟢 | 🟢 | 🟢 | |
| FLOAT | 🟢 | 🟢 | 🟢 | |
| VARCHAR | 🟢 | 🟢 | 🟢 | |
| BINARY (hex String input) | 🟢 | 🟢 | 🟡 | Server-side may interpret hex as base64, producing incorrect bytes |
| BOOLEAN | 🟢 | 🟢 | 🟢 | |
| BOOLEAN (Integer 0/1 input) | 🟢 | 🟢 | 🟡 | Server-side rejects Integer boolean values; rows not ingested |
| DATE | 🟢 | 🟢 | 🟢 | |
| TIME | 🟢 | 🟢 | 🟢 | |
| TIMESTAMP_NTZ | 🟢 | 🟢 | 🟢 | |
| TIMESTAMP_NTZ (Integer epoch) | 🟢 | 🟢 | 🟡 | Server-side shifts stored value by default timezone offset (~8h) |
| TIMESTAMP_LTZ | 🟢 | 🟢 | 🟢 | |
| TIMESTAMP_TZ | 🟢 | 🟢 | 🟢 | |
| VARIANT | 🟢 | 🟢 | 🟢 | |
| VARIANT (JSON String input) | 🟢 | 🟢 | 🟢 | |
| VARIANT (bare String input) | 🟢 | 🟢 | 🟡 | Server-side accepts invalid JSON scalars; client-side correctly rejects to DLQ |
| OBJECT | 🟢 | 🟢 | 🟢 | |
| ARRAY | 🟢 | 🟢 | 🟢 | |
| ARRAY (JSON String input) | 🟢 | 🟢 | 🟡 | Server-side wraps string as single-element array instead of parsing |
| NULL | 🟢 | 🟢 | 🟢 | |
| NULL (VARIANT column) | 🟢 | 🟡 | 🟡 | Stored as text `'null'` instead of SQL NULL |
| Cross-type mismatch | 🟢 | 🟢 | 🟢 | |
| GEOGRAPHY, GEOMETRY | 🟢 | 🟢 | 🟢 | Unsupported in Streaming; rejected in all modes |
| VECTOR | 🟡 | 🟢 | 🟢 | New in v4. Not supported in v3. |
| Structured OBJECT/ARRAY | 🟡 | 🟢 | 🟢 | New in v4. Not supported in v3. |
| Collated VARCHAR | 🔴 | 🔴 | 🔴 | Not tested. Unit-level coverage only. |

### Avro-Specific Type Mapping

Tests: `test_type_compatibility_avro.py` (Avro SR, v4-compat + v4-ht). V3 parity testing is blocked by the SR classloader conflict.

Avro provides typed values (native int, float, boolean, bytes, logical types) unlike schemaless JSON. The AvroConverter produces Kafka Connect Structs with schemas, testing a different pipeline path than JSON.

| Avro Type | Target Column | v4 Client | v4 Server | Notes |
|---|:---:|:---:|:---:|---|
| `int` | NUMBER | 🟢 | 🟢 | 32-bit typed integer |
| `long` | NUMBER | 🟢 | 🟢 | 64-bit typed integer |
| `float` | FLOAT | 🟢 | 🟢 | 32-bit; incl. NaN, Inf, -Inf as native floats |
| `double` | FLOAT | 🟢 | 🟢 | 64-bit; incl. NaN, Inf, -Inf |
| `string` | VARCHAR | 🟢 | 🟢 | |
| `boolean` | BOOLEAN | 🟢 | 🟢 | Native bool (no 0/1 coercion path) |
| `bytes` | BINARY | 🟢 | 🟢 | Raw bytes; RowValidator unwraps ByteBuffer to byte[] |
| `date` logical | DATE | 🟢 | 🟢 | Days-from-epoch via Avro logical type |
| `timestamp-millis` logical | TIMESTAMP_NTZ | 🟢 | 🟢 | Millis-from-epoch via logical type |
| `array` | ARRAY | 🟢 | 🟢 | Native Avro array |
| `map` | VARIANT | 🟢 | 🟢 | Avro map → VARIANT |
| null unions | various | 🟢 | 🟢 | Nullable union handling |
| `bytes` → VARCHAR | divergence | 🟢 | 🟡 | v4-compat rejects byte[]; v4-ht coerces to base64 |
| `bytes` → NUMBER | error | 🟢 | 🟢 | Cross-type: byte[] rejected (Avro-specific) |
| `float` NaN/Inf → NUMBER | error | 🟢 | 🟢 | Cross-type: native float NaN (Avro-specific) |
| `map`/`array` → BOOLEAN | error | 🟢 | 🟢 | Cross-type: typed complex → primitive |
