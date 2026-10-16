# KC v4 SPCS ingestion test

**Author:** Berni Schiefer (openai-gpt-6-astra, Cortex Code agent mode)
**Date:** 2026-10-11 20:00 UTC
**Status:** Draft; no release gate enabled
**Document version:** 7.0

One Kafka Connector (KC) v4 test sends 100 numbered records from Snowpark
Container Services (SPCS) and checks every value through a runtime-authenticated
SQL connection. Ordinary test lanes exclude it unless `--spcs` is supplied.
The shared driver's default private-key authentication is unchanged.

## Review order

1. [#1585](https://github.com/snowflakedb/snowflake-kafka-connector/pull/1585): ingestion test and observer helpers.
2. [#1586](https://github.com/snowflakedb/snowflake-kafka-connector/pull/1586): optional container, host runner, cleanup, and manual workflow.
3. [#1587](https://github.com/snowflakedb/snowflake-kafka-connector/pull/1587): separate functional CI pacing and trial policy.

These are finite tests, not continuous monitoring. The separate prober work is
[SNOW-4202413](https://snowflakecomputing.atlassian.net/browse/SNOW-4202413).

## Run inside an existing SPCS job

Provide Kafka, distributed Connect, the KC v4 artifact, Python test dependencies,
an isolated schema, and a usable warehouse. In that schema, prepare an **empty**
`SMOKE_ROWS` table with `RECORD_METADATA VARIANT` and a quoted lowercase
`"number" VARCHAR` column. The test checks access and emptiness before creating
the connector. The caller owns cleanup; #1586 automates this setup and cleanup.

Required runtime values: `SNOWFLAKE_RUNNING_INSIDE_SPCS=true`, `SNOWFLAKE_HOST`,
`SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_DATABASE`, `SNOWFLAKE_SCHEMA`,
`SPCS_QUERY_WAREHOUSE`, and `/snowflake/session/token`. Configure the job for
SPCS streaming authentication; do not supply host credentials to the container.

From `test/` inside the job:

```bash
python3 -m pytest tests/spcs --spcs \
  --kafka-address localhost:9092 --kafka-connect-address localhost:8083 \
  --platform apache --platform-version 4.1.1 -v
```

## Verification

With the dependencies from the `spcs-contracts` workflow, run from the repo root:

```bash
python3 test/spcs/observer_contract.py
```

These offline checks cover default/injected connections, ambient configuration,
and missing or nonempty fixtures. They do not establish live SPCS qualification.
Earlier personal-fixture runs passed on AWS, Azure, and GCP; later recovery
changes were requalified on Azure only. The new fixture checks require live
requalification. Team-owned CI fixtures and release publishing enforcement remain
outside this PR.
