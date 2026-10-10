# KC v4 SPCS ingestion test

**Author:** Berni Schiefer (openai-gpt-6-astra, Cortex Code agent mode)
**Date:** 2026-10-10 22:14 UTC
**Status:** Draft; test and helpers only, execution harness in a dependent PR
**Document version:** 6.0

## Scope

This change adds one Kafka Connector (KC) v4 ingestion test for Snowpark
Container Services (SPCS). It reuses the existing end-to-end producer,
connector lifecycle, and row-polling helpers, sends 100 numbered records,
and verifies exactly the expected values through a runtime-authenticated SQL
connection. Ordinary test lanes exclude the `spcs` marker unless `--spcs`
is supplied explicitly.

The shared driver accepts an optional observer connection. Its default
private-key connection behavior is unchanged. The SPCS fixture uses the
runtime token and does not supply host credentials to the connector.

## Running the test

Run from the `test/` directory inside an already prepared SPCS job with Kafka,
distributed Connect, the KC v4 artifact, and the usual Python test dependencies:

```bash
python3 -m pytest tests/spcs --spcs \
  --kafka-address localhost:9092 --kafka-connect-address localhost:8083 \
  --platform apache --platform-version 4.1.1 -v
```

The fixture requires `SNOWFLAKE_RUNNING_INSIDE_SPCS=true`, `SNOWFLAKE_HOST`,
`SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_DATABASE`, `SNOWFLAKE_SCHEMA`,
`SPCS_QUERY_WAREHOUSE`, and the runtime token at `/snowflake/session/token`.
The caller must provide an isolated schema and usable warehouse, own resource
cleanup, and configure the job for streaming authentication. The test rejects
execution outside SPCS. Setting environment variables alone is not a valid
live qualification.

The host runner, container recipe, job lifecycle, independent host verification,
and manual workflow are deliberately separated into the dependent
`bschiefer/SNOW-4202412-spcs-opt-in-harness` PR. They are not included here.
No release gate or publishing enforcement is activated by either change.

## Offline regression checks

From the repository root, install the same dependencies as the `spcs-contracts`
workflow job, then run:

```bash
python3 test/spcs/observer_contract.py
```

The three checks cover default private-key connection construction, an injected
observer that never loads a private key, and ambient-role configuration without
supplied connector credentials. They require no live account. CI invokes this
file explicitly; default unittest discovery does not select its filename.

## Qualification limits and review order

The combined implementation at `ba367d6` passed 34 runner contracts and these
three observer contracts in CI. Earlier live runs on personal AWS, Azure, and
GCP fixtures verified 100 records; final recovery changes were requalified on
Azure only. Those runs used the separate harness and do not establish live
qualification of this test-only PR or a team-owned release gate.

Review and merge #1585 first, then review the dependent harness PR. Fixture
ownership, live GitHub Actions qualification, and publishing enforcement remain
separate follow-up work. No connector production code or account policies are
changed here.
