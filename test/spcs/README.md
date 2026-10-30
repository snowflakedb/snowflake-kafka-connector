# KC v4 SPCS smoke test

**Author:** Berni Schiefer (openai-gpt-6-astra, Cortex Code agent mode)
**Date:** 2026-10-16 07:20 UTC
**Status:** Draft; manual execution only, no release gate enabled
**Document version:** 7.2

A finite Snowpark Container Services (SPCS) job runs Kafka, Connect, and the
KC v4 test. It sends 100 numbered records using ambient authentication. The
host independently checks every value, then removes the job and test data.

## Related changes

1. [#1585](https://github.com/snowflakedb/snowflake-kafka-connector/pull/1585): merged ingestion test and observer helpers.
2. [#1586](https://github.com/snowflakedb/snowflake-kafka-connector/pull/1586): merged runner, container, cleanup, and manual CI job.
3. [#1587](https://github.com/snowflakedb/snowflake-kafka-connector/pull/1587): separate functional CI pacing and opt-in v4 trial.

Continuous monitoring is separate: [SNOW-4202413](https://snowflakecomputing.atlassian.net/browse/SNOW-4202413),
[snowprober #7096](https://github.com/snowflake-eng/snowprober/pull/7096), and
[metrics configuration #167951](https://github.com/snowflake-eng/instance-metrics-config/pull/167951).
Neither smoke-test PR enables those probers or blocks release publishing.

## One-time setup: enable SPCS provisioning in CI

[`setup_ci.py`](setup_ci.py) creates four permanent objects, once, in the
existing CI account `SFCTEST0` as `TESTROLE_KAFKA`. Later CI runs reuse them.

| Object | Purpose |
|---|---|
| Compute pool `KC_SPCS_CI_POOL` | `GEN_X64_G2_4`, at most one node, starts suspended, auto-suspends after 300 s |
| Schema `TESTDB_KAFKA.KC_SPCS_CI` | Holds the two objects below |
| Stage `...KC_SPCS_CI.HARNESS_STAGE` | Test payloads |
| Image repository `...KC_SPCS_CI.IMAGES` | Test container images |

The script never alters, drops, or grants anything, and rerunning it is safe.

1. **Grant privileges** (Snowflake `ACCOUNTADMIN` in `SFCTEST0`):

   ```sql
   GRANT CREATE COMPUTE POOL ON ACCOUNT TO ROLE TESTROLE_KAFKA;
   GRANT USAGE, CREATE SCHEMA ON DATABASE TESTDB_KAFKA TO ROLE TESTROLE_KAFKA;
   ```

   Owning the new schema lets the role create the stage and repository.
2. **Protect the workflow** (GitHub repository admin): create environment
   `kc-spcs-setup` with required reviewers, self-review prevented, and
   deployments limited to branch `master`. Then set environment variable
   `KC_SPCS_SETUP_ENABLED=true`.
3. **Run** **Set up permanent KC SPCS infrastructure** from `master`: first
   `check` (read-only; lists missing objects), then `apply`, then `check` again.
   It should report all four objects present.
4. **Optionally revoke** `CREATE COMPUTE POOL` and `CREATE SCHEMA` if nothing
   else needs them. Keep database `USAGE`; the role keeps object ownership.

Offline test: `python3 test/spcs/test_setup_ci.py -v`.

## Fixture and execution

Provide an existing account, database, internal stage, warehouse, named CLI
connection, and digest-pinned image. The service-owner role must be able to use
these without secondary roles. Reserve an unused, suspended, auto-resuming
compute pool with room for a 6 GiB job through cleanup. `--exclusive-pool` is an
operator acknowledgment, not a lock. Concurrent pool use is unsafe.

Build the connector normally. From `test/`, build the image:

```bash
podman build --platform linux/amd64 -f docker/Dockerfile.apache-kafka \
  --build-arg KAFKA_VERSION=4.1.1 --build-arg SCALA_VERSION=2.13 \
  --build-arg JAVA_VERSION=17 -t localhost/kc-spcs-apache:4.1.1-java17 .
podman build --platform linux/amd64 -f spcs/Dockerfile \
  --build-arg KAFKA_IMAGE=localhost/kc-spcs-apache:4.1.1-java17 \
  -t localhost/kc-spcs-smoke:1 .
```

Publish to an existing Snowflake image repository using the approved registry
login. With Podman, pass the complete CLI JSON credential response as the
`0sessiontoken` password, not just its token field. Never store credentials in
the image or stage. Use the published digest below.

From the repository root, create `test/spcs/.scratch/`, then run:

```bash
python3 test/spcs/run_spcs_release.py \
  --connection MY_TEST_CONNECTION --expected-account MY_ACCOUNT_LOCATOR \
  --role MY_SERVICE_OWNER_ROLE --database MY_TEST_DB \
  --pool MY_UNUSED_POOL --stage MY_TEST_DB.MY_SCHEMA.HARNESS_STAGE \
  --warehouse MY_TEST_WH --exclusive-pool --cleanup-policy warn \
  --image /my_test_db/my_schema/my_repo/kc-spcs-smoke@sha256:<digest> \
  --jar target/snowflake-kafka-connector-4.2.1.jar \
  --evidence test/spcs/.scratch/result.json
```

Supply `--egress MY_EXISTING_INTEGRATION` if networking requires it. Use a fresh
evidence filename per run. The runner creates a unique schema and an empty
`SMOKE_ROWS` table with `RECORD_METADATA VARIANT` and quoted `"number" VARCHAR`.
The test checks table access and emptiness before starting a connector.
Host credentials stay outside the container; the test uses the runtime token.
The runner does not provision accounts, grant access, or change policies.

## Result and recovery

Pass requires job status DONE, one successful pytest completion marker, and
exactly the expected 100 values. Timeout, interruption, and missing evidence fail.
The container has a ten-minute deadline. Host polling defaults to fifteen minutes,
plus bounded diagnostic and cleanup calls.

Cleanup drops the job, schema, and payload, then suspends the empty pool and
checks resource removal. `--cleanup-policy warn` preserves an ingestion pass as
`PASS_WITH_CLEANUP_WARNING`; `fail` makes incomplete cleanup fail the run. Both
record `test_passed` and `cleanup_complete`. Warnings require operator follow-up.
SIGTERM and Ctrl-C trigger cleanup. Host loss or SIGKILL may require manual
recovery using the recorded resource names. Never suspend a pool with other work.

The result records run timestamps, execution stages, checksums, resource
names, and bounded diagnostic signals, including labeled request IDs and HTTP
status codes. This supports server-log lookup without uploading raw logs. The
container scans at most the last 2 MiB of each Kafka/Connect log; earlier context
can be lost. Raw log excerpts remain in a local 0600 file and are not retained
by CI. Log capture failure does not prevent cleanup.

## CI and verification

The manual job requires `run_spcs=true`, repository variable
`SPCS_RELEASE_ENABLED=true`, and the `spcs-release` environment. Configure:

- Secrets: `SPCS_RELEASE_ACCOUNT`, `SPCS_RELEASE_USER`, `SPCS_RELEASE_PRIVATE_KEY`.
- Variables: `SPCS_EXPECTED_ACCOUNT`, `SPCS_POOL`, `SPCS_STAGE`, `SPCS_DATABASE`,
  `SPCS_ROLE`, `SPCS_WAREHOUSE`, and `SPCS_RELEASE_IMAGE_DIGEST`.
- `SPCS_EXCLUSIVE_POOL=true` after reserving the pool; optional `SPCS_EGRESS`.
- `SPCS_CLEANUP_POLICY`: `warn` by default, or `fail`.

CI retains only `result.json` for 14 days, never the key or raw log. Offline checks:

```bash
python3 -m unittest discover -s test/spcs -v
python3 test/spcs/observer_contract.py
```

Personal-fixture runs on October 10 verified 100 rows on AWS, Azure, and GCP;
later recovery changes were requalified on Azure only. New fixture/diagnostic
changes have offline coverage but need live requalification. The manual GitHub
Actions job still needs qualification with a team-owned fixture. The six-case
trial in #1587 does not exercise SPCS. Token-rotation soak, policy-matrix tests,
continuous monitoring, and exact-release-artifact enforcement are not covered.
