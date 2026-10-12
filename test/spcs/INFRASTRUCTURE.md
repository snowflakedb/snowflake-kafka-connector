# Permanent Kafka Connector SPCS infrastructure

Document version: 1.0, 2026-10-12 10:03 UTC

This setup adds persistent Snowpark Container Services (SPCS) resources to the
existing AWS Kafka Connector CI account. It reuses the encrypted AWS profile,
`SNOWFLAKE_TEST_PROFILE_SECRET`, and `TESTROLE_KAFKA`; it creates no account,
user, credentials, or grants. The validation harness remains in PRs
[#1585](https://github.com/snowflakedb/snowflake-kafka-connector/pull/1585),
[#1586](https://github.com/snowflakedb/snowflake-kafka-connector/pull/1586), and
[#1587](https://github.com/snowflakedb/snowflake-kafka-connector/pull/1587).

## After approval and merge

A KC maintainer runs **Set up permanent KC SPCS infrastructure** on `master`,
with `confirm_setup=true`. Do not run provisioning from a draft branch. Pull
requests run only credential-free setup contracts. The master guard prevents
branch provisioning; it does not replace the repository's human review process.

The setup creates these resources once and verifies them on subsequent runs:

- Pool `KC_SPCS_CI_POOL`: one node maximum, initially suspended, auto-resume on,
  auto-suspend after 300 seconds. Selects the smallest available allowlisted x64
  family with at least 7 GiB for the workload and stage-volume sidecar.
- Schema `TESTDB_KAFKA.KC_SPCS_CI`.
- Stage `TESTDB_KAFKA.KC_SPCS_CI.HARNESS_STAGE`: permanent internal, SSE-encrypted.
- Image repository `TESTDB_KAFKA.KC_SPCS_CI.IMAGES`.

The existing CI role must be able to create a compute pool and a schema in the
KC database. Current pool-creation privileges remain unverified: the read-only
CI grant inspection timed out. If creation is denied, stop and have the existing
account administrator resolve that specific prerequisite. The script neither
elevates its role nor issues grants.

## Reruns and recovery

Existing objects must have the expected owner and purpose comment. Name
collisions, configuration drift, and an active pool stop setup without modifying
those objects. No object is replaced or dropped. DDL is not transactional:
completed objects remain after a later failure. Rerun the same workflow after
resolving the failure; it verifies existing objects and creates only missing ones.
A `create_submitted` status can mean a server-side creation completed even if its
response was lost. Do not delete resources to retry.

The workflow prints resource-step status and numeric error codes, not credentials
or raw error text. It removes the decrypted profile on exit and uploads no files.
Setup and the planned validation job share concurrency group
`spcs-release-account`; other tools are not covered by that lock. Keep this pool
exclusive to KC validation.

## What remains before validation

Setup does not publish an image, start a service, or enable a release gate. After
merge and successful setup, publish the harness image to the new repository,
record its digest, and qualify the manual GitHub validation job using the existing
CI profile. That credential wiring belongs with the harness, not this prerequisite
PR. Image publication and live qualification have not been performed here.

Validation cleanup must remove only that run's job, temporary schema, and stage
prefix, then suspend the empty pool. The pool, `KC_SPCS_CI` schema, stage, and image
repository persist between runs. They incur storage charges while holding data;
compute charges begin when the pool resumes.

Offline checks (no profile required):

```bash
python3 test/spcs/test_setup_ci.py -v
ruff check test/spcs/setup_ci.py test/spcs/test_setup_ci.py
ruff format --check test/spcs/setup_ci.py test/spcs/test_setup_ci.py
```
