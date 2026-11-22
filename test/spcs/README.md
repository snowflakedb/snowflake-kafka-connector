# KC v4 SPCS smoke test

**Author:** Berni Schiefer (claude-opus-5-5, Cortex Code agent mode)
**Date:** 2026-11-22 14:00 UTC
**Status:** Runs nightly and on demand; no release gate enabled
**Document version:** 8.1

A finite Snowpark Container Services (SPCS) job runs Kafka, Connect, and the
KC v4 test. It sends 100 numbered records using ambient authentication. The
host independently checks every value, then removes the job and test data.

## Related changes

1. [#1585](https://github.com/snowflakedb/snowflake-kafka-connector/pull/1585): merged ingestion test and observer helpers.
2. [#1586](https://github.com/snowflakedb/snowflake-kafka-connector/pull/1586): merged runner, container, cleanup, and manual CI job.
3. [#1587](https://github.com/snowflakedb/snowflake-kafka-connector/pull/1587): separate functional CI pacing and opt-in v4 trial.
4. [#1588](https://github.com/snowflakedb/snowflake-kafka-connector/pull/1588):
merged one-time setup of the permanent CI objects.
5. [#1626](https://github.com/snowflakedb/snowflake-kafka-connector/pull/1626)
and [#1628](https://github.com/snowflakedb/snowflake-kafka-connector/pull/1628):
the CI job uses the existing CI login and builds its own image.
6. [#1632](https://github.com/snowflakedb/snowflake-kafka-connector/pull/1632):
setup adds the pool's network rule to the user and account network policies.
7. This change: nightly run with a self-repairing setup step and Jira failure routing.

Continuous monitoring is separate: [SNOW-4202413](https://snowflakecomputing.atlassian.net/browse/SNOW-4202413),
[snowprober #7096](https://github.com/snowflake-eng/snowprober/pull/7096), and
[metrics configuration #167951](https://github.com/snowflake-eng/instance-metrics-config/pull/167951).
Neither smoke-test PR enables those probers or blocks release publishing.

## One-time setup: enable SPCS provisioning in CI

[`setup_ci.py`](setup_ci.py) creates five permanent objects, once, in the
existing CI account `SFCTEST0` as `TESTROLE_KAFKA`. Later CI runs reuse them.

| Object | Purpose |
|---|---|
| Compute pool `KC_SPCS_CI_POOL` | `GEN_X64_G2_4`, at most one node, starts suspended, auto-suspends after 300 s |
| Schema `TESTDB_KAFKA.KC_SPCS_CI` | Holds the two objects below |
| Stage `...KC_SPCS_CI.HARNESS_STAGE` | Test payloads |
| Image repository `...KC_SPCS_CI.IMAGES` | Test container images |
| Network rule `...KC_SPCS_CI.KC_SPCS_CI_POOL_INGRESS` | `TYPE=COMPUTE_POOL`, allows `KC_SPCS_CI_POOL` only |

Run **Set up permanent KC SPCS infrastructure** from `master` with `apply`. As
`ACCOUNTADMIN`, which the CI user holds in `SFCTEST0`, it grants:

```sql
GRANT CREATE COMPUTE POOL ON ACCOUNT TO ROLE TESTROLE_KAFKA;
GRANT USAGE, CREATE SCHEMA ON DATABASE TESTDB_KAFKA TO ROLE TESTROLE_KAFKA;
```

It then switches back to `TESTROLE_KAFKA`, creates what is missing, and lists
all five objects. Last, as `ACCOUNTADMIN`, it adds the network rule to the CI
user's network policy and the account's network policy, if not already there.
Without this, every call the connector makes from SPCS gets HTTP 401 ("Incoming
request with IP/Token is not allowed", error 390422): with
`enableCustomCredentials`, only `COMPUTE_POOL` rules can admit SPCS traffic, and
IPv4 rules such as `0.0.0.0/0` never match. In `SFCTEST0` the account policy is
the one applied. Adding the rule was authorized by Berni Schiefer on 2026-11-18. Using `ACCOUNTADMIN` for these two grants was authorized by
Berni Schiefer on 2026-11-14. If the CI user cannot use `ACCOUNTADMIN`, `apply`
creates nothing and prints the two lines for an `SFCTEST0` `ACCOUNTADMIN` to run
by hand. `check` only reads: it lists the objects and, once all exist, confirms
that each applied network policy contains the network rule, and prints
`MISSING: ...` with exit code 3 for any that does not. The nightly run starts
with `apply`, so drift repairs itself. The script never drops or removes
anything, and rerunning it is safe.

The workflow has no approval step or on switch. It runs only from `master` of
this repository and uses the same CI login as the other end-to-end tests.

Offline test: `python3 test/spcs/test_setup_ci.py -v`.

## Running from your own account

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
  --jar target/snowflake-kafka-connector-<version>.jar \
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

[`spcs-smoke.yml`](../../.github/workflows/spcs-smoke.yml) holds the job. Two
workflows call it, and both run only from `master` of this repository:

- **Nightly**: [`spcs-nightly.yml`](../../.github/workflows/spcs-nightly.yml)
  at 06:00 UTC, separate from the 20:00 UTC end-to-end nightly. It first runs
  setup with `apply`, which repairs drift before the job starts. SFCTEST0's
  account network policy has been swapped without notice (Nov 2026), which
  otherwise causes HTTP 401 inside the job. Each repair adds only the pool rule
  and shows as a `self-repair` warning annotation on the run. The preflight
  fails, and reports, if it cannot repair (for example, the CI user loses
  `ACCOUNTADMIN`) or if its contracts job fails.
- **Manual**: Kafka Connector end-to-end tests with `run_spcs=true`.

A failed scheduled run calls [`report_failure.py`](report_failure.py). It
comments on the open SNOW ticket labelled `kc-spcs-nightly-failure`, or creates
one: a Bug assigned to `triage-data_pipelines-snowpipe-streaming-v2-dl`,
component "Connectors - Kafka Push (Kafka Connector)", Area "Data Ingestion".
One ticket collects repeated failures until triage closes it. Manual runs do
not file tickets. Reporting needs three repository secrets; see
[Owning the nightly and Jira reporting](#owning-the-nightly-and-jira-reporting).

The job runs as the existing CI profile (`TESTROLE_KAFKA` in
`SFCTEST0`) against the permanent objects from the one-time setup above, so it
needs no secrets of its own and no approval step.
[`ci_connection.py`](ci_connection.py) turns the profile into the `snow`
connection and supplies the profile's warehouse.
[`publish_image.sh`](publish_image.sh) then builds the test image from this
commit, pushes it to `TESTDB_KAFKA.KC_SPCS_CI.IMAGES`, and the run tests that
exact digest. Nothing needs to be configured; `SPCS_EGRESS` is an optional
repository variable naming an existing external access integration, only if the
container ever needs one.

It always uses `--cleanup-policy fail`: leftovers in the shared account fail the run.

The pool `KC_SPCS_CI_POOL` is dedicated to this job, and the job and the
setup workflow share one concurrency group, so `--exclusive-pool` is always passed.

CI retains only `result.json` for 14 days, never the key or raw log. Offline checks:

```bash
python3 -m unittest discover -s test/spcs -v
python3 test/spcs/observer_contract.py
```

Runs in personal accounts on October 10 verified 100 rows on AWS, Azure, and GCP;
later recovery changes were requalified on Azure only. New setup and diagnostic
changes have offline coverage but need live requalification. The manual GitHub
Actions job passed on the permanent CI objects on 2026-11-18 and 2026-11-22
(100 of 100 rows, cleanup complete). The six-case
trial in #1587 does not exercise SPCS. Token-rotation soak, policy-matrix tests,
continuous monitoring, and exact-release-artifact enforcement are not covered.

## Owning the nightly and Jira reporting

### What runs by itself and what needs a person

| Situation | What happens |
|---|---|
| A permanent object, grant, or the pool rule in a network policy is missing | The nightly's `apply` step adds it and logs a `self-repair` warning on the run. No ticket. |
| The test fails, `apply` cannot repair (for example, the CI user lost `ACCOUNTADMIN`), the setup contracts fail, or the run is cancelled because another SPCS run took the shared concurrency slot | The run is red (grey if cancelled) and one SNOW ticket labelled `kc-spcs-nightly-failure` gets a comment, or is created if none is open, assigned to `triage-data_pipelines-snowpipe-streaming-v2-dl`. |
| The Jira token expired or is wrong | The test result is unaffected, but on a failing night the `report-failure` job also goes red with `Jira request failed: 401`. Renew the token (below). |
| Triage closes the ticket | The next failure opens a new one. |

### The Jira account

Tickets and comments are filed as the Jira account in the `JIRA_USER_EMAIL`
secret, using that account's API token. It was first set up with Berni
Schiefer's account as a stopgap; the intended long-term owner is a member of
the Kafka connector team or a service account. The account needs to browse,
create, assign, and comment on SNOW issues, and the triage DL must be
assignable in SNOW; the check below confirms all of this. Changing the account needs no code change.

### Set up, take over, or renew

Anyone with push access to this repository can do this. The GitHub settings
page hides secrets from non-admins, but the `gh` CLI works.

1. **The new owner creates an API token** at
   <https://id.atlassian.com/manage-profile/security/api-tokens>, signed in as
   the account that will own reporting. Use **Create API token**, not the
   scoped variant: scoped tokens need a different API URL and do not work with
   this script (requires validation if Atlassian changes this). Name it
   `kc-spcs-nightly`, choose the longest expiry offered, and put the expiry date
   in your calendar. Never send the token to anyone; set it yourself in step 2.
2. **Set the secrets.** Each command prompts for the value, so it stays out of
   shell history:

   ```bash
   gh secret set JIRA_USER_EMAIL -R snowflakedb/snowflake-kafka-connector  # the owner's Atlassian email
   gh secret set JIRA_API_TOKEN -R snowflakedb/snowflake-kafka-connector   # the token from step 1
   ```

   `JIRA_BASE_URL` is already set. Only if it is ever missing:
   `gh secret set JIRA_BASE_URL -R snowflakedb/snowflake-kafka-connector --body https://snowflakecomputing.atlassian.net`.
3. **Check it works.** This files nothing:

   ```bash
   gh workflow run spcs-nightly.yml -R snowflakedb/snowflake-kafka-connector --ref master -f check_jira=true
   gh run watch -R snowflakedb/snowflake-kafka-connector \
     "$(gh run list -R snowflakedb/snowflake-kafka-connector -w spcs-nightly.yml -L 1 --json databaseId --jq '.[0].databaseId')"
   ```

   The `check-jira` job must be green, and its log must show
   `Jira account: <the new owner>` and `Jira reporting is ready.` Otherwise it
   names each missing permission, or says the triage DL cannot be assigned.
4. **The previous owner revokes their old token** on the same Atlassian page.
5. **Renew before expiry** by repeating steps 1 to 3 with the same account.

An open `kc-spcs-nightly-failure` ticket carries over: the new account keeps
commenting on it, because the ticket is found by its label, not its reporter.

### Troubleshooting the report job

| Message | Cause and fix |
|---|---|
| `Cannot report to Jira; secrets not set: ...` | Step 2 for the named secrets. |
| `Jira request failed: 401` | Token wrong, expired, or not owned by the `JIRA_USER_EMAIL` account. Steps 1 to 3. |
| `Jira request failed: 400` or `403` | Run the check (step 3). If it passes, the component, Area, or assignee IDs in `FIELDS` in [`report_failure.py`](report_failure.py) may have changed in Jira. |

