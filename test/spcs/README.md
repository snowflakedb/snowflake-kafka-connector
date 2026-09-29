# KC v4 release test in SPCS (SNOW-4202412)

Runs the Snowflake Kafka Connector v4 **inside** Snowpark Container Services
(SPCS) with ambient auth (no credentials in connector config), produces
records, and proves rows land in Snowflake. A connector in `RUNNING` state with
zero rows is a failure. Background: SNOW-4201892 (KC 4.2.0 in SPCS failed with
390422 on `get_pipe_info` because the account network policy had no
`TYPE=COMPUTE_POOL` rule).

## Files

| File | Purpose |
|---|---|
| `provision.sql` | One-time account setup by an admin (pool, repo, stage, rules, policies, driver user). Not run by CI. |
| `Dockerfile` | JRE-only base image, pushed once at provisioning. |
| `job.yaml` | Finite SPCS job spec template (`${IMAGE}`, `${STAGE}`, `${TABLE}`, ...). |
| `run-e2e.sh` | Harness inside the container: KRaft broker + producer + Connect standalone. Prints `E2E_ERR`/`E2E_EXIT`. |
| `run_spcs_release.py` | Driver, runs outside SPCS (key-pair auth). Runs cells, applies oracle, exits non-zero on failure. |
| `spcs_oracle.py` | Pure pass/fail oracle. |
| `test_oracle.py` | Offline oracle unit tests. |

## Matrix

Cells run sequentially on one compute pool, each a fresh `EXECUTE JOB SERVICE`
with its own table. The driver sets the **account** network policy per cell
(dedicated test account only); the driver user has its own user-level policy
so it cannot lock itself out. The policy is always unset at the end.

| Cell | Account network policy | Expected |
|---|---|---|
| A | none | PASS: rows >= N |
| B | `KC_NP_WITH_POOL` (IPv4 rule + `TYPE=COMPUTE_POOL` rule) | PASS: rows >= N |
| C | `KC_NP_WITHOUT_POOL` (IPv4 rule only) | Documented failure: PASS only if rows == 0 **and** 390422 seen |

## Oracle (`spcs_oracle.evaluate`)

- Any non-terminal job status (`RUNNING`, `PENDING`, unknown) is never a pass.
- A, B: rows >= N, zero `390422` and `395090` occurrences in container logs,
  `E2E_EXIT=0`, job status `DONE`.
- C: rows == 0 and `390422` present. Rows appearing, or a different error,
  fails the cell and flags a possible Global Services behavior change for
  triage (update the oracle deliberately if GS semantics change).

Each failed cell is retried once automatically (fresh job and table).

## Running

Offline oracle tests (no Snowflake needed):

```
python3 -m unittest test/spcs/test_oracle.py -v
```

Full run against the provisioned test account:

```
pip install snowflake-connector-python cryptography
mvn -DskipTests package            # produces target/snowflake-kafka-connector-<ver>.jar
curl -fsSLO https://archive.apache.org/dist/kafka/4.1.1/kafka_2.13-4.1.1.tgz
export SPCS_ACCOUNT=<account> SPCS_USER=<driver user>
export SPCS_PRIVATE_KEY_FILE=<path to PEM key> SPCS_IMAGE=<repo_url>/kc-spcs-release:1
export KC_JAR=target/snowflake-kafka-connector-<ver>.jar KAFKA_TGZ=kafka_2.13-4.1.1.tgz
python3 test/spcs/run_spcs_release.py            # --cells A,B,C --nrecords 1000 --timeout-secs 600
```

## CI and release gate

Job `spcs-release` in `.github/workflows/end-to-end.yaml` runs on push to
`release-*`, on the nightly schedule, and on `workflow_dispatch` with
`run_spcs`. It is serialized (`concurrency: spcs-release-account`) because
cells mutate the account network policy. Secrets: `SPCS_RELEASE_ACCOUNT`,
`SPCS_RELEASE_USER`, `SPCS_RELEASE_PRIVATE_KEY`, `SPCS_RELEASE_IMAGE`,
optional `SPCS_RELEASE_HOST`.

**Where the gate must be wired (open):** this repo has no publish workflow.
Maven Central publishing is `deploy.sh` (`mvn clean deploy` with GPG/Sonatype
settings) and internal Nexus upload is `upload_jar.sh`; neither is invoked by
anything in this repo. They are run externally (Jenkins job
`BuildKafkaConnectorArtifactory` and/or a release pipeline outside this repo).
Until that pipeline is changed to require a green `spcs-release` run for the
release commit (or the publish step moves into GitHub Actions with
`needs: spcs-release`), this job alerts but does not block publishing.

**Override:** `workflow_dispatch` with `override_spcs_gate: true` and a
non-empty `override_reason` skips the cells, succeeds, and logs actor + reason
as a warning and in the job summary. Use only with sign-off from the **SSv2
(Snowpipe Streaming) on-call**; there is no KC on-call. Put the approver's name
in `override_reason`.

## Network policy pitfall (customer-facing)

- **390422 from KC in SPCS means the network policy has no `TYPE=COMPUTE_POOL`
  rule.** For SPCS source IPs, GS evaluates only compute-pool rules and fails
  closed.
- **IPv4 rules never match SPCS IPs**, even `0.0.0.0/0`.
- `capabilities.securityContext.enableCustomCredentials: true` is required
  (without it ingest returns 395090), but it makes the session
  `SPCS_CUSTOM_CREDS`, which **forfeits the SPCS-native network-policy
  bypass**.
- Fix: create a compute-pool rule and add it to the policy. The rule list is
  parenthesized and names are fully qualified:

```sql
CREATE NETWORK RULE <db>.<schema>.<rule>
  TYPE = COMPUTE_POOL MODE = INGRESS VALUE_LIST = ('<compute_pool>');
ALTER NETWORK POLICY <policy>
  ADD ALLOWED_NETWORK_RULE_LIST = ('<db>.<schema>.<rule>');
```

- `VALUE_LIST = ('ALL')` matches every compute pool in the account. Prefer it
  for prober fixtures; this test pins `KC_POOL` because cell B exercises a
  named compute-pool rule.

### Fixture trap

A fixture built only from the Snowpipe Streaming in SPCS docs page will not
have a compute-pool rule: that page's Limitations section points at egress and
External Access Integrations, not at the ingress network policy. Any KC-on-SPCS
test or prober (this test, SNOW-4202413) must add the `TYPE=COMPUTE_POOL` rule
explicitly. The Terraform provider (`snowflakedb/snowflake` ~> 2.20) has no
`COMPUTE_POOL` network-rule type, so create the rule with `snowflake_execute`
or plain SQL.
