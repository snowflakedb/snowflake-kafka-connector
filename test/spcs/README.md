# KC v4 SPCS smoke test

**Author:** Berni Schiefer (openai-gpt-6-astra, Cortex Code agent mode)
**Date:** 2026-10-10 22:14 UTC
**Status:** Draft dependent harness; prior qualification preserved, activation deferred
**Document version:** 6.1

One positive ingestion test, not a new release framework. A finite Snowpark
Container Services (SPCS) job runs Kafka, distributed Connect, and pytest.
`tests/spcs/test_spcs_ingestion.py` reuses the existing E2E producer, connector
lifecycle, and row polling. It sends 100 numbered records using KC v4 ambient
SPCS authentication. The host independently verifies all 100 values afterward.

## Review and merge order

The ingestion test, ambient-authentication fixtures, shared driver changes, and
three observer contracts belong to [PR #1585](https://github.com/snowflakedb/snowflake-kafka-connector/pull/1585).
This dependent PR adds the host runner, container, recovery tests, and manual
workflow. Review and merge #1585 first, then this harness. Both remain draft
and unassigned until explicitly approved for human review.

The combined runtime code and workflow match the previously qualified
`ba367d6` implementation. The split changes review boundaries and documentation,
not execution behavior. Live results below are prior qualification evidence,
not new runs on the split commits. Release-gate activation and publishing
integration remain separate follow-up work.

## An SPCS environment is required

Before running, please provide an existing test account with:

- An unused, suspended, auto-resuming compute pool with room for a 6 GiB job.
  Reserve it exclusively through cleanup, including other developers and CI.
  `--exclusive-pool` acknowledges this requirement; it is not a distributed lock.
  Pool inspection and suspension are separate operations, so concurrent use
  cannot be made safe by the final empty-pool check alone.
- A database where the selected service-owner role can create temporary test
  schemas, and an internal stage suitable for SPCS mounts.
- An image repository containing the test image, pinned by digest.
- A warehouse that the service-owner role can use without secondary roles.
- Working SPCS-to-Snowflake networking. Supply an existing approved external
  access integration with `--egress` if the environment requires one.
- A named Snowflake CLI connection for the outside observer/operator.

The runner does not provision accounts, grant permissions, change network
policies, or change authentication. Personal QA/preprod accounts can qualify
this test; recurring release CI still needs a team-owned fixture and owner.

## Build once, run the current connector artifact

Build the connector using the repository's normal Maven build. Build Linux
AMD64 images with Docker or Podman, from the `test/` directory:

```bash
podman build --platform linux/amd64 -f docker/Dockerfile.apache-kafka \
  --build-arg KAFKA_VERSION=4.1.1 --build-arg SCALA_VERSION=2.13 \
  --build-arg JAVA_VERSION=17 -t localhost/kc-spcs-apache:4.1.1-java17 .
podman build --platform linux/amd64 -f spcs/Dockerfile \
  --build-arg KAFKA_IMAGE=localhost/kc-spcs-apache:4.1.1-java17 \
  -t localhost/kc-spcs-smoke:1 .
```

Publish the smoke image to the existing Snowflake repository through the normal
registry authentication process. For Podman, pass the complete JSON credential
response to `0sessiontoken`, not just its `token` field. This works with an
existing key-pair connection; a new user or programmatic access token is not
required for the Azure and GCP fixtures whose registry logins were verified.

```bash
set -o pipefail
snow spcs image-registry token --connection MY_TEST_CONNECTION --format JSON |
  podman login MY_REGISTRY_HOST --username 0sessiontoken --password-stdin
```

An uncached Podman login does not mean the Snowflake credentials are missing.
A registry response of `source IP address not allowed` requires restoring the
approved network path before testing credentials. Obtain the published image
digest from `SHOW IMAGES IN IMAGE REPOSITORY`. Do not print the token response
or copy credentials into an image or stage.

From the repository root, with Snowflake CLI installed and an existing local
evidence directory:

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

Use a new evidence filename per run. The runner verifies the account before
mutations, creates a uniquely named schema/job/stage prefix, and records the
artifact checksums and resource names. Host credentials remain on the host.
The in-container SQL observer uses its runtime token, not a supplied private key.

## Pass, failure, and cleanup

Ingestion passes only with a DONE job, exactly one successful pytest completion
marker, and independent verification of exactly the expected 100 unique values.
RUNNING, missing evidence, wrong data, timeout, and interruption always fail.
Cleanup is recorded separately:

- `--cleanup-policy warn` (default): successful ingestion plus incomplete cleanup
  exits zero with `outcome=PASS_WITH_CLEANUP_WARNING` and a visible warning.
- `--cleanup-policy fail`: the same cleanup problem exits nonzero with `outcome=FAIL`.
- Both policies preserve `test_passed`, `cleanup_complete`, cleanup error type,
  and resource names. A cleanup warning never converts a failed test into a pass.

The ten-minute container deadline includes payload extraction and execution.
The host polls for up to fifteen minutes, plus bounded diagnostic and cleanup
calls. Cleanup drops the run's job before its schema and payload, then suspends
only an empty pool. Readbacks verify the schema and payload are absent and the
pool reaches SUSPENDED. Existing services and fixture objects are not deleted.

SIGTERM and Ctrl-C trigger recovery. An uncertain schema-creation response is
reconciled using the unique name and a run-specific comment before deletion.
An absent or mismatched marker leaves cleanup unresolved rather than guessing.
SIGKILL, a host crash, lost connectivity, or repeated interruption can still
require manual recovery using the recorded names. Never suspend a pool that
has acquired other work; warnings still require operator follow-up.

Before cleanup, the runner attempts bounded log collection, including on timeout.
Raw logs stay in a local 0600 file and are never uploaded by the workflow. The
result JSON contains only selected diagnostic signals: completion codes, pytest
counts, SQL error codes, and known exception types, not arbitrary log excerpts
or CLI error strings. If collection fails, cleanup still runs.

## Current qualification status

The Linux AMD64 smoke image builds. All 34 offline runner contracts and three
observer and smoke-configuration regression tests pass. Collection selects
exactly one v4 SPCS test; ordinary lanes exclude it. A network-disabled local run starts Kafka
and Connect, reaches pytest, and fails at the expected real-SPCS guard. This
checks startup only, not SPCS authentication or ingestion.

With explicit approval, the service-owner role received USAGE on the existing
warehouse in both personal preprod fixtures. Real queries verified that access
with secondary roles disabled. No account policies were changed.

Azure network access was restored by selecting the approved DEV VPN gateway.
Registry login succeeded with the existing service-user key pair after passing
the complete CLI JSON response as the registry password. The smoke image was
published. Azure live qualification passed on 2026-10-10: KC 4.2.1,
Snowflake 10.37.100, job DONE, one successful pytest marker, and independent
verification of all 100 expected values. The run's job, schema, and staged
payload were removed; separate checks confirmed the schema and prefix absent
and the pool SUSPENDED with zero jobs and services.

The live run also exposed two test-harness fixes: the job uses a single command
with `-c` in its argument list, and the connector config supplies the role read
from the ambient SQL session. KC's existing streaming validator requires that
field; SPCS still determines privileges from the service owner. No connector
production code or account policies were changed.

AWS QA6 also passed on 2026-10-10 with Snowflake 10.38.10, the same KC 4.2.1
artifact and image digest, and no further harness changes. Pytest passed in
21.93 seconds; the job reached DONE and the outside observer verified all 100
expected values. Independent cleanup checks confirmed the schema and staged
payload absent and the pool SUSPENDED with zero jobs and services.

QA6 registry-token issuance through the CLI returned error 390115 for both
existing identities. Registry authentication and image publication succeeded
using the existing service user's active SQL session token in the JSON
credential envelope, keeping that session open through the push. This used
private connector APIs for manual qualification, not a qualified CI login path.
No new credentials or policy changes were needed.

GCP preprod3 passed on 2026-10-10 with Snowflake 10.37.100 and the same KC 4.2.1
artifact and image digest. The existing key-pair connection worked with the
normal CLI registry-token flow and complete JSON envelope. Pytest passed in
21.45 seconds; the job reached DONE and the outside observer verified all 100
expected values. Independent cleanup checks confirmed the schema and staged
payload absent and the pool SUSPENDED with zero jobs and services. No additional
harness changes, credentials, or network-policy changes were needed for GCP.

After the review fixes, Azure passed again with `--cleanup-policy fail` using
the same artifact and image. The runner survived a desktop crash, verified all
100 expected values, captured diagnostics, and recorded `outcome=PASS`,
`test_passed=true`, and `cleanup_complete=true`. Separate queries confirmed the
schema and staged payload absent and the pool SUSPENDED with zero jobs,
services, and active nodes. No duplicate run or manual cleanup was needed.

The revised recovery and cleanup code has been live-tested on Azure only.
AWS QA and GCP results above predate those changes. These are smoke checks on
personal fixtures, not soak tests or a qualified team-owned release gate.
AWS QA ran a different Snowflake version from Azure and GCP.

## Validation and CI

```bash
python3 -m unittest discover -s test/spcs -v
# With the Python dependencies listed in test/spcs/Dockerfile installed:
python3 test/spcs/observer_contract.py
```

Ordinary E2E lanes exclude the `spcs` marker. The live workflow remains opt-in
(`run_spcs`, `SPCS_RELEASE_ENABLED=true`, protected `spcs-release` environment).
Configure account/user/private-key environment secrets and fixture variables
`SPCS_EXPECTED_ACCOUNT`, `SPCS_POOL`, `SPCS_STAGE`, `SPCS_DATABASE`, `SPCS_ROLE`,
`SPCS_WAREHOUSE`, and `SPCS_RELEASE_IMAGE_DIGEST` before activation. Set
`SPCS_EXCLUSIVE_POOL=true` only after reserving the pool across all operators.
`SPCS_CLEANUP_POLICY` accepts `warn` (default) or `fail`. The offline CI job
installs the observer dependencies and explicitly runs both contract suites.
The exact observer dependency install passed in a fresh Python 3.13 environment
inside the Linux image; `pip check` and all three observer tests passed.
The artifact step includes the hidden `.scratch` directory but selects only
`result.json`, not the private key or raw log. The workflow itself has not been
live-qualified on GitHub Actions. Do not put personal account configuration
into repository defaults.

Not covered: network-policy A/B/C testing, token-rotation soak, throughput/file
mode, continuous monitoring, or enforcement by the publishing pipeline. A
manual smoke pass is not an enforced release gate.
