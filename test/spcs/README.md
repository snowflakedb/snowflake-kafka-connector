# KC v4 SPCS release validation (SNOW-4202412)

**Author:** Berni Schiefer (assisted by openai-gpt-6-astra, Cortex Code agent mode)
**Date:** 2026-10-09
**Status:** Draft; opt-in, isolated fixture and live qualification required
**Document version:** 2.0

Run Kafka Connector v4 inside Snowpark Container Services (SPCS) and verify the
actual landed records independently of connector status. This addresses the
RUNNING-but-zero-rows regression reported under SNOW-4201892. It is not yet a
publishing gate or a qualified production test.

## Contract

- A: no account policy; B: a policy containing the named compute-pool rule.
  Both require exactly N distinct IDs with the expected values, a DONE job,
  complete harness summaries and no 390422/395090.
- C: policy without the compute-pool rule. Require FAILED, zero rows, complete
  summaries, exit 1 and 390422 without 395090. Submission failure, unknown state,
  empty logs and startup failure cannot satisfy this negative test.
- One retry at most, with distinct job/table names. Infrastructure/cleanup
  exceptions abort rather than retry unsafe account mutations.

## Dedicated fixture only

An owner must provision a dedicated empty account using `provision.sql` after
replacing its placeholders, approving driver egress CIDRs and setting a budget.
Never use a customer or shared Snowprober account. The operator creates a
`FIXTURE_IDENTITY` sentinel and driver-specific network policy. The CI driver
requires account-policy privileges within this isolated fixture; it is not a
least-privilege production identity.

The driver requires matching `SPCS_EXPECTED_ACCOUNT` and
`SPCS_CONFIRM_DEDICATED_ACCOUNT`, verifies CURRENT_ACCOUNT and the sentinel,
and checks the driver's explicit user-level policy. These checks prevent
accidental target selection; they cannot prove an administrator never placed
unrelated data in the account. Owner isolation remains a prerequisite.

An exclusive `RELEASE_RUN_LOCK` table serializes policy changes across drivers.
It is never stolen automatically. Before policy changes, a private recovery
journal records the previous policy, run ID, stage prefix and current objects.
The original policy is restored, including an originally unset policy. Failed
restoration or uncertain cleanup retains the lock for owner recovery. Policy
changes by other administrators during a run are forbidden; the table lock
coordinates this harness, not external operators.

## Artifacts and execution

Use Python 3.11+. `Dockerfile` requires a reviewed immutable JRE 17 base digest;
the resulting SPCS image must also be specified by digest. Kafka 4.1.1 matches
the repository's Apache test lane and requires JRE 17. The driver verifies
`KAFKA_SHA256` from an independently approved distribution checksum and
`KC_JAR_SHA256` for the exact connector build. It does not download or trust a
mutable latest connector. Stage uploads use a unique run prefix.

Required environment: `SPCS_ACCOUNT`, `SPCS_USER`, `SPCS_PRIVATE_KEY_FILE`,
`SPCS_EXPECTED_ACCOUNT`, `SPCS_CONFIRM_DEDICATED_ACCOUNT`, `SPCS_IMAGE`,
`KC_JAR`, `KC_JAR_SHA256`, `KAFKA_TGZ`, `KAFKA_SHA256`. Optional:
`SPCS_HOST`, `SPCS_PRIVATE_KEY_PASSPHRASE`. Never commit credentials.

```bash
python3 -m unittest discover -s test/spcs -v
# Only after owner-approved fixture setup:
python3 test/spcs/run_spcs_release.py --journal /approved/evidence/new-run.json
```

The journal's parent directory must exist; an existing journal is never
replaced. Default matrix is A,B,C, 1000 records, 600 seconds per cell and one
retry. Driver statements and network calls are bounded; the job is polled
asynchronously. The Linux container has an explicit coreutils dependency for
bounded Kafka CLI commands. Missing final container logs fail closed rather
than imply success; durable event-log fallback remains follow-up work.

## CI and publication

`spcs-contracts` runs offline tests without secrets. The live `spcs-release`
job is **manual only**, additionally requiring repository variable
`SPCS_RELEASE_ENABLED=true` and the `spcs-release` GitHub environment. Leave
it disabled until the fixture is qualified. Configure protected-environment
approvals before enabling it; the workflow cannot create that protection.

Environment secrets: `SPCS_RELEASE_ACCOUNT`, `SPCS_RELEASE_USER`,
`SPCS_RELEASE_PRIVATE_KEY`, optional `SPCS_RELEASE_HOST`. Variables:
`SPCS_EXPECTED_ACCOUNT`, `SPCS_RELEASE_IMAGE_DIGEST`, `SPCS_KAFKA_SHA256`.
The workflow deletes its temporary private key and uploads only the recovery
journal, not credentials or raw logs.

There is no success-producing override. A skipped job is not validation.
`deploy.sh` and external publishing orchestration still need a deliberate
integration requiring a passing run for the exact release commit. Nightly and
release-trigger activation are follow-ups after live qualification. Do not
claim SNOW-4202412 complete until those gates are connected.

## Recovery and qualification

On interruption, first stop/inspect the named job, then restore the exact
journaled account policy using an authorized owner connection. Verify policy
readback and absence of the run's service/table/stage prefix before removing
the lock. Never drop another run's resources, clear an unknown lock, or assume
process termination ran cleanup. Retain evidence if any outcome is unknown.

Still required before activation: real A/B/C runs, cancellation and recovery,
concurrent-run rejection, schema/cardinality verification, immutable image and
Kafka checksum qualification, and publisher integration. Offline tests do not
prove account-policy behavior. 390422 is a general rejection code, not by itself
proof of a missing pool rule; the isolated matrix establishes that distinction.
