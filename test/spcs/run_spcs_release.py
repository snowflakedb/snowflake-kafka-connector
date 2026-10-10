#!/usr/bin/env python3
"""Run one finite KC v4 smoke job against an existing SPCS fixture."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import tarfile
import tempfile
import time
import uuid

HERE = Path(__file__).resolve().parent
TEST = HERE.parent


def identifier(value):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"Expected an unquoted Snowflake identifier: {value!r}")
    return value.upper()


def qualified(value):
    parts = value.split(".")
    if len(parts) != 3:
        raise ValueError("Expected DATABASE.SCHEMA.OBJECT")
    return ".".join(identifier(part) for part in parts)


def error_summary(error):
    # Never copy arbitrary CLI/log text into the CI-uploaded evidence.
    return {
        "type": type(error).__name__,
        "sql_codes": sorted(
            set(re.findall(r"\b\d{6}(?= \([A-Z0-9]{5}\))", str(error)))
        ),
    }


def sql(args, statement, *, timeout=90):
    result = subprocess.run(
        [
            "snow",
            "sql",
            "-c",
            args.connection,
            "--role",
            args.role,
            "--warehouse",
            args.warehouse,
            "--secondary-roles",
            "NONE",
            "--format",
            "JSON",
            "-q",
            statement,
        ],
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if result.returncode:
        raise RuntimeError(result.stderr.strip() or "Snowflake CLI failed")
    return json.loads(result.stdout)


def save(path, evidence):
    descriptor, temporary = tempfile.mkstemp(dir=path.parent, prefix=path.name + ".")
    try:
        with os.fdopen(descriptor, "w") as stream:
            json.dump(evidence, stream, indent=2)
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def preflight(args):
    if not args.exclusive_pool:
        raise ValueError("Reserve the pool for this run and supply --exclusive-pool")
    identity = sql(
        args,
        "SELECT CURRENT_ACCOUNT() AS ACCOUNT, CURRENT_ROLE() AS ROLE, "
        "CURRENT_VERSION() AS VERSION",
    )[0]
    if identity["ACCOUNT"].upper() != args.expected_account.upper():
        raise RuntimeError("Wrong account; no resources created")
    if identity["ROLE"].upper() != args.role:
        raise RuntimeError("Wrong service-owner role")
    pools = sql(args, "SHOW COMPUTE POOLS")
    pool = next((pool for pool in pools if pool["name"] == args.pool), None)
    if pool is None or pool["num_services"] or pool["num_jobs"]:
        raise RuntimeError("Select an existing unused compute pool")
    if pool["state"] != "SUSPENDED" or str(pool["auto_resume"]).lower() != "true":
        raise RuntimeError("Select an unused suspended pool with auto_resume enabled")
    sql(args, f"DESCRIBE STAGE {args.stage}")
    # This requires actual warehouse access with secondary roles disabled.
    sql(args, "SELECT COUNT(*) FROM TABLE(GENERATOR(ROWCOUNT => 1))")
    return identity


def make_payload(path, jar):
    with tarfile.open(path, "w:gz") as archive:
        for relative in ["lib", "tests/spcs", "conftest.py", "pyproject.toml"]:
            source = TEST / relative
            files = sorted(source.rglob("*")) if source.is_dir() else [source]
            for file in files:
                if (
                    file.is_file()
                    and "__pycache__" not in file.parts
                    and file.suffix != ".pyc"
                ):
                    archive.add(file, arcname="test/" + str(file.relative_to(TEST)))
        archive.add(HERE / "run-e2e.sh", arcname="run-e2e.sh")
        archive.add(jar, arcname="kc.jar")


def specification(args, stage_path):
    # JSON is valid YAML and avoids interpolating arbitrary strings into a spec.
    return json.dumps(
        {
            "spec": {
                "containers": [
                    {
                        "name": "smoke",
                        "image": args.image,
                        "command": ["/bin/bash"],
                        "args": [
                            "-c",
                            "exec timeout --signal=TERM --kill-after=15s 600 bash -c "
                            "'set -e; cd /work; tar xzf /mnt/harness/payload.tgz; "
                            "exec bash /work/run-e2e.sh'",
                        ],
                        "env": {"SPCS_QUERY_WAREHOUSE": args.warehouse},
                        "volumeMounts": [
                            {"name": "harness", "mountPath": "/mnt/harness"},
                            {"name": "work", "mountPath": "/work"},
                        ],
                        "resources": {
                            "requests": {"memory": "4Gi"},
                            "limits": {"memory": "6Gi"},
                        },
                    }
                ],
                "volumes": [
                    {
                        "name": "harness",
                        "source": "stage",
                        "stageConfig": {"name": stage_path},
                    },
                    {"name": "work", "source": "local"},
                ],
            },
            "capabilities": {"securityContext": {"enableCustomCredentials": True}},
        }
    )


def wait_for_job(args, job):
    deadline = time.monotonic() + args.timeout
    while time.monotonic() < deadline:
        rows = sql(args, f"DESCRIBE SERVICE {job}")
        if len(rows) != 1:
            raise RuntimeError("Unexpected job status response")
        status = rows[0]["status"].upper()
        if status in ("DONE", "FAILED"):
            return status
        time.sleep(5)
    raise TimeoutError("SPCS job exceeded its deadline")


def verify(status, logs, values):
    if status != "DONE":
        raise RuntimeError(f"Job did not succeed: {status}")
    if re.findall(r"^SPCS_SMOKE_EXIT=(-?\d+)$", logs, re.MULTILINE) != ["0"]:
        raise RuntimeError("Missing, failed, or duplicate pytest completion marker")
    if any(not isinstance(value, str) for value in values) or sorted(values) != sorted(
        str(value) for value in range(1, 101)
    ):
        raise RuntimeError("Landed records do not match the expected 100 unique values")


def capture_diagnostics(args, job, evidence):
    """Keep raw logs private; only fixed-format signals enter the shared JSON."""
    logs = (
        sql(
            args,
            f"SELECT SYSTEM$GET_SERVICE_LOGS('{job}', 0, 'smoke', 1000) AS LOGS",
            timeout=30,
        )[0]["LOGS"]
        or ""
    )
    evidence["diagnostics"] = {
        "completion_codes": re.findall(
            r"^SPCS_SMOKE_EXIT=(-?\d{1,3})$", logs, re.MULTILINE
        ),
        "pytest_counts": re.findall(
            r"\b\d{1,6} (?:passed|failed|errors?|skipped)\b", logs
        )[-10:],
        "sql_codes": sorted(set(re.findall(r"\b\d{6}(?= \([A-Z0-9]{5}\))", logs))),
        "exception_types": sorted(
            set(
                re.findall(
                    r"\b(?:TimeoutError|AssertionError|ConnectionError|ProgrammingError|OperationalError|SnowflakeKafkaConnectorException)\b",
                    logs,
                )
            )
        ),
    }
    descriptor = os.open(
        args.evidence.with_suffix(".log"), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600
    )
    with os.fdopen(descriptor, "w") as stream:
        stream.write(logs)
    return logs


def reconcile_schema(args, schema, marker):
    """Delete only a schema positively identified as this run's creation."""
    database, name = schema.split(".")
    rows = sql(args, f"SHOW SCHEMAS LIKE '{name}' IN DATABASE {database}", timeout=30)
    matches = [row for row in rows if row["name"] == name]
    if not matches:
        raise RuntimeError("Schema creation outcome unresolved; inspect recorded name")
    if len(matches) != 1 or matches[0].get("comment") != marker:
        raise RuntimeError("Schema ownership marker mismatch; refusing cleanup")
    return True


def terminate(signum, frame):
    raise InterruptedError("Run interrupted by SIGTERM")


def finalize_result(evidence, cleanup_policy):
    evidence["passed"] = evidence["test_passed"] and (
        evidence["cleanup_complete"] or cleanup_policy == "warn"
    )
    evidence["outcome"] = (
        "PASS"
        if evidence["passed"] and evidence["cleanup_complete"]
        else "PASS_WITH_CLEANUP_WARNING"
        if evidence["passed"]
        else "FAIL"
    )


def cleanup(args, schema, job, stage_path):
    # Drop job before its data. On an uncertain deletion, preserve recovery evidence.
    sql(args, f"DROP SERVICE IF EXISTS {job}")
    sql(args, f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    sql(args, f"REMOVE {stage_path}")
    pools = sql(args, "SHOW COMPUTE POOLS")
    pool = next(pool for pool in pools if pool["name"] == args.pool)
    if pool["num_services"] or pool["num_jobs"]:
        raise RuntimeError("Pool now contains other work; refusing to suspend it")
    sql(args, f"ALTER COMPUTE POOL {args.pool} SUSPEND")
    database, name = schema.split(".")
    rows = sql(args, f"SHOW SCHEMAS LIKE '{name}' IN DATABASE {database}")
    if any(row["name"] == name for row in rows) or sql(args, f"LIST {stage_path}"):
        raise RuntimeError("Run resources still present after cleanup")
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        pool = sql(args, f"DESCRIBE COMPUTE POOL {args.pool}", timeout=15)[0]
        if pool["num_services"] or pool["num_jobs"]:
            raise RuntimeError("Pool now contains other work; manual recovery required")
        if pool["state"] == "SUSPENDED":
            return
        time.sleep(3)
    raise TimeoutError("Pool suspension was not confirmed")


def run(args):
    identity = preflight(args)
    run_id = "KC_SMOKE_" + uuid.uuid4().hex.upper()
    schema = f"{args.database}.{run_id}"
    job = f"{schema}.SMOKE_JOB"
    stage_path = f"@{args.stage}/{run_id}/"
    evidence = dict(
        identity=identity,
        schema=schema,
        job=job,
        stage=stage_path,
        pool=args.pool,
        image=args.image,
        passed=False,
        test_passed=False,
        cleanup_policy=args.cleanup_policy,
        cleanup_complete=False,
        connector_sha256=hashlib.sha256(args.jar.read_bytes()).hexdigest(),
    )
    # Exclusive creation protects prior evidence; record intended names before mutations.
    descriptor = os.open(args.evidence, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    with os.fdopen(descriptor, "w") as stream:
        json.dump(evidence, stream, indent=2)
    schema_created = False
    schema_attempted = False
    job_attempted = False
    diagnostics_attempted = False
    failure = None
    marker = "kc-smoke:" + run_id
    previous_handler = signal.signal(signal.SIGTERM, terminate)
    try:
        schema_attempted = True
        sql(args, f"CREATE SCHEMA {schema} COMMENT = '{marker}'")
        schema_created = True
        sql(
            args,
            f'CREATE TABLE {schema}.SMOKE_ROWS (RECORD_METADATA VARIANT, "number" VARCHAR)',
        )
        with tempfile.TemporaryDirectory(dir=args.evidence.parent) as directory:
            payload = Path(directory) / "payload.tgz"
            make_payload(payload, args.jar)
            evidence["payload_sha256"] = hashlib.sha256(
                payload.read_bytes()
            ).hexdigest()
            save(args.evidence, evidence)
            uri = payload.as_uri().replace("'", "''")
            sql(args, f"PUT '{uri}' {stage_path} AUTO_COMPRESS=FALSE OVERWRITE=FALSE")
        spec = specification(args, stage_path)
        egress = (
            f" EXTERNAL_ACCESS_INTEGRATIONS = ({args.egress})" if args.egress else ""
        )
        job_attempted = True
        sql(
            args,
            f"EXECUTE JOB SERVICE IN COMPUTE POOL {args.pool} NAME={job} "
            f"ASYNC=TRUE{egress} FROM SPECIFICATION $${spec}$$",
        )
        status = wait_for_job(args, job)
        evidence["job_status"] = status
        diagnostics_attempted = True
        logs = capture_diagnostics(args, job, evidence)
        values = [
            row["number"]
            for row in sql(args, f'SELECT "number" FROM {schema}.SMOKE_ROWS')
        ]
        evidence["landed_rows"] = len(values)
        evidence["distinct_values"] = len(set(values))
        verify(status, logs, values)
        evidence["test_passed"] = True
    except BaseException as error:
        failure = error
        evidence["test_passed"] = False
        evidence["error"] = error_summary(error)
    finally:
        # A second TERM must not interrupt bounded recovery. SIGKILL remains unrecoverable.
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        try:
            if job_attempted and not diagnostics_attempted:
                try:
                    capture_diagnostics(args, job, evidence)
                except Exception as error:
                    evidence["diagnostics_error"] = error_summary(error)
            try:
                if schema_attempted and not schema_created:
                    schema_created = reconcile_schema(args, schema, marker)
                if schema_created:
                    cleanup(args, schema, job, stage_path)
                    evidence["cleanup_complete"] = True
                else:
                    evidence["cleanup_complete"] = not schema_attempted
            except Exception as error:
                evidence["cleanup_error"] = error_summary(error)
            finalize_result(evidence, args.cleanup_policy)
            save(args.evidence, evidence)
        finally:
            signal.signal(signal.SIGTERM, previous_handler)
    print(json.dumps(evidence, indent=2))
    if not evidence["cleanup_complete"]:
        print(
            "WARNING: cleanup incomplete; inspect resources recorded in evidence",
            file=sys.stderr,
        )
    if failure is not None:
        raise RuntimeError(
            f"Smoke failed ({type(failure).__name__}); see evidence"
        ) from None
    if not evidence["passed"]:
        raise RuntimeError("Cleanup incomplete under strict cleanup policy")
    return evidence


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in (
        "connection",
        "expected-account",
        "pool",
        "stage",
        "image",
        "warehouse",
    ):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--database", default="KC_TEST")
    parser.add_argument("--role", default="SYSADMIN")
    parser.add_argument(
        "--egress", help="Existing approved external access integration, if needed"
    )
    parser.add_argument("--jar", required=True, type=Path)
    parser.add_argument("--evidence", required=True, type=Path)
    parser.add_argument("--timeout", type=int, default=900)
    parser.add_argument(
        "--cleanup-policy",
        choices=("warn", "fail"),
        default="warn",
        help="Whether incomplete cleanup changes a successful test to failure",
    )
    parser.add_argument(
        "--exclusive-pool",
        action="store_true",
        required=True,
        help="Confirm pool is reserved for this run, including manual/other CI users",
    )
    args = parser.parse_args(argv)
    for name in ("database", "role", "pool", "warehouse"):
        setattr(args, name, identifier(getattr(args, name)))
    args.stage = qualified(args.stage)
    if args.egress:
        args.egress = identifier(args.egress)
    if not re.fullmatch(r"/[A-Za-z0-9_./-]+@sha256:[a-f0-9]{64}", args.image):
        parser.error("--image must be a Snowflake repository path pinned by digest")
    if not 60 <= args.timeout <= 1200:
        parser.error("--timeout must be 60..1200 seconds")
    args.jar = args.jar.resolve(strict=True)
    args.evidence = args.evidence.absolute()
    if args.evidence.exists() or args.evidence.with_suffix(".log").exists():
        parser.error(
            "Use a fresh evidence basename; previous evidence must not be overwritten"
        )
    if not args.evidence.parent.is_dir():
        parser.error("--evidence parent directory must exist")
    return args


if __name__ == "__main__":
    run(parse_args())
