#!/usr/bin/env python3
"""Opt-in SPCS release validation on an owner-provisioned, dedicated test account.

Never run on a shared account. SPCS_EXPECTED_ACCOUNT is the CURRENT_ACCOUNT()
locator, not a hostname. A fixture marker and a driver-specific network policy
must be provisioned first. Journal recovery obligations before any mutation.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
from string import Template
import sys
import tempfile
import time
import uuid

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from spcs_oracle import Observation, evaluate

DB, SCHEMA = "KC_TEST", "KC"
STAGE = "KC_TEST.KC.HARNESS_STAGE"
POOL, ROLE, WAREHOUSE = "KC_POOL", "KC_SPCS_TEST", "KC_WH"
POLICY_FOR_CELL = {"A": None, "B": "KC_NP_WITH_POOL", "C": "KC_NP_WITHOUT_POOL"}
LOCK = "KC_TEST.KC.RELEASE_RUN_LOCK"


def quoted_identifier(value):
    if not isinstance(value, str) or not value or "\x00" in value:
        raise ValueError("invalid identifier")
    return '"' + value.replace('"', '""') + '"'


def connect():
    import snowflake.connector
    from cryptography.hazmat.primitives import serialization

    password = os.environ.get("SPCS_PRIVATE_KEY_PASSPHRASE")
    key = serialization.load_pem_private_key(
        Path(os.environ["SPCS_PRIVATE_KEY_FILE"]).read_bytes(),
        password.encode() if password else None,
    )
    options = dict(
        account=os.environ["SPCS_ACCOUNT"], user=os.environ["SPCS_USER"],
        private_key=key.private_bytes(serialization.Encoding.DER,
                                     serialization.PrivateFormat.PKCS8,
                                     serialization.NoEncryption()),
        role=ROLE, warehouse=WAREHOUSE, database=DB, schema=SCHEMA,
        login_timeout=30, network_timeout=60,
        session_parameters={"STATEMENT_TIMEOUT_IN_SECONDS": 60,
                            "STATEMENT_QUEUED_TIMEOUT_IN_SECONDS": 30,
                            "QUERY_TAG": "SNOW-4202412"},
    )
    if os.environ.get("SPCS_HOST"):
        options["host"] = os.environ["SPCS_HOST"]
    return snowflake.connector.connect(**options)


def query(conn, sql, params=None, admin=False, dictionaries=False):
    with conn.cursor() as cur:
        try:
            if admin:
                cur.execute("USE ROLE ACCOUNTADMIN", timeout=30)
            cur.execute(sql, params, timeout=60)
            rows = cur.fetchall()
            if dictionaries:
                columns = [column[0].lower() for column in cur.description]
                return [dict(zip(columns, row)) for row in rows]
            return rows
        finally:
            if admin:
                cur.execute("USE ROLE " + ROLE, timeout=30)


def account_policy(conn):
    rows = query(conn, "SHOW PARAMETERS LIKE 'NETWORK_POLICY' IN ACCOUNT",
                 admin=True, dictionaries=True)
    if len(rows) != 1 or "value" not in rows[0] or "level" not in rows[0]:
        raise RuntimeError("unrecognized account policy result")
    row = rows[0]
    if row["value"] and str(row["level"]).upper() != "ACCOUNT":
        raise RuntimeError("cannot safely restore inherited network policy")
    return row["value"] or None


def set_policy(conn, policy):
    sql = ("ALTER ACCOUNT SET NETWORK_POLICY = " + quoted_identifier(policy)
           if policy else "ALTER ACCOUNT UNSET NETWORK_POLICY")
    query(conn, sql, admin=True)
    if account_policy(conn) != policy:
        raise RuntimeError("network policy readback mismatch")


def preflight(conn, expected):
    actual = str(query(conn, "SELECT CURRENT_ACCOUNT()")[0][0])
    if actual.upper() != expected.upper():
        raise RuntimeError("refusing unexpected account")
    marker = query(conn, "SELECT ACCOUNT_LOCATOR, PURPOSE FROM KC_TEST.KC.FIXTURE_IDENTITY")
    if marker != [(actual, "SNOW-4202412_DEDICATED")]:
        raise RuntimeError("dedicated fixture marker missing or mismatched")
    rows = query(conn, "SHOW PARAMETERS LIKE 'NETWORK_POLICY' IN USER " +
                 quoted_identifier(os.environ["SPCS_USER"]), admin=True, dictionaries=True)
    if (len(rows) != 1 or rows[0].get("value") != "KC_NP_DRIVER"
            or str(rows[0].get("level", "")).upper() != "USER"):
        raise RuntimeError("driver needs its own KC_NP_DRIVER policy before testing")


class Journal:
    def __init__(self, path, account, run_id):
        self.path = Path(path)
        self.state = dict(account=account, run_id=run_id, phase="preflight", results=[])
        # Exclusive creation prevents overwriting earlier recovery evidence.
        with self.path.open("x") as stream:
            os.chmod(self.path, 0o600)
            json.dump(self.state, stream)
            stream.flush()
            os.fsync(stream.fileno())

    def save(self, **changes):
        self.state.update(changes)
        temporary = self.path.with_suffix(self.path.suffix + ".new")
        with temporary.open("w") as stream:
            os.chmod(temporary, 0o600)
            json.dump(self.state, stream, indent=2)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, self.path)


def verify_artifact(path, expected):
    if not re.fullmatch(r"[a-fA-F0-9]{64}", expected):
        raise ValueError("a pinned SHA256 is required")
    with Path(path).open("rb") as stream:
        digest = hashlib.file_digest(stream, "sha256").hexdigest()
    if digest.lower() != expected.lower():
        raise ValueError("artifact checksum mismatch: " + Path(path).name)


def upload_harness(conn, run_id):
    scratch = HERE / ".scratch"
    scratch.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(dir=scratch) as directory:
        for source, name in ((os.environ["KC_JAR"], "kc.jar"),
                             (os.environ["KAFKA_TGZ"], "kafka.tgz"),
                             (HERE / "run-e2e.sh", "run-e2e.sh")):
            destination = Path(directory) / name
            shutil.copyfile(source, destination)
            uri = destination.as_uri().replace("'", "''")
            query(conn, f"PUT '{uri}' @{STAGE}/{run_id}/ AUTO_COMPRESS=FALSE OVERWRITE=FALSE")


def run_cell(conn, cell, attempt, args, run_id, journal):
    suffix = f"{run_id}_{cell}_{attempt}"
    table = "KC_SPCS_REL_" + suffix
    job = f"{DB}.{SCHEMA}.KC_SPCS_JOB_{suffix}"
    journal.save(phase="cell_pending", cell=cell, job=job, table=table,
                 intended_policy=POLICY_FOR_CELL[cell])
    try:
        set_policy(conn, POLICY_FOR_CELL[cell])
        query(conn, f"CREATE TABLE {table} (RECORD_METADATA VARIANT, ID NUMBER, NAME VARCHAR)"
              " ENABLE_SCHEMA_EVOLUTION = TRUE")
        spec = Template((HERE / "job.yaml").read_text()).substitute(
            IMAGE=os.environ["SPCS_IMAGE"], STAGE=f"@{STAGE}/{run_id}/",
            TABLE=table, NRECORDS=args.nrecords, TIMEOUT_SECS=args.timeout_secs,
        )
        # Submission errors never count as an expected negative-test outcome.
        query(conn, f"EXECUTE JOB SERVICE IN COMPUTE POOL {POOL} NAME = {job} "
              f"ASYNC = TRUE FROM SPECIFICATION $$\n{spec}\n$$")
        deadline = time.monotonic() + args.timeout_secs + 180
        status, logs = None, ""
        while time.monotonic() < deadline:
            rows = query(conn, "DESCRIBE SERVICE " + job, dictionaries=True)
            status = str(rows[0]["status"]).upper() if len(rows) == 1 else None
            # Logs may not be available while the job is provisioning. Missing
            # final summaries remain a hard failure in the independent oracle.
            try:
                text = query(conn, "SELECT SYSTEM$GET_SERVICE_LOGS(%s, 0, 'e2e', 1000)",
                             (job,))[0][0]
                if text:
                    logs = text
            except Exception:
                pass
            if status in ("DONE", "FAILED"):
                break
            time.sleep(5)
        else:
            raise TimeoutError("job did not reach a known terminal state")
        stats = query(conn, f"SELECT COUNT(*), COUNT(DISTINCT ID), "
                      f"COALESCE(COUNT_IF(ID IS NULL OR ID < 1 OR ID > {args.nrecords} "
                      "OR NAME IS NULL OR NAME != 'spcs-release-' || ID::VARCHAR), 0) "
                      f"FROM {table}")[0]
        verdict = evaluate(Observation(cell, int(stats[0]), status, logs,
                                       int(stats[1]), int(stats[2])), args.nrecords)
        journal.state["results"].append(dict(cell=cell, attempt=attempt,
                                            passed=verdict.passed, reasons=verdict.reasons))
        journal.save(phase="cell_observed")
        return verdict
    finally:
        # If service deletion fails, retain the table and journal for recovery.
        query(conn, "DROP SERVICE IF EXISTS " + job)
        query(conn, "DROP TABLE IF EXISTS " + table)
        journal.save(phase="cell_cleaned")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cells", default="A,B,C")
    parser.add_argument("--nrecords", type=int, default=1000)
    parser.add_argument("--timeout-secs", type=int, default=600)
    parser.add_argument("--retries", type=int, default=1)
    parser.add_argument("--journal", required=True)
    args = parser.parse_args(argv)
    args.cells = [cell.strip().upper() for cell in args.cells.split(",")]
    if not args.cells or any(cell not in POLICY_FOR_CELL for cell in args.cells):
        parser.error("select at least one cell from A,B,C; empty cells are invalid")
    if len(set(args.cells)) != len(args.cells):
        parser.error("duplicate cells")
    if not 1 <= args.nrecords <= 10000 or not 30 <= args.timeout_secs <= 600:
        parser.error("nrecords must be 1..10000; timeout-secs must be 30..600")
    if not 0 <= args.retries <= 1:
        parser.error("retries must be 0 or 1")
    return args


def execute(conn, args, expected, run_id, journal):
    preflight(conn, expected)
    # CREATE without IF NOT EXISTS is the account-local concurrency guard.
    # Never steal a stale lock: its journal must be reconciled by the owner.
    query(conn, f"CREATE TABLE {LOCK} (RUN_ID VARCHAR) COMMENT='SNOW-4202412 {run_id}'")
    restored, cleaned, prior_known = False, False, False
    try:
        prior = account_policy(conn)
        prior_known = True
        journal.save(phase="locked", prior_policy=prior, stage=f"@{STAGE}/{run_id}/")
        upload_harness(conn, run_id)
        passed = True
        for cell in args.cells:
            for attempt in range(args.retries + 1):
                verdict = run_cell(conn, cell, attempt, args, run_id, journal)
                if verdict.passed:
                    break
            passed = passed and verdict.passed
        query(conn, f"REMOVE @{STAGE}/{run_id}/")
        cleaned = True
        return 0 if passed else 1
    finally:
        if prior_known:
            # Journal IO must never prevent restoration of the account policy.
            try:
                journal.save(phase="restoring_policy")
            finally:
                set_policy(conn, prior)
                restored = True
        if restored and cleaned:
            query(conn, "DROP TABLE " + LOCK)
            journal.save(phase="complete", cleanup_complete=True)
        else:
            journal.save(phase="recovery_required", cleanup_complete=False)


def main(argv=None):
    args = parse_args(argv)
    expected = os.environ["SPCS_EXPECTED_ACCOUNT"]
    if os.environ.get("SPCS_CONFIRM_DEDICATED_ACCOUNT") != expected:
        raise ValueError("explicit dedicated-account confirmation required")
    image = os.environ["SPCS_IMAGE"]
    if not re.fullmatch(r"/[A-Za-z0-9_./-]+@sha256:[a-f0-9]{64}", image):
        raise ValueError("SPCS_IMAGE must be an immutable repository digest path")
    verify_artifact(os.environ["KC_JAR"], os.environ["KC_JAR_SHA256"])
    verify_artifact(os.environ["KAFKA_TGZ"], os.environ["KAFKA_SHA256"])
    run_id = "R" + uuid.uuid4().hex.upper()
    journal = Journal(args.journal, expected, run_id)
    conn = connect()
    try:
        return execute(conn, args, expected, run_id, journal)
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
