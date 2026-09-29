#!/usr/bin/env python3
"""Driver for the KC SPCS release test (SNOW-4202412).

Runs OUTSIDE SPCS (CI runner) with key-pair auth and, for each cell A/B/C:
sets the account network policy, runs one finite EXECUTE JOB SERVICE (see
job.yaml / run-e2e.sh), counts rows, fetches container logs, and applies the
pure oracle in spcs_oracle.py. One automatic retry per failed cell. Exits
non-zero if any cell fails. The account policy is always UNSET at the end.

Environment:
  SPCS_ACCOUNT, SPCS_USER            test account and driver service user
  SPCS_PRIVATE_KEY_FILE              PEM private key (unencrypted, or with
  SPCS_PRIVATE_KEY_PASSPHRASE)       optional passphrase)
  SPCS_HOST                          optional explicit host
  SPCS_IMAGE                         image URL pushed at provisioning time
  KC_JAR, KAFKA_TGZ                  connector JAR and Kafka tarball to test
  NRECORDS (1000) TIMEOUT_SECS (600) CELLS ("A,B,C")
"""
import argparse
import os
import sys
import time
from string import Template

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from spcs_oracle import Observation, evaluate  # noqa: E402

DB, SCHEMA = "KC_TEST", "KC"
STAGE = "%s.%s.HARNESS_STAGE" % (DB, SCHEMA)
POOL = "KC_POOL"
ROLE = "KC_SPCS_TEST"
WAREHOUSE = "KC_WH"
POLICY_FOR_CELL = {"A": None, "B": "KC_NP_WITH_POOL", "C": "KC_NP_WITHOUT_POOL"}


def log(msg):
    print("[spcs-release %s] %s" % (time.strftime("%H:%M:%S"), msg), flush=True)


def connect():
    import snowflake.connector
    from cryptography.hazmat.primitives import serialization

    pw = os.environ.get("SPCS_PRIVATE_KEY_PASSPHRASE")
    with open(os.environ["SPCS_PRIVATE_KEY_FILE"], "rb") as f:
        key = serialization.load_pem_private_key(f.read(), pw.encode() if pw else None)
    der = key.private_bytes(serialization.Encoding.DER,
                            serialization.PrivateFormat.PKCS8,
                            serialization.NoEncryption())
    kw = dict(account=os.environ["SPCS_ACCOUNT"], user=os.environ["SPCS_USER"],
              private_key=der, role=ROLE, warehouse=WAREHOUSE,
              database=DB, schema=SCHEMA)
    if os.environ.get("SPCS_HOST"):
        kw["host"] = os.environ["SPCS_HOST"]
    return snowflake.connector.connect(**kw)


def q(conn, sql, role=None):
    cur = conn.cursor()
    try:
        if role:
            cur.execute("USE ROLE " + role)
        cur.execute(sql)
        return cur.fetchall()
    finally:
        if role:
            cur.execute("USE ROLE " + ROLE)
        cur.close()


def set_policy(conn, policy):
    if policy:
        q(conn, "ALTER ACCOUNT SET NETWORK_POLICY = " + policy, role="ACCOUNTADMIN")
    else:
        q(conn, "ALTER ACCOUNT UNSET NETWORK_POLICY", role="ACCOUNTADMIN")
    log("account network policy = %s" % (policy or "<none>"))


def upload_harness(conn):
    for path in (os.environ["KC_JAR"], os.environ["KAFKA_TGZ"],
                 os.path.join(HERE, "run-e2e.sh")):
        if not os.path.isfile(path):
            raise SystemExit("missing input file: " + path)
    # Fixed names on stage: the harness reads kc.jar / kafka.tgz / run-e2e.sh.
    import shutil, tempfile
    tmp = tempfile.mkdtemp()
    for src, dst in ((os.environ["KC_JAR"], "kc.jar"),
                     (os.environ["KAFKA_TGZ"], "kafka.tgz"),
                     (os.path.join(HERE, "run-e2e.sh"), "run-e2e.sh")):
        shutil.copy(src, os.path.join(tmp, dst))
        q(conn, "PUT 'file://%s' @%s AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
          % (os.path.join(tmp, dst), STAGE))
    shutil.rmtree(tmp)
    log("harness uploaded to @%s" % STAGE)


def render_spec(table, nrecords, timeout_secs):
    with open(os.path.join(HERE, "job.yaml")) as f:
        return Template(f.read()).substitute(
            IMAGE=os.environ["SPCS_IMAGE"], STAGE="@" + STAGE, TABLE=table,
            NRECORDS=nrecords, TIMEOUT_SECS=timeout_secs, ROLE=ROLE)


def job_status(conn, job):
    """Terminal status of the job service, or None if it cannot be read."""
    try:
        cur = conn.cursor()
        cur.execute("DESCRIBE SERVICE " + job)
        cols = [c[0].lower() for c in cur.description]
        row = cur.fetchone()
        cur.close()
        return str(row[cols.index("status")]).upper() if row else None
    except Exception as e:  # noqa: BLE001
        log("could not read status of %s: %s" % (job, e))
        return None


def job_logs(conn, job):
    try:
        rows = q(conn, "SELECT SYSTEM$GET_SERVICE_LOGS('%s', 0, 'e2e', 1000)" % job)
        return rows[0][0] or ""
    except Exception as e:  # noqa: BLE001
        log("could not read logs of %s: %s" % (job, e))
        return ""


def run_cell(conn, cell, attempt, nrecords, timeout_secs):
    suffix = "%s_%d_%d" % (cell, int(time.time()), attempt)
    table = "KC_SPCS_REL_" + suffix
    job = "%s.%s.KC_SPCS_JOB_%s" % (DB, SCHEMA, suffix)
    set_policy(conn, POLICY_FOR_CELL[cell])
    q(conn, "CREATE OR REPLACE TABLE %s (RECORD_METADATA VARIANT, ID NUMBER, NAME VARCHAR)"
      " ENABLE_SCHEMA_EVOLUTION = TRUE" % table)
    spec = render_spec(table, nrecords, timeout_secs)
    log("cell %s attempt %d: EXECUTE JOB SERVICE %s" % (cell, attempt, job))
    try:
        # Synchronous: returns when the job ends; raises if it ends FAILED.
        q(conn, "EXECUTE JOB SERVICE IN COMPUTE POOL %s NAME = %s "
          "FROM SPECIFICATION $$\n%s\n$$" % (POOL, job, spec))
    except Exception as e:  # noqa: BLE001
        log("cell %s: job raised (expected for cell C): %s" % (cell, str(e)[:300]))
    status = job_status(conn, job)
    logs = job_logs(conn, job)
    rows = q(conn, "SELECT COUNT(*) FROM " + table)[0][0]
    obs = Observation(cell, int(rows), status, logs)
    verdict = evaluate(obs, nrecords)
    log("cell %s attempt %d: rows=%d status=%s -> %s %s" % (
        cell, attempt, obs.rows, status, "PASS" if verdict.passed else "FAIL",
        "; ".join(verdict.reasons)))
    if not verdict.passed:
        print("----- logs %s -----\n%s\n-----" % (job, logs[-8000:]), flush=True)
    for sql in ("DROP SERVICE IF EXISTS " + job, "DROP TABLE IF EXISTS " + table):
        try:
            q(conn, sql)
        except Exception as e:  # noqa: BLE001
            log("cleanup failed (%s): %s" % (sql, e))
    return verdict


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--cells", default=os.environ.get("CELLS", "A,B,C"))
    ap.add_argument("--nrecords", type=int, default=int(os.environ.get("NRECORDS", 1000)))
    ap.add_argument("--timeout-secs", type=int,
                    default=int(os.environ.get("TIMEOUT_SECS", 600)))
    ap.add_argument("--retries", type=int, default=1)
    args = ap.parse_args(argv)
    cells = [c.strip().upper() for c in args.cells.split(",") if c.strip()]
    bad = [c for c in cells if c not in POLICY_FOR_CELL]
    if bad:
        raise SystemExit("unknown cells: %s" % bad)

    conn = connect()
    results = {}
    try:
        upload_harness(conn)
        for cell in cells:
            for attempt in range(1, args.retries + 2):
                v = run_cell(conn, cell, attempt, args.nrecords, args.timeout_secs)
                results[cell] = (v, attempt)
                if v.passed:
                    break
    finally:
        try:
            set_policy(conn, None)
        finally:
            conn.close()

    print("\n| cell | result | attempts | reasons |\n|---|---|---|---|")
    for cell in cells:
        v, n = results.get(cell, (None, 0))
        ok = bool(v and v.passed)
        print("| %s | %s | %d | %s |" % (cell, "PASS" if ok else "FAIL", n,
                                         "; ".join(v.reasons) if v else "not run"))
    failed = [c for c in cells if not (results.get(c) and results[c][0].passed)]
    if failed:
        log("FAILED cells: %s" % ",".join(failed))
        return 1
    log("all cells passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
