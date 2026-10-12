"""Create permanent AWS KC SPCS resources after merge. No jobs or grants."""

import json
import os
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from lib.config import Profile, SnowflakeConnectorConfig  # noqa: E402
import snowflake.connector  # noqa: E402


def query(connection, statement):
    with connection.cursor() as cursor:
        cursor.execute(statement)
        keys = [column[0].lower() for column in cursor.description]
        return [dict(zip(keys, row)) for row in cursor.fetchall()]


ACCOUNT = "SFCTEST0"
DATABASE = "TESTDB_KAFKA"
ROLE = "TESTROLE_KAFKA"
SCHEMA = "KC_SPCS_CI"
POOL = "KC_SPCS_CI_POOL"
STAGE = f"{DATABASE}.{SCHEMA}.HARNESS_STAGE"
REPOSITORY = f"{DATABASE}.{SCHEMA}.IMAGES"
MARKER = "Kafka Connector SPCS validation: permanent CI infrastructure"
WORKFLOW = "snowflakedb/snowflake-kafka-connector/.github/workflows/spcs-infrastructure.yml@refs/heads/master"


def emit(report):
    # Only fixed labels and sanitized codes belong in public CI output.
    print(json.dumps(report, sort_keys=True), flush=True)


def require_merged_dispatch():
    expected = {
        "GITHUB_ACTIONS": "true",
        "GITHUB_EVENT_NAME": "workflow_dispatch",
        "GITHUB_REF": "refs/heads/master",
        "GITHUB_REPOSITORY": "snowflakedb/snowflake-kafka-connector",
        "GITHUB_WORKFLOW_REF": WORKFLOW,
        "KC_SPCS_CONFIRM_SETUP": "true",
    }
    if any(os.environ.get(key) != value for key, value in expected.items()):
        raise ValueError("Use the confirmed manual setup workflow on merged master")


def ensure(connection, report, *, kind, name, show, create):
    """Never replace or adopt a colliding object, even if its name matches."""
    report["phase"] = kind

    def matches():
        return [row for row in query(connection, show) if row["name"] == name]

    rows = matches()
    if not rows:
        # DDL commits independently. A lost response may still mean creation succeeded.
        report["resources"][kind] = "create_submitted"
        emit(report)
        query(connection, create)
        report["resources"][kind] = "created"
        emit(report)
        rows = matches()
    if (
        len(rows) != 1
        or rows[0].get("owner") != ROLE
        or rows[0].get("comment") != MARKER
    ):
        raise ValueError("Ownership or purpose mismatch; no existing object changed")
    report["resources"][kind] = "ownership_verified"
    return rows[0]


def setup(connection, report):
    report["phase"] = "identity"
    identity = query(
        connection,
        "SELECT CURRENT_ACCOUNT() AS ACCOUNT, CURRENT_ROLE() AS ROLE, "
        "CURRENT_REGION() AS REGION",
    )[0]
    if (
        identity["account"] != ACCOUNT
        or identity["role"] != ROLE
        or identity["region"] != "AWS_US_WEST_2"
    ):
        raise ValueError("Setup requires the existing AWS KC CI account and role")

    report["phase"] = "instance_families"
    families = query(connection, "SHOW COMPUTE POOL INSTANCE FAMILIES")
    candidates = [
        row
        for row in families
        if row["name"] in ("CPU_X64_XS", "GEN_X64_G2_4")
        # The workload's 6 GiB limit plus the stage-volume sidecar's 1 GiB limit.
        and float(row["memory_gib"]) >= 7
        and int(row["gpu_count"]) == 0
    ]
    if not candidates:
        raise ValueError("No allowed small x64 instance family with at least 7 GiB")
    family = min(
        candidates, key=lambda row: (float(row["memory_gib"]), float(row["vcpu"]))
    )["name"]
    # Pool first: missing account-level privilege must not leave schema objects behind.
    pool = ensure(
        connection,
        report,
        kind="pool",
        name=POOL,
        show=f"SHOW COMPUTE POOLS LIKE '{POOL}'",
        create=f"CREATE COMPUTE POOL {POOL} MIN_NODES=1 MAX_NODES=1 "
        f"INSTANCE_FAMILY={family} AUTO_RESUME=TRUE INITIALLY_SUSPENDED=TRUE "
        f"AUTO_SUSPEND_SECS=300 COMMENT='{MARKER}'",
    )
    if (
        int(pool["min_nodes"]) != 1
        or int(pool["max_nodes"]) != 1
        or pool["instance_family"] not in {row["name"] for row in candidates}
        or str(pool["auto_resume"]).lower() != "true"
        or int(pool["auto_suspend_secs"]) != 300
        or pool.get("backup_instance_families") not in (None, "", "[]", [])
    ):
        raise ValueError("Existing pool differs; review drift before changing it")
    if (
        pool["state"] != "SUSPENDED"
        or int(pool["num_jobs"])
        or int(pool["num_services"])
    ):
        raise ValueError("Pool is busy or not suspended; setup will not interrupt it")
    report["resources"]["pool"] = "verified"

    ensure(
        connection,
        report,
        kind="schema",
        name=SCHEMA,
        show=f"SHOW SCHEMAS LIKE '{SCHEMA}' IN DATABASE {DATABASE}",
        create=f"CREATE SCHEMA {DATABASE}.{SCHEMA} COMMENT='{MARKER}'",
    )
    report["resources"]["schema"] = "verified"
    stage = ensure(
        connection,
        report,
        kind="stage",
        name="HARNESS_STAGE",
        show=f"SHOW STAGES LIKE 'HARNESS_STAGE' IN SCHEMA {DATABASE}.{SCHEMA}",
        create=f"CREATE STAGE {STAGE} ENCRYPTION=(TYPE='SNOWFLAKE_SSE') "
        f"COMMENT='{MARKER}'",
    )
    # SHOW STAGES distinguishes SSE from the default client-side-encrypted INTERNAL.
    if stage.get("type") != "INTERNAL NO CSE":
        raise ValueError("Payload stage must be permanent internal with SSE encryption")
    report["resources"]["stage"] = "verified"
    ensure(
        connection,
        report,
        kind="repository",
        name="IMAGES",
        show=f"SHOW IMAGE REPOSITORIES LIKE 'IMAGES' IN SCHEMA {DATABASE}.{SCHEMA}",
        create=f"CREATE IMAGE REPOSITORY {REPOSITORY} COMMENT='{MARKER}'",
    )
    report["resources"]["repository"] = "verified"
    report.update(
        setup_complete=True, phase="complete", image_publication_required=True
    )


def main():
    report = {"setup_complete": False, "phase": "dispatch_guard", "resources": {}}
    try:
        require_merged_dispatch()
        report["phase"] = "load_profile"
        profile = Profile.load(Path(os.environ["SNOWFLAKE_CREDENTIAL_FILE"]))
        if (profile.role or "").upper() != ROLE or (
            profile.database or ""
        ).upper() != DATABASE:
            raise ValueError("Unexpected CI profile")
        params = SnowflakeConnectorConfig.from_profile(profile).to_dict()
        params.update(
            role=ROLE,
            login_timeout=30,
            network_timeout=60,
            session_parameters={"STATEMENT_TIMEOUT_IN_SECONDS": 60},
        )
        report["phase"] = "connect"
        with snowflake.connector.connect(**params) as connection:
            report["phase"] = "secondary_roles"
            query(connection, "USE SECONDARY ROLES NONE")
            setup(connection, report)
    except Exception as error:
        report.update(
            setup_complete=False,
            error_type=type(error).__name__,
            errno=getattr(error, "errno", None),
            sqlstate=getattr(error, "sqlstate", None),
        )
        emit(report)
        return 1
    emit(report)
    return 0


if __name__ == "__main__":
    sys.exit(main())
