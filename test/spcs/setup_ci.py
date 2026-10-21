"""One-time setup of the fixed KC SPCS CI fixture.

`check` (default) only reads. `apply` creates missing objects with IF NOT EXISTS.
Never alters, drops, or grants, so rerunning is always safe.
"""

import argparse
import os
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from lib.config import Profile, SnowflakeConnectorConfig  # noqa: E402
import snowflake.connector  # noqa: E402

ACCOUNT = "SFCTEST0"
ROLE = "TESTROLE_KAFKA"
DATABASE = "TESTDB_KAFKA"
SCHEMA = f"{DATABASE}.KC_SPCS_CI"
MARKER = "Kafka Connector SPCS validation: permanent CI infrastructure"

# (kind, name, SHOW statement, CREATE options). Pool first: if account-level
# creation is denied, nothing else is created. Only one node can ever run.
OBJECTS = (
    (
        "COMPUTE POOL",
        "KC_SPCS_CI_POOL",
        "SHOW COMPUTE POOLS LIKE 'KC_SPCS_CI_POOL'",
        "MIN_NODES=1 MAX_NODES=1 INSTANCE_FAMILY=GEN_X64_G2_4 AUTO_RESUME=TRUE "
        "INITIALLY_SUSPENDED=TRUE AUTO_SUSPEND_SECS=300",
    ),
    ("SCHEMA", SCHEMA, f"SHOW SCHEMAS LIKE 'KC_SPCS_CI' IN DATABASE {DATABASE}", ""),
    (
        "STAGE",
        f"{SCHEMA}.HARNESS_STAGE",
        f"SHOW STAGES LIKE 'HARNESS_STAGE' IN SCHEMA {SCHEMA}",
        "ENCRYPTION=(TYPE='SNOWFLAKE_SSE')",
    ),
    (
        "IMAGE REPOSITORY",
        f"{SCHEMA}.IMAGES",
        f"SHOW IMAGE REPOSITORIES LIKE 'IMAGES' IN SCHEMA {SCHEMA}",
        "",
    ),
)


def rows(cursor, sql):
    cursor.execute(sql)
    keys = [column[0].lower() for column in cursor.description]
    return [dict(zip(keys, row)) for row in cursor.fetchall()]


def find(cursor, name, show):
    # LIKE treats "_" as a wildcard, so require an exact name match.
    short = name.rsplit(".", 1)[-1]
    found = [row for row in rows(cursor, show) if row["name"] == short]
    if found and (found[0]["owner"], found[0]["comment"]) != (ROLE, MARKER):
        raise RuntimeError(f"{name} exists but is not owned by this fixture")
    return bool(found)


def setup(cursor, apply):
    """Return the number of objects still missing."""
    (identity,) = rows(cursor, "SELECT CURRENT_ACCOUNT() AS A, CURRENT_ROLE() AS R")
    if (identity["a"], identity["r"]) != (ACCOUNT, ROLE):
        raise RuntimeError(f"connected as {identity}, expected {ACCOUNT}/{ROLE}")
    missing = 0
    schema_exists = True
    for kind, name, show, options in OBJECTS:
        exists = (kind in ("SCHEMA", "COMPUTE POOL") or schema_exists) and find(
            cursor, name, show
        )
        if not exists and apply:
            cursor.execute(
                f"CREATE {kind} IF NOT EXISTS {name} {options} COMMENT='{MARKER}'"
            )
            exists = find(cursor, name, show)
        print(f"{kind} {name}: {'present' if exists else 'missing'}", flush=True)
        missing += not exists
        if kind == "SCHEMA":
            schema_exists = exists
    return missing


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "action", choices=("check", "apply"), nargs="?", default="check"
    )
    args = parser.parse_args(argv)
    profile = Profile.load(Path(os.environ["SNOWFLAKE_CREDENTIAL_FILE"]))
    params = SnowflakeConnectorConfig.from_profile(profile).to_dict()
    params.update(role=ROLE, login_timeout=30, network_timeout=60)
    try:
        with snowflake.connector.connect(**params) as connection:
            with connection.cursor() as cursor:
                cursor.execute("USE SECONDARY ROLES NONE")
                missing = setup(cursor, args.action == "apply")
    except (snowflake.connector.Error, RuntimeError) as error:
        # Snowflake errors carry the query ID for follow-up; never print credentials.
        print(f"ERROR: {type(error).__name__}: {error}", flush=True)
        return 1
    return 3 if missing else 0


if __name__ == "__main__":
    sys.exit(main())
