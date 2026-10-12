"""Read-only SPCS discovery using the existing KC CI profile; never print secrets."""

import json
import os
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from lib.config import Profile, SnowflakeConnectorConfig  # noqa: E402
import snowflake.connector  # noqa: E402


def quoted(value):
    if not isinstance(value, str) or not value or len(value) > 255:
        raise ValueError("Invalid identifier")
    return '"' + value.replace('"', '""') + '"'


def query(connection, statement):
    with connection.cursor() as cursor:
        cursor.execute(statement)
        keys = [column[0].lower() for column in cursor.description]
        return [dict(zip(keys, row)) for row in cursor.fetchall()]


def inspect(connection, profile):
    identity = query(
        connection,
        "SELECT CURRENT_ACCOUNT() AS ACCOUNT, CURRENT_ROLE() AS ROLE, CURRENT_DATABASE() AS DATABASE",
    )[0]
    result = {
        "database_matches_profile": identity["database"] == profile.database.upper(),
        "role_matches_profile": identity["role"] == profile.role.upper(),
        "read_only": True,
    }
    if not all((result["database_matches_profile"], result["role_matches_profile"])):
        raise ValueError("Unexpected CI identity")
    pending = [identity["role"], "PUBLIC"]
    visited = set()
    grants = []
    while pending:
        role = pending.pop()
        if role in visited:
            continue
        if len(visited) >= 50:
            raise ValueError("Role hierarchy exceeds inspection bound")
        visited.add(role)
        rows = query(connection, f"SHOW GRANTS TO ROLE {quoted(role)}")
        grants.extend(rows)
        for row in rows:
            if row.get("granted_on") == "ROLE" and row.get("privilege") == "USAGE":
                pending.append(row["name"])
    result["roles_inspected"] = len(visited)
    result["database_owned_by_role_hierarchy"] = any(
        row.get("granted_on") == "DATABASE"
        and row.get("name") == profile.database.upper()
        and row.get("privilege") == "OWNERSHIP"
        for row in grants
    )
    result["account_create_compute_pool_granted"] = any(
        row.get("granted_on") == "ACCOUNT"
        and row.get("privilege") == "CREATE COMPUTE POOL"
        for row in grants
    )
    database = quoted(profile.database)
    for label, statement in (
        ("compute_pools", "SHOW COMPUTE POOLS"),
        ("image_repositories", f"SHOW IMAGE REPOSITORIES IN DATABASE {database}"),
        ("stages", f"SHOW STAGES IN DATABASE {database}"),
    ):
        try:
            rows = query(connection, statement)
            result[label] = {"visible_count": len(rows), "inspection_succeeded": True}
        except snowflake.connector.Error as error:
            result[label] = {"inspection_succeeded": False, "errno": error.errno}
    # Counts and capability flags only: no account names, grants, URLs, or profiles in public CI logs.
    return result


def main():
    phase = "load_profile"
    try:
        profile = Profile.load(Path(os.environ["SNOWFLAKE_CREDENTIAL_FILE"]))
        if not profile.role or not profile.database:
            raise ValueError("CI profile must specify role and database")
        parameters = SnowflakeConnectorConfig.from_profile(profile).to_dict()
        parameters.update(
            role=profile.role,
            login_timeout=30,
            network_timeout=30,
            session_parameters={"STATEMENT_TIMEOUT_IN_SECONDS": 30},
        )
        phase = "connect"
        with snowflake.connector.connect(**parameters) as connection:
            # Session scope only; do not inherit unrelated default secondary roles.
            phase = "secondary_roles"
            query(connection, "USE SECONDARY ROLES NONE")
            phase = "inspect"
            print(json.dumps(inspect(connection, profile), sort_keys=True))
    except Exception as error:
        print(
            json.dumps(
                {
                    "inspection_succeeded": False,
                    "error_type": type(error).__name__,
                    "phase": phase,
                    "errno": getattr(error, "errno", None),
                    "sqlstate": getattr(error, "sqlstate", None),
                }
            )
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
