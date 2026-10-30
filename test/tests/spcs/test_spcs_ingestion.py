"""KC v4 ingestion checks under SPCS auth, one per snowflake.role.name variant."""

import os

import pytest

from lib.config_migration import V4_CONNECTOR_CLASS
from lib.utils import RecordProducer

# Chosen by the host runner (--role-variants); OWNER alone is the historical smoke test.
#   OWNER  - role set to the service owner role
#   ABSENT - no role, as in the documented minimal SPCS config (SNOW-4392681)
#   PUBLIC - a role without table grants; passes only if SPCS really overrides it
ROLE_VARIANTS = [
    v.strip().upper()
    for v in os.environ.get("SPCS_ROLE_VARIANTS", "OWNER").split(",")
    if v.strip()
]


def table_for(variant):
    return "SMOKE_ROWS" if variant == "OWNER" else f"SMOKE_ROWS_{variant}"


@pytest.mark.spcs
@pytest.mark.parametrize("role_variant", ROLE_VARIANTS)
@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
def test_spcs_ingestion(
    connector_version, role_variant, driver, name_salt, create_connector, wait_for_rows
):
    table = table_for(role_variant)
    # Check the caller-owned fixture before creating a connector or sending records.
    try:
        with driver.snowflake_conn.cursor() as cursor:
            cursor.execute(f'SELECT RECORD_METADATA, "number" FROM {table} LIMIT 1')
            if cursor.fetchone() is not None:
                pytest.fail(f"{table} must be empty; use a fresh isolated schema")
    except Exception:
        pytest.fail(
            f"SPCS fixture requires an accessible {table} table with "
            'RECORD_METADATA VARIANT and "number" VARCHAR; see test/spcs/README.md',
            pytrace=False,
        )
    topic = f"spcs_smoke{name_salt}_{role_variant.lower()}"
    config = {
        "connector.class": V4_CONNECTOR_CLASS,
        "topics": topic,
        "tasks.max": "1",
        "snowflake.topic2table.map": f"{topic}:{table}",
        "snowflake.authenticator": "spcs",
        "snowflake.streaming.validate.compatibility.with.classic": "false",
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
    }
    if role_variant == "OWNER":
        config["snowflake.role.name"] = (
            driver.snowflake_conn.cursor()
            .execute("SELECT CURRENT_ROLE()")
            .fetchone()[0]
        )
    elif role_variant == "PUBLIC":
        config["snowflake.role.name"] = "PUBLIC"
    connector = create_connector(v4_config=config)
    try:
        driver.wait_for_connector_running(connector.name)
        RecordProducer(driver, topic).send(100)
        wait_for_rows(table, 100, connector_name=connector.name)
        values = (
            driver.snowflake_conn.cursor()
            .execute(f'SELECT "number" FROM {table}')
            .fetchall()
        )
        assert sorted(row[0] for row in values) == sorted(
            str(value) for value in range(1, 101)
        )
    finally:
        if not connector.close(wait_timeout=30):
            pytest.fail("Connector did not stop cleanly")
        driver.deleteTopic(topic)
