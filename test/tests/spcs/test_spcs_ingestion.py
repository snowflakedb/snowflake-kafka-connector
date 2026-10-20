"""One positive KC v4 ingestion check using the existing E2E helpers."""

import pytest

from lib.config_migration import V4_CONNECTOR_CLASS
from lib.utils import RecordProducer


@pytest.mark.spcs
@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
def test_spcs_ingestion(
    connector_version, driver, name_salt, create_connector, wait_for_rows
):
    # Check the caller-owned fixture before creating a connector or sending records.
    try:
        with driver.snowflake_conn.cursor() as cursor:
            cursor.execute('SELECT RECORD_METADATA, "number" FROM SMOKE_ROWS LIMIT 1')
            if cursor.fetchone() is not None:
                pytest.fail("SMOKE_ROWS must be empty; use a fresh isolated schema")
    except Exception:
        pytest.fail(
            "SPCS fixture requires an accessible SMOKE_ROWS table with "
            'RECORD_METADATA VARIANT and "number" VARCHAR; see test/spcs/README.md',
            pytrace=False,
        )
    topic = f"spcs_smoke{name_salt}"
    config = {
        "connector.class": V4_CONNECTOR_CLASS,
        "topics": topic,
        "tasks.max": "1",
        "snowflake.topic2table.map": f"{topic}:SMOKE_ROWS",
        "snowflake.authenticator": "spcs",
        # KC's streaming validator requires a role; SPCS still uses the service owner.
        "snowflake.role.name": driver.snowflake_conn.cursor()
        .execute("SELECT CURRENT_ROLE()")
        .fetchone()[0],
        "snowflake.streaming.validate.compatibility.with.classic": "false",
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
    }
    connector = create_connector(v4_config=config)
    try:
        driver.wait_for_connector_running(connector.name)
        RecordProducer(driver, topic).send(100)
        wait_for_rows("SMOKE_ROWS", 100, connector_name=connector.name)
        values = (
            driver.snowflake_conn.cursor()
            .execute('SELECT "number" FROM SMOKE_ROWS')
            .fetchall()
        )
        assert sorted(row[0] for row in values) == sorted(
            str(value) for value in range(1, 101)
        )
    finally:
        if not connector.close(wait_timeout=30):
            pytest.fail("Connector did not stop cleanly")
        driver.deleteTopic(topic)
