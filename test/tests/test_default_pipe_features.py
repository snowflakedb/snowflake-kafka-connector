"""E2E tests for FR7 default pipe features: identity and default columns.

These tests verify that the Kafka Connector correctly handles tables with
AUTOINCREMENT (identity) columns and columns with DEFAULT values. The
primary concern is client-side validation: the RowValidator must not reject
records that omit server-filled columns.

v4-only — no v3 equivalent (FR7 requires SSv2 default pipe support).
"""

import json
import logging

import pytest

from lib.config_migration import V4_CONFIG_TEMPLATE
from lib.driver import KafkaDriver

logger = logging.getLogger(__name__)

RECORD_COUNT = 20


def _connector_config(topic: str, *, validation: bool) -> dict:
    """Build a v4 connector config for default pipe feature tests."""
    return {
        **V4_CONFIG_TEMPLATE,
        "tasks.max": "1",
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
        "snowflake.enable.schematization": "true",
        "snowflake.validation": "client_side" if validation else "server_side",
        "snowflake.compatibility.enable.column.identifier.normalization": "true",
        "topics": topic,
    }


@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
@pytest.mark.parametrize("validation", [True, False], ids=["compat", "ht"])
def test_identity_column(
    driver: KafkaDriver,
    create_table,
    create_topics,
    create_connector,
    wait_for_rows,
    validation: bool,
):
    """Ingest into a table with an AUTOINCREMENT identity column.

    The record does NOT include a value for the identity column.
    The server should auto-fill sequential IDs.
    """
    tag = "compat" if validation else "ht"
    base_name = f"fr7_identity_{tag}"

    table = create_table(
        base_name,
        columns=(
            "(ID NUMBER AUTOINCREMENT START 1 INCREMENT 1, "
            "RECORD_METADATA VARIANT, "
            "DATA VARCHAR)"
        ),
        cleanup_topic=False,
    )
    topic = create_topics([base_name], with_tables=False)[0]

    create_connector(v4_config=_connector_config(topic, validation=validation))
    driver.startConnectorWaitTime()

    records = [
        json.dumps({"data": f"row_{i}"}).encode("utf-8") for i in range(RECORD_COUNT)
    ]
    driver.sendBytesData(topic, records, partition=0)

    wait_for_rows(table.name, RECORD_COUNT)

    rows = table.select('"ID", "DATA"', 'ORDER BY "ID"')
    assert len(rows) == RECORD_COUNT
    ids = [row["ID"] for row in rows]
    assert ids == list(range(1, RECORD_COUNT + 1)), (
        f"Expected sequential IDs, got {ids}"
    )
    assert rows[0]["DATA"] == "row_0"


@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
@pytest.mark.parametrize("validation", [True, False], ids=["compat", "ht"])
def test_default_timestamp_column(
    driver: KafkaDriver,
    create_table,
    create_topics,
    create_connector,
    wait_for_rows,
    validation: bool,
):
    """Ingest into a table with a DEFAULT CURRENT_TIMESTAMP() NOT NULL column.

    The record does NOT include a value for the timestamp column.
    The server should auto-fill the current timestamp.
    """
    tag = "compat" if validation else "ht"
    base_name = f"fr7_defts_{tag}"

    table = create_table(
        base_name,
        columns=(
            "(RECORD_METADATA VARIANT, "
            "DATA VARCHAR, "
            "CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() NOT NULL)"
        ),
        cleanup_topic=False,
    )
    topic = create_topics([base_name], with_tables=False)[0]

    create_connector(v4_config=_connector_config(topic, validation=validation))
    driver.startConnectorWaitTime()

    records = [
        json.dumps({"data": f"row_{i}"}).encode("utf-8") for i in range(RECORD_COUNT)
    ]
    driver.sendBytesData(topic, records, partition=0)

    wait_for_rows(table.name, RECORD_COUNT)

    rows = table.select('"DATA", "CREATED_AT"', "LIMIT 1")
    assert rows, "Expected at least one row"
    assert rows[0]["CREATED_AT"] is not None, (
        "CREATED_AT should be filled by server default"
    )
    assert rows[0]["DATA"] == "row_0"


@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
@pytest.mark.parametrize("validation", [True, False], ids=["compat", "ht"])
def test_default_numeric_column(
    driver: KafkaDriver,
    create_table,
    create_topics,
    create_connector,
    wait_for_rows,
    validation: bool,
):
    """Ingest into a table with a DEFAULT 0 NOT NULL numeric column.

    The record does NOT include a value for the status column.
    The server should auto-fill with the default value 0.
    """
    tag = "compat" if validation else "ht"
    base_name = f"fr7_defnum_{tag}"

    table = create_table(
        base_name,
        columns=(
            "(RECORD_METADATA VARIANT, DATA VARCHAR, STATUS NUMBER DEFAULT 0 NOT NULL)"
        ),
        cleanup_topic=False,
    )
    topic = create_topics([base_name], with_tables=False)[0]

    create_connector(v4_config=_connector_config(topic, validation=validation))
    driver.startConnectorWaitTime()

    records = [
        json.dumps({"data": f"row_{i}"}).encode("utf-8") for i in range(RECORD_COUNT)
    ]
    driver.sendBytesData(topic, records, partition=0)

    wait_for_rows(table.name, RECORD_COUNT)

    rows = table.select('"DATA", "STATUS"', "LIMIT 1")
    assert rows, "Expected at least one row"
    assert rows[0]["STATUS"] == 0, (
        f"STATUS should be 0 (server default), got {rows[0]['STATUS']}"
    )
    assert rows[0]["DATA"] == "row_0"


@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
@pytest.mark.parametrize("validation", [True, False], ids=["compat", "ht"])
def test_mixed_identity_and_defaults(
    driver: KafkaDriver,
    create_table,
    create_topics,
    create_connector,
    wait_for_rows,
    validation: bool,
):
    """Ingest into a table with identity + default + regular columns.

    Only the DATA column is populated by the record. The server fills:
    - ID: auto-increment
    - CREATED_AT: CURRENT_TIMESTAMP()
    - STATUS: default 1
    """
    tag = "compat" if validation else "ht"
    base_name = f"fr7_mixed_{tag}"

    table = create_table(
        base_name,
        columns=(
            "(ID NUMBER AUTOINCREMENT, "
            "RECORD_METADATA VARIANT, "
            "DATA VARCHAR, "
            "CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() NOT NULL, "
            "STATUS NUMBER DEFAULT 1 NOT NULL)"
        ),
        cleanup_topic=False,
    )
    topic = create_topics([base_name], with_tables=False)[0]

    create_connector(v4_config=_connector_config(topic, validation=validation))
    driver.startConnectorWaitTime()

    records = [
        json.dumps({"data": f"row_{i}"}).encode("utf-8") for i in range(RECORD_COUNT)
    ]
    driver.sendBytesData(topic, records, partition=0)

    wait_for_rows(table.name, RECORD_COUNT)

    rows = table.select('"ID", "DATA", "CREATED_AT", "STATUS"', 'ORDER BY "ID" LIMIT 5')
    assert len(rows) >= 1
    row = rows[0]
    assert row["ID"] == 1, f"Expected ID=1, got {row['ID']}"
    assert row["DATA"] == "row_0"
    assert row["CREATED_AT"] is not None, "CREATED_AT should be filled by default"
    assert row["STATUS"] == 1, f"STATUS should be 1 (default), got {row['STATUS']}"
