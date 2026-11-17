"""E2E tests for server-side schema evolution with Kafka Connect schemas.

With snowflake.feature.evolve.schema.enabled=true and server_side validation, the connector
sends each new value schema to the SDK's evolveSchema before appending. Columns are created
with the types the schema declares, not types guessed from JSON values.

Records are JSON with an embedded schema (JsonConverter schemas.enable=true), so the connector
has a Kafka Connect schema to send. Evolve-schema is KC v4 only.
"""

import base64
import json
from datetime import datetime
from pathlib import Path
from typing import Callable

import pytest

from lib.driver import KafkaDriver

pytestmark = [
    pytest.mark.schema_evolution,
    pytest.mark.parametrize("connector_version", ["v4"], indirect=True),
]

TEMPLATE_DIR = Path("rest_request_template")
BASE_TEMPLATE = "datatype_ingestion.json"

# Kafka Connect logical type names, as JsonConverter writes them into the schema envelope.
TIMESTAMP_NAME = "org.apache.kafka.connect.data.Timestamp"
DECIMAL_NAME = "org.apache.kafka.connect.data.Decimal"

BASE_MS = 1700000000000  # 2023-11-14 22:13:20 UTC

TYPED_SCHEMA = {
    "type": "struct",
    "name": "Value",
    "optional": False,
    "fields": [
        {"field": "ID", "type": "int64", "optional": False},
        {"field": "SMALL", "type": "int16", "optional": True},
        {
            "field": "EVENT_TIME",
            "type": "int64",
            "optional": True,
            "name": TIMESTAMP_NAME,
            "version": 1,
        },
    ],
}

# A decimal precision above 38 is one Snowflake can't store, so Snowflake declines the schema.
UNSUPPORTED_SCHEMA = {
    "type": "struct",
    "name": "Value",
    "optional": False,
    "fields": [
        {"field": "ID", "type": "int64", "optional": False},
        {
            "field": "AMOUNT",
            "type": "bytes",
            "optional": True,
            "name": DECIMAL_NAME,
            "version": 1,
            "parameters": {"scale": "2", "connect.decimal.precision": "39"},
        },
    ],
}

TYPED_COLUMNS = {
    "RECORD_METADATA": "VARIANT",
    "ID": "NUMBER(19,0)",
    "SMALL": "NUMBER(5,0)",
    "EVENT_TIME": "TIMESTAMP_NTZ(3)",
}


def _config(dlq_topic: str = None) -> dict:
    """A server_side v4 config with evolve-schema on, from the base template."""
    config = dict(json.loads((TEMPLATE_DIR / BASE_TEMPLATE).read_text())["config"])
    config["value.converter.schemas.enable"] = "true"
    config["snowflake.enable.schematization"] = "true"
    config["snowflake.validation"] = "server_side"
    config["snowflake.feature.evolve.schema.enabled"] = "true"
    config["snowflake.compatibility.enable.column.identifier.normalization"] = "true"
    config["snowflake.streaming.validate.compatibility.with.classic"] = "false"
    if dlq_topic:
        config["errors.tolerance"] = "all"
        config["errors.deadletterqueue.topic.name"] = dlq_topic
        config["errors.deadletterqueue.topic.replication.factor"] = "1"
    return config


def _envelope(schema, payload):
    return json.dumps({"schema": schema, "payload": payload}).encode("utf-8")


def _column_types(table) -> dict:
    return {row[0]: row[1] for row in table.schema()}


def _start(driver: KafkaDriver, create_table, create_custom_connector, name, dlq=False):
    """Creates the table, its topic, and a connector, all named after `name`."""
    table = create_table(
        name, columns="(RECORD_METADATA VARIANT) ENABLE_SCHEMA_EVOLUTION = TRUE"
    )
    driver.createTopics(table.name, partitionNum=1, replicationNum=1)
    dlq_topic = None
    if dlq:
        dlq_topic = f"dlq_{table.name.lower()}"
        driver.createTopics(dlq_topic, partitionNum=1, replicationNum=1)
    connector = create_custom_connector(name, _config(dlq_topic))
    driver.startConnectorWaitTime()
    return table, connector


# create_custom_connector is last in each signature so the connector is torn down before the
# table and its topic.


def test_new_fields_get_schema_declared_types(
    driver: KafkaDriver,
    create_table: Callable,
    wait_for_rows: Callable,
    create_custom_connector: Callable,
):
    """New fields are added with the types the Kafka Connect schema declares.

    SMALL is int16, so NUMBER(5,0). EVENT_TIME is a Timestamp, so TIMESTAMP_NTZ(3). Under
    JSON-value inference, both would have come out as NUMBER(38,0).
    """
    table, connector = _start(
        driver, create_table, create_custom_connector, "evolve_schema_types"
    )

    record_count = 100
    values = [
        _envelope(TYPED_SCHEMA, {"ID": i, "SMALL": i % 100, "EVENT_TIME": BASE_MS + i})
        for i in range(record_count)
    ]
    driver.sendBytesData(table.name, values, [], partition=0)

    wait_for_rows(table.name, record_count, connector_name=connector.name)

    assert _column_types(table) == TYPED_COLUMNS
    rows = table.select(
        '"SMALL", "EVENT_TIME"',
        'WHERE RECORD_METADATA:"offset"::number = 0',
    )
    assert rows, "Expected row with offset 0"
    assert rows[0]["SMALL"] == 0
    assert rows[0]["EVENT_TIME"] == datetime(2023, 11, 14, 22, 13, 20)


def test_second_schema_adds_only_its_new_columns(
    driver: KafkaDriver,
    create_table: Callable,
    wait_for_rows: Callable,
    create_custom_connector: Callable,
):
    """A second schema adds only the column that is missing, with its declared type.

    Wave 1 creates ID, SMALL, EVENT_TIME. Wave 2 adds EXTRA, an int8, so NUMBER(3,0): JSON-value
    inference would give NUMBER(38,0), so this fails if the second call is skipped. The earlier
    columns keep their types, and wave-1 rows read NULL for EXTRA.
    """
    table, connector = _start(
        driver, create_table, create_custom_connector, "evolve_schema_second"
    )

    wave1_count = 50
    wave1 = [
        _envelope(TYPED_SCHEMA, {"ID": i, "SMALL": 1, "EVENT_TIME": BASE_MS + i})
        for i in range(wave1_count)
    ]
    driver.sendBytesData(table.name, wave1, [], partition=0)
    wait_for_rows(table.name, wave1_count, connector_name=connector.name)

    wave2_schema = {
        **TYPED_SCHEMA,
        "fields": TYPED_SCHEMA["fields"]
        + [{"field": "EXTRA", "type": "int8", "optional": True}],
    }
    wave2_count = 50
    wave2 = [
        _envelope(
            wave2_schema,
            {"ID": 1000 + i, "SMALL": 2, "EVENT_TIME": BASE_MS + i, "EXTRA": 7},
        )
        for i in range(wave2_count)
    ]
    driver.sendBytesData(table.name, wave2, [], partition=0)
    wait_for_rows(table.name, wave1_count + wave2_count, connector_name=connector.name)

    assert _column_types(table) == {**TYPED_COLUMNS, "EXTRA": "NUMBER(3,0)"}
    nulls = table.select_scalar(
        "count(*)",
        f'WHERE RECORD_METADATA:"offset"::number < {wave1_count} AND "EXTRA" IS NULL',
    )
    assert nulls == wave1_count, (
        f"Expected {wave1_count} wave-1 rows with NULL EXTRA, got {nulls}"
    )


def test_unsupported_schema_goes_to_dlq(
    driver: KafkaDriver,
    create_table: Callable,
    create_custom_connector: Callable,
):
    """A schema Snowflake declines is routed to the DLQ, not the table.

    The decimal precision of 39 can't be stored in a Snowflake NUMBER, so the evolve-schema call
    fails with SfApiUserError and errors.tolerance=all sends each record to the DLQ.
    """
    table, connector = _start(
        driver, create_table, create_custom_connector, "evolve_schema_dlq", dlq=True
    )

    record_count = 10
    raw_amount = base64.b64encode(b"\x01").decode("ascii")
    values = [
        _envelope(UNSUPPORTED_SCHEMA, {"ID": i, "AMOUNT": raw_amount})
        for i in range(record_count)
    ]
    driver.sendBytesData(table.name, values, [], partition=0)

    offsets_in_dlq = driver.consume_messages_dlq(
        {"config": connector.config}, 0, record_count - 1
    )
    assert offsets_in_dlq == record_count, (
        f"Expected {record_count} records in DLQ, got {offsets_in_dlq}"
    )
    assert not driver.get_failed_tasks(connector.name), "Connector task failed"
    count = table.select_scalar("count(*)")
    assert count == 0, f"Expected 0 rows in table (DLQ), got {count}"


@pytest.mark.skip(
    reason="needs a 403 ERR_EVOLVE_SCHEMA_NOT_ENABLED injection in test/docker/mitmproxy; "
    "addon_410.py only injects 410 and 404 today"
)
def test_not_enabled_falls_back_and_keeps_ingesting(
    driver: KafkaDriver,
    create_table: Callable,
    wait_for_rows: Callable,
    create_custom_connector: Callable,
):
    """A 403 ERR_EVOLVE_SCHEMA_NOT_ENABLED turns the evolver off and records still land.

    Columns then come from the JSON values (the previous behavior), so SMALL isn't NUMBER(5,0).
    That's what tells the fallback apart from a successful evolve-schema call.
    """
    table, connector = _start(
        driver, create_table, create_custom_connector, "evolve_schema_not_enabled"
    )

    record_count = 20
    values = [
        _envelope(TYPED_SCHEMA, {"ID": i, "SMALL": 1, "EVENT_TIME": BASE_MS + i})
        for i in range(record_count)
    ]
    driver.sendBytesData(table.name, values, [], partition=0)

    wait_for_rows(table.name, record_count, connector_name=connector.name)
    assert _column_types(table).get("SMALL") != "NUMBER(5,0)"
