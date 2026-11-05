"""E2E tests for server-side schema evolution with Kafka Connect schemas.

With snowflake.feature.evolve.schema.enabled=true and server_side validation, the connector
sends each new value schema to the evolve-schema endpoint before appending. Columns are created
with the types the schema declares, not types guessed from JSON values.

Records are JSON with an embedded schema (JsonConverter schemas.enable=true), so the connector
has a Kafka Connect schema to send.
"""

import base64
import json

import pytest

pytestmark = pytest.mark.schema_evolution

FILE_NAME = "snowpipe_streaming_evolve_schema"
CONFIG_FILE = f"{FILE_NAME}.json"

# Kafka Connect logical type names, as JsonConverter writes them into the schema envelope.
TIMESTAMP_NAME = "org.apache.kafka.connect.data.Timestamp"
DECIMAL_NAME = "org.apache.kafka.connect.data.Decimal"

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

# A decimal precision above 38 is one Snowflake can't store, so the endpoint answers 400.
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


def _envelope(schema, payload):
    return json.dumps({"schema": schema, "payload": payload}).encode("utf-8")


def _assert_dlq(driver, config, table, record_count):
    """Shared assertions for DLQ tests: every record is in the DLQ and none reached the table."""
    offsets_in_dlq = driver.consume_messages_dlq(config, 0, record_count - 1)
    assert offsets_in_dlq == record_count, (
        f"Expected {record_count} records in DLQ, got {offsets_in_dlq}"
    )

    count = table.select_scalar("count(*)")
    assert count == 0, f"Expected 0 rows in table (DLQ), got {count}"


def test_new_fields_get_schema_declared_types(
    driver, create_connector_from_file, create_table, create_topics, wait_for_rows
):
    """New fields are added with the types the Kafka Connect schema declares.

    SMALL is int16, so NUMBER(5,0). EVENT_TIME is a Timestamp, so TIMESTAMP_NTZ(3). Under
    JSON-value inference, both would have come out as NUMBER(38,0) or TEXT.
    """
    table = create_table(
        FILE_NAME.upper(),
        columns="(RECORD_METADATA VARIANT) ENABLE_SCHEMA_EVOLUTION = TRUE",
        cleanup_topic=False,
    )
    topic = create_topics([FILE_NAME], with_tables=False)[0]

    create_connector_from_file(CONFIG_FILE)
    driver.startConnectorWaitTime()

    record_count = 100
    base_ms = 1700000000000
    values = [
        _envelope(
            TYPED_SCHEMA,
            {"ID": i, "SMALL": i % 100, "EVENT_TIME": base_ms + i},
        )
        for i in range(record_count)
    ]
    driver.sendBytesData(topic, values, [], partition=0)

    wait_for_rows(table.name, record_count)

    cols = {row[0]: row[1] for row in table.schema()}
    assert cols.get("SMALL") == "NUMBER(5,0)", f"SMALL type: {cols.get('SMALL')}"
    assert cols.get("EVENT_TIME") == "TIMESTAMP_NTZ(3)", (
        f"EVENT_TIME type: {cols.get('EVENT_TIME')}"
    )

    rows = table.select(
        '"SMALL", "EVENT_TIME"',
        'WHERE RECORD_METADATA:"offset"::number = 0',
    )
    assert rows, "Expected row with offset 0"
    assert rows[0]["SMALL"] == 0
    assert rows[0]["EVENT_TIME"] is not None

    count = table.select_scalar("count(*)")
    assert count == record_count, f"Expected {record_count} rows, got {count}"


def test_second_schema_adds_only_its_new_columns(
    driver, create_connector_from_file, create_table, create_topics, wait_for_rows
):
    """A second schema adds only the columns that are missing.

    Wave 1 creates ID, SMALL, EVENT_TIME. Wave 2 adds EXTRA. The earlier columns are untouched,
    and wave-1 rows read NULL for EXTRA.
    """
    table = create_table(
        FILE_NAME.upper(),
        columns="(RECORD_METADATA VARIANT) ENABLE_SCHEMA_EVOLUTION = TRUE",
        cleanup_topic=False,
    )
    topic = create_topics([FILE_NAME], with_tables=False)[0]

    create_connector_from_file(CONFIG_FILE)
    driver.startConnectorWaitTime()

    wave1_count = 50
    base_ms = 1700000000000
    wave1 = [
        _envelope(TYPED_SCHEMA, {"ID": i, "SMALL": 1, "EVENT_TIME": base_ms + i})
        for i in range(wave1_count)
    ]
    driver.sendBytesData(topic, wave1, [], partition=0)
    wait_for_rows(table.name, wave1_count)

    wave2_schema = {
        **TYPED_SCHEMA,
        "fields": TYPED_SCHEMA["fields"]
        + [{"field": "EXTRA", "type": "string", "optional": True}],
    }
    wave2_count = 50
    wave2 = [
        _envelope(
            wave2_schema,
            {"ID": 1000 + i, "SMALL": 2, "EVENT_TIME": base_ms + i, "EXTRA": "x"},
        )
        for i in range(wave2_count)
    ]
    driver.sendBytesData(topic, wave2, [], partition=0)
    wait_for_rows(table.name, wave1_count + wave2_count)

    cols = {row[0]: row[1] for row in table.schema()}
    assert cols.get("EXTRA") == "VARCHAR(16777216)", f"EXTRA type: {cols.get('EXTRA')}"

    nulls = table.select_scalar(
        "count(*)",
        'WHERE RECORD_METADATA:"offset"::number < 50 AND "EXTRA" IS NULL',
    )
    assert nulls == wave1_count, (
        f"Expected {wave1_count} wave-1 rows with NULL EXTRA, got {nulls}"
    )


def test_unsupported_schema_goes_to_dlq(
    driver, create_connector_from_file, create_table, create_topics, wait_for_rows
):
    """A schema the endpoint rejects with 400 is routed to the DLQ, not the table.

    The decimal precision of 39 can't be stored in a Snowflake NUMBER, so the evolve-schema call
    fails and errors.tolerance=all sends each record to the DLQ.
    """
    table = create_table(
        FILE_NAME.upper(),
        columns="(RECORD_METADATA VARIANT) ENABLE_SCHEMA_EVOLUTION = TRUE",
        cleanup_topic=False,
    )
    topic = create_topics([FILE_NAME], with_tables=False)[0]

    config = create_connector_from_file(CONFIG_FILE)
    driver.startConnectorWaitTime()

    record_count = 10
    raw_amount = base64.b64encode(b"\x01").decode("ascii")
    values = [
        _envelope(UNSUPPORTED_SCHEMA, {"ID": i, "AMOUNT": raw_amount})
        for i in range(record_count)
    ]
    driver.sendBytesData(topic, values, [], partition=0)

    _assert_dlq(driver, config, table, record_count)


@pytest.mark.skip(
    reason="needs a 403 ERR_EVOLVE_SCHEMA_NOT_ENABLED injection in test/docker/mitmproxy; "
    "addon_410.py only injects 410 and 404 today"
)
def test_not_enabled_falls_back_and_keeps_ingesting(
    driver, create_connector_from_file, create_table, create_topics, wait_for_rows
):
    """A 403 ERR_EVOLVE_SCHEMA_NOT_ENABLED turns the evolver off and records still land.

    The records are appended, with columns coming from the JSON values (the previous behavior).
    """
    table = create_table(
        FILE_NAME.upper(),
        columns="(RECORD_METADATA VARIANT) ENABLE_SCHEMA_EVOLUTION = TRUE",
        cleanup_topic=False,
    )
    topic = create_topics([FILE_NAME], with_tables=False)[0]

    create_connector_from_file(CONFIG_FILE)
    driver.startConnectorWaitTime()

    record_count = 20
    values = [
        _envelope(TYPED_SCHEMA, {"ID": i, "SMALL": 1, "EVENT_TIME": 1700000000000 + i})
        for i in range(record_count)
    ]
    driver.sendBytesData(topic, values, [], partition=0)

    wait_for_rows(table.name, record_count)
    count = table.select_scalar("count(*)")
    assert count == record_count, f"Expected {record_count} rows, got {count}"
