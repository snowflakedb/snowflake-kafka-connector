"""End-to-end coverage for Kafka record headers across different header converters.

Kafka Connect deserializes raw header bytes into structured ``ConnectHeaders``
using the worker/connector ``header.converter`` *before* the record reaches the
Snowflake connector. The connector then writes them to ``RECORD_METADATA:headers``.

These tests exercise the **structured** behavior only: converted header values keep
their types (objects, arrays, numbers, booleans) in ``RECORD_METADATA:headers``. On
v4 this requires ``snowflake.feature.structured.headers=true``; v3 preserves
structure natively. The v3 run and the structured v4 run must agree, so the test
asserts the *same* expected map for both versions, i.e. structured v4 == v3. (Legacy
string flattening, the v4 flag-off default, is covered by unit tests.)

The v3/v4 comparison is provided for free by the ``connector_version`` fixture,
which reruns each test once per version.

The connector config is built from ``V4_CONFIG_TEMPLATE`` and passed to the
``create_connector`` fixture, which migrates it to v3 automatically when the
``connector_version`` fixture selects v3. The target table is auto-created by the
connector (schematization off). KC v3 uppercases the topic->table name while v4
preserves it, so the topic is kept uppercase to make both versions resolve to the
same table.

All scenarios run in a single test: each gets its own topic (distinct base name)
and its own connector (distinguished via ``name_suffix``), so they ingest
concurrently instead of one sequential connector startup per scenario.
"""

import base64
import json
from dataclasses import dataclass

import pytest

from lib.config_migration import V4_CONFIG_TEMPLATE
from lib.fixtures.table import Table

RECORD = b'{"number": "1"}'


@dataclass
class Scenario:
    # Extra connector config selecting the header.converter (may be empty).
    converter_config: dict[str, str]
    # Raw header bytes sent to Kafka; the converter decides how to parse them.
    headers: list[tuple[str, bytes]]
    # Expected RECORD_METADATA:headers when structured headers are preserved.
    # Must hold for both v3 and structured v4.
    expected: dict[str, object]


# Keyed by a short id used verbatim in the per-variant topic/table base name.
SCENARIOS = {
    # Default SimpleHeaderConverter: infers types from the literal via Kafka's
    # Values parser. Note the parser's quirk: "{}" parses to an empty array.
    "simple": Scenario(
        converter_config={},
        headers=[
            ("h_string", b"value1"),
            ("h_number", b"42"),
            ("h_object", b"{}"),
        ],
        expected={
            "h_string": "value1",
            "h_number": 42,
            "h_object": [],
        },
    ),
    # JsonConverter (schemas off): parses JSON header bytes into native types.
    "json": Scenario(
        converter_config={
            "header.converter": "org.apache.kafka.connect.json.JsonConverter",
            "header.converter.schemas.enable": "false",
        },
        headers=[
            ("h_string", b'"hello"'),
            ("h_number", b"42"),
            ("h_bool", b"true"),
            ("h_object", b'{"a":1,"b":"x"}'),
            ("h_array", b"[1,2,3]"),
        ],
        expected={
            "h_string": "hello",
            "h_number": 42,
            "h_bool": True,
            "h_object": {"a": 1, "b": "x"},
            "h_array": [1, 2, 3],
        },
    ),
    # StringConverter: every header stays a UTF-8 string (JSON text is not parsed).
    "text": Scenario(
        converter_config={
            "header.converter": "org.apache.kafka.connect.storage.StringConverter",
        },
        headers=[
            ("h_string", b"hello"),
            ("h_json_text", b'{"a":1}'),
        ],
        expected={
            "h_string": "hello",
            "h_json_text": '{"a":1}',
        },
    ),
    # ByteArrayConverter: header value stays a byte[], which the SDK serializes as
    # base64 in the structured VARIANT.
    "byte": Scenario(
        converter_config={
            "header.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
        },
        headers=[
            ("h_bytes", b"hello"),
        ],
        expected={
            "h_bytes": base64.b64encode(b"hello").decode("ascii"),
        },
    ),
}


@pytest.mark.correctness
def test_headers(
    driver,
    connector_version,
    create_connector,
    create_topics,
    wait_for_rows,
):
    # Uppercase topics: the connector auto-creates the table, and v3 uppercases the
    # topic->table name while v4 preserves it. Keeping the topics uppercase makes
    # both land on the same table name.
    topics = {
        scenario_id: create_topics(
            [f"HEADERS_{scenario_id.upper()}"], with_tables=False
        )[0]
        for scenario_id in SCENARIOS
    }

    # One connector per scenario, all started before any ingestion so they run
    # concurrently.
    connectors = {}
    for scenario_id, scenario in SCENARIOS.items():
        config = {
            **V4_CONFIG_TEMPLATE,
            "key.converter": "org.apache.kafka.connect.storage.StringConverter",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter.schemas.enable": "false",
            "snowflake.enable.schematization": "false",
            "topics": topics[scenario_id],
            **scenario.converter_config,
        }
        # v4 needs the flag; v3 preserves structure natively (rejects the v4-only flag).
        if connector_version == "v4":
            config["snowflake.feature.structured.headers"] = "true"
        connectors[scenario_id] = create_connector(
            v4_config=config, name_suffix=f"_{scenario_id}"
        )

    driver.startConnectorWaitTime()

    for scenario_id, scenario in SCENARIOS.items():
        driver.sendBytesData(
            topics[scenario_id], [RECORD], partition=0, headers=scenario.headers
        )

    for scenario_id, scenario in SCENARIOS.items():
        table = Table(driver, topics[scenario_id])
        wait_for_rows(table.name, 1, connector_name=connectors[scenario_id].name)

        rows = table.select("record_metadata", "WHERE record_metadata:offset::int = 0")
        assert rows, f"{scenario_id}: no row at offset 0"
        headers = json.loads(rows[0]["RECORD_METADATA"])["headers"]
        assert headers == scenario.expected, (
            f"{scenario_id}: {headers} != {scenario.expected}"
        )
