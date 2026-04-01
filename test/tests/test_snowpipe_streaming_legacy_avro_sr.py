"""RECORD_CONTENT mode with Avro SR converter.

Verifies that Avro-encoded records land correctly in the legacy
RECORD_CONTENT / RECORD_METADATA VARIANT columns when
snowflake.enable.schematization=false.

v3 parity cannot be verified: even with v4 removed, v3's bundled SR
classes clash with the Confluent 7.8.0 platform's SR classes
(ServiceConfigurationError: CelExecutor not a subtype of RuleExecutor).
Assertions reflect expected Avro deserialization behavior (JSON object
with correct field values). v4-only.
"""

import json

import pytest
from confluent_kafka import avro

FILE_NAME = "snowpipe_streaming_legacy_avro_sr"
CONFIG_FILE = f"{FILE_NAME}.json"
RECORD_COUNT = 100

VALUE_SCHEMA = avro.loads("""
{
    "type": "record",
    "name": "value_schema",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "firstName", "type": "string"},
        {"name": "time", "type": "int"}
    ]
}
""")


@pytest.mark.confluent_only
@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
def test_snowpipe_streaming_legacy_avro_sr(
    connector_version,
    driver,
    name_salt,
    create_connector_from_file,
    create_table,
    wait_for_rows,
):
    """Verify that Avro SR records land in RECORD_CONTENT as JSON objects."""
    table = create_table(
        FILE_NAME.upper(),
        columns="(RECORD_METADATA variant, RECORD_CONTENT variant)",
    )
    topic = f"{FILE_NAME}{name_salt}"

    create_connector_from_file(CONFIG_FILE)
    driver.startConnectorWaitTime()

    # -- Send Avro SR records --
    values = [{"id": i, "firstName": "abc0", "time": 1835} for i in range(RECORD_COUNT)]
    driver.sendAvroSRData(topic, values, VALUE_SCHEMA)

    # -- Verify row count --
    wait_for_rows(table.name, RECORD_COUNT)

    # -- Verify RECORD_CONTENT for offset 0 --
    row = table.select(
        "RECORD_CONTENT, RECORD_METADATA",
        'WHERE RECORD_METADATA:"offset"::number = 0',
    )[0]

    content = json.loads(row["RECORD_CONTENT"])
    if isinstance(content, str):
        content = json.loads(content)
    assert content["id"] == 0, (
        f"Expected id=0 in RECORD_CONTENT, got: {row['RECORD_CONTENT']}"
    )
    assert content["firstName"] == "abc0", (
        f"Expected firstName=abc0 in RECORD_CONTENT, got: {row['RECORD_CONTENT']}"
    )
    assert content["time"] == 1835, (
        f"Expected time=1835 in RECORD_CONTENT, got: {row['RECORD_CONTENT']}"
    )

    metadata = json.loads(row["RECORD_METADATA"])
    if isinstance(metadata, str):
        metadata = json.loads(metadata)
    for key in ("offset", "partition", "topic"):
        assert key in metadata, (
            f"RECORD_METADATA missing '{key}': {row['RECORD_METADATA']}"
        )
