"""Schema registry ids in RECORD_METADATA with SnowflakeAvroConverter.

KC v3 wrote ``schema_id`` for the value and ``key_schema_id`` for the key
when ``SnowflakeAvroConverter`` converted them. This test pins the same
fields for v4, in RECORD_CONTENT mode with a VARIANT RECORD_METADATA column.

v4-only. v3 cannot run on this platform, for the reason given in
``test_snowpipe_streaming_legacy_avro_sr.py``.
"""

import json

import pytest
from confluent_kafka import avro
from lib.matchers import ANY_INT

FILE_NAME = "snowflake_avro_converter_schema_id"
CONFIG_FILE = f"{FILE_NAME}.json"
RECORD_COUNT = 100

KEY_SCHEMA = avro.loads("""
{
    "type": "record",
    "name": "key_schema",
    "fields": [
        {"name": "id", "type": "int"}
    ]
}
""")

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
def test_snowflake_avro_converter_schema_id(
    connector_version,
    driver,
    name_salt,
    create_connector_from_file,
    create_table,
    wait_for_rows,
):
    """Verify that RECORD_METADATA carries schema_id and key_schema_id."""
    table = create_table(
        FILE_NAME.upper(),
        columns="(RECORD_METADATA variant, RECORD_CONTENT variant)",
    )
    topic = f"{FILE_NAME}{name_salt}"

    create_connector_from_file(CONFIG_FILE)
    driver.startConnectorWaitTime()

    # -- Send Avro SR records with Avro SR keys --
    keys = [{"id": i} for i in range(RECORD_COUNT)]
    values = [{"id": i, "firstName": "abc0", "time": 1835} for i in range(RECORD_COUNT)]
    driver.sendAvroSRData(topic, values, VALUE_SCHEMA, keys, KEY_SCHEMA)

    # -- Verify row count --
    wait_for_rows(table.name, RECORD_COUNT)

    # -- Verify the row at offset 0 --
    row = table.select(
        "RECORD_CONTENT, RECORD_METADATA",
        'WHERE RECORD_METADATA:"offset"::number = 0',
    )[0]

    content = json.loads(row["RECORD_CONTENT"])
    if isinstance(content, str):
        content = json.loads(content)
    assert content == {"id": 0, "firstName": "abc0", "time": 1835}

    metadata = json.loads(row["RECORD_METADATA"])
    if isinstance(metadata, str):
        metadata = json.loads(metadata)
    assert metadata == {
        "CreateTime": ANY_INT,
        "SnowflakeConnectorPushTime": ANY_INT,
        "key": {"id": 0},
        "key_schema_id": ANY_INT,
        "offset": 0,
        "partition": 0,
        "schema_id": ANY_INT,
        "topic": topic,
    }
    # The key and the value have different schemas, so the registry gives them different ids.
    assert metadata["key_schema_id"] != metadata["schema_id"]
