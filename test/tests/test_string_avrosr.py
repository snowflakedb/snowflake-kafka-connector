import json

import pytest
from confluent_kafka import avro
from lib.matchers import ANY_INT

FILE_NAME = "travis_correct_string_avrosr"
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
def test_string_avrosr(
    driver,
    name_salt,
    connector_version,
    create_connector_from_file,
    create_table,
    wait_for_rows,
):
    # Assertions below capture v3 reference behavior (test ported from v3).
    # v4 parity confirmed 2026-03-31. v3 cannot run due to SR classloader conflict.
    table = create_table(
        FILE_NAME.upper(),
        columns="(record_metadata variant, id number, firstName varchar, time number)",
    )
    topic = f"{FILE_NAME}{name_salt}"

    create_connector_from_file(CONFIG_FILE)
    driver.startConnectorWaitTime()

    # -- Send --
    values = [{"id": i, "firstName": "abc0", "time": 1835} for i in range(RECORD_COUNT)]
    driver.sendAvroSRData(topic, values, VALUE_SCHEMA)

    # -- Verify row count --
    wait_for_rows(table.name, RECORD_COUNT)

    # -- Verify first row content --
    row = table.select("*")[0]

    assert row["ID"] == 0
    assert row["FIRSTNAME"] == "abc0"
    assert row["TIME"] == 1835

    record_metadata = json.loads(row["RECORD_METADATA"])
    assert record_metadata == {
        "CreateTime": ANY_INT,
        "SnowflakeConnectorPushTime": ANY_INT,
        "offset": 0,
        "partition": 0,
        "topic": topic,
    }
