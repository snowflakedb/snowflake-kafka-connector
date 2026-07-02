import json

import pytest
from confluent_kafka import avro
from lib.matchers import ANY_INT

FILE_NAME = "travis_correct_avrosr_avrosr"
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
        {"name": "time", "type": "int"},
        {"name": "someFloat", "type": "float"},
        {"name": "someFloatNaN", "type": "float"},
        {"name": "someFloatPositiveInfinity", "type": "float"},
        {"name": "someFloatNegativeInfinity", "type": "float"},
        {"name": "someDouble", "type": "double"},
        {"name": "someDoubleNaN", "type": "double"},
        {"name": "someDoublePositiveInfinity", "type": "double"},
        {"name": "someDoubleNegativeInfinity", "type": "double"}
    ]
}
""")


@pytest.mark.confluent_only
@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
def test_avrosr_avrosr(
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
        columns="(record_metadata variant, id number, firstName varchar, time number, "
        "someFloat number, someFloatNaN varchar, "
        "someFloatPositiveInfinity varchar, someFloatNegativeInfinity varchar, "
        "someDouble number, someDoubleNaN varchar, "
        "someDoublePositiveInfinity varchar, someDoubleNegativeInfinity varchar)",
    )
    topic = f"{FILE_NAME}{name_salt}"

    create_connector_from_file(CONFIG_FILE)
    driver.startConnectorWaitTime()

    # -- Send --
    keys = [{"id": i} for i in range(RECORD_COUNT)]
    values = [
        {
            "id": i,
            "firstName": "abc0",
            "time": 1835,
            "someFloat": 21.37,
            "someFloatNaN": "NaN",
            "someFloatPositiveInfinity": "inf",
            "someFloatNegativeInfinity": "-inf",
            "someDouble": 15.10,
            "someDoubleNaN": "NaN",
            "someDoublePositiveInfinity": "inf",
            "someDoubleNegativeInfinity": "-inf",
        }
        for i in range(RECORD_COUNT)
    ]
    driver.sendAvroSRData(topic, values, VALUE_SCHEMA, keys, KEY_SCHEMA)

    # -- Verify row count --
    wait_for_rows(table.name, RECORD_COUNT)

    # -- Verify first row content --
    # Snowflake does not guarantee row ordering without ORDER BY, so we must
    # select the specific record at offset 0 rather than relying on insertion order.
    row = table.select("*", "WHERE record_metadata:offset::int = 0")[0]

    assert row["ID"] == 0
    assert row["FIRSTNAME"] == "abc0"
    assert row["TIME"] == 1835
    assert row["SOMEFLOAT"] == 21
    assert row["SOMEFLOATNAN"] == "NaN"
    assert row["SOMEFLOATPOSITIVEINFINITY"] == "Inf"
    assert row["SOMEFLOATNEGATIVEINFINITY"] == "-Inf"
    assert row["SOMEDOUBLE"] == 15
    assert row["SOMEDOUBLENAN"] == "NaN"
    assert row["SOMEDOUBLEPOSITIVEINFINITY"] == "Inf"
    assert row["SOMEDOUBLENEGATIVEINFINITY"] == "-Inf"

    record_metadata = json.loads(row["RECORD_METADATA"])
    assert record_metadata == {
        "CreateTime": ANY_INT,
        "SnowflakeConnectorPushTime": ANY_INT,
        "key": {"id": 0},
        "offset": 0,
        "partition": 0,
        "topic": topic,
    }
