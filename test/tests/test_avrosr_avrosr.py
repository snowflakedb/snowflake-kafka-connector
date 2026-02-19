import pytest
from confluent_kafka import avro
from snowflake.connector import DictCursor

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
def test_avrosr_avrosr(
    driver, name_salt, create_connector, snowflake_table, wait_for_rows
):
    topic = snowflake_table(
        FILE_NAME,
        f"CREATE OR REPLACE TABLE {FILE_NAME}{name_salt} ("
        f"record_metadata variant, id number, firstName varchar, time number, "
        f"someFloat number, someFloatNaN varchar, "
        f"someFloatPositiveInfinity varchar, someFloatNegativeInfinity varchar, "
        f"someDouble number, someDoubleNaN varchar, "
        f"someDoublePositiveInfinity varchar, someDoubleNegativeInfinity varchar)",
    )

    create_connector(CONFIG_FILE)
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
    wait_for_rows(topic, RECORD_COUNT)

    # -- Verify first row content --
    row = (
        driver.snowflake_conn.cursor(DictCursor)
        .execute(f"SELECT * FROM {topic} LIMIT 1")
        .fetchone()
    )

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

    gold_meta = r'{"CreateTime":\d*,"SnowflakeConnectorPushTime":\d*,"key":{"id":0},"offset":0,"partition":0,"topic":"travis_correct_avrosr_avrosr_\w*"}'
    driver.regexMatchMeta(row["RECORD_METADATA"], gold_meta)
