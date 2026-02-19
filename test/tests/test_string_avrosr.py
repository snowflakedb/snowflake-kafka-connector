import pytest
from confluent_kafka import avro
from snowflake.connector import DictCursor

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
def test_string_avrosr(
    driver, name_salt, create_connector, snowflake_table, wait_for_rows
):
    topic = snowflake_table(
        FILE_NAME,
        f"CREATE OR REPLACE TABLE {FILE_NAME}{name_salt} "
        f"(record_metadata variant, id number, firstName varchar, time number)",
    )

    create_connector(CONFIG_FILE)
    driver.startConnectorWaitTime()

    # -- Send --
    values = [{"id": i, "firstName": "abc0", "time": 1835} for i in range(RECORD_COUNT)]
    driver.sendAvroSRData(topic, values, VALUE_SCHEMA)

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

    gold_meta = r'{"CreateTime":\d*,"SnowflakeConnectorPushTime":\d*,"offset":0,"partition":0,"topic":"travis_correct_string_avrosr_\w*"}'
    driver.regexMatchMeta(row["RECORD_METADATA"], gold_meta)
