from time import sleep

import pytest
from confluent_kafka import avro

FILE_NAME = "travis_correct_snowpipe_streaming_string_avro_sr"
CONFIG_FILE = f"{FILE_NAME}.json"
PARTITION_COUNT = 3
RECORDS_PER_PARTITION = 1000

VALUE_SCHEMA = avro.loads("""
{
    "type": "record",
    "name": "value_schema",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "firstName", "type": "string"},
        {"name": "time", "type": "int"},
        {"name": "someFloat", "type": "float"},
        {"name": "someFloatNaN", "type": "float"}
    ]
}
""")


@pytest.mark.confluent_only
def test_snowpipe_streaming_string_avro_sr(
    driver, name_salt, create_connector, snowflake_table, wait_for_rows
):
    topic = snowflake_table(
        FILE_NAME,
        f"CREATE OR REPLACE TABLE {FILE_NAME}{name_salt} ("
        f"record_metadata variant, id number, firstName string, "
        f"time number, someFloat number, someFloatNaN string)",
    )

    driver.createTopics(topic, partitionNum=PARTITION_COUNT, replicationNum=1)

    create_connector(CONFIG_FILE)
    driver.startConnectorWaitTime()

    # -- Send --
    for p in range(PARTITION_COUNT):
        values = [
            {
                "id": i,
                "firstName": "abc0",
                "time": 1835,
                "someFloat": 21.37,
                "someFloatNaN": "NaN",
            }
            for i in range(RECORDS_PER_PARTITION)
        ]
        driver.sendAvroSRData(
            topic, values, VALUE_SCHEMA, key=[], key_schema="", partition=p
        )
        sleep(2)

    total_expected = RECORDS_PER_PARTITION * PARTITION_COUNT

    # -- Verify row count --
    wait_for_rows(topic, total_expected)

    # -- Verify no duplicates --
    dup = (
        driver.snowflake_conn.cursor()
        .execute(
            f'SELECT record_metadata:"offset"::string AS offset_no, '
            f'record_metadata:"partition"::string AS partition_no '
            f"FROM {topic} GROUP BY offset_no, partition_no HAVING count(*) > 1"
        )
        .fetchone()
    )
    assert dup is None, f"Duplicate detected: {dup}"

    # -- Verify unique offsets per partition --
    rows = (
        driver.snowflake_conn.cursor()
        .execute(
            f'SELECT count(DISTINCT record_metadata:"offset"::number) AS unique_offsets, '
            f'record_metadata:"partition"::number AS partition_no '
            f"FROM {topic} GROUP BY partition_no ORDER BY partition_no"
        )
        .fetchall()
    )
    assert len(rows) == PARTITION_COUNT
    for p in range(PARTITION_COUNT):
        assert rows[p][0] == RECORDS_PER_PARTITION
        assert rows[p][1] == p
