import json
from time import sleep

FILE_NAME = "travis_correct_snowpipe_streaming_string_json"
CONFIG_FILE = f"{FILE_NAME}.json"
PARTITION_COUNT = 3
RECORDS_PER_PARTITION = 1000


def test_snowpipe_streaming_string_json(
    driver, name_salt, create_connector, snowflake_table, wait_for_rows
):
    topic = snowflake_table(
        FILE_NAME,
        f"CREATE OR REPLACE TABLE {FILE_NAME}{name_salt} "
        f"(record_metadata variant, fieldName varchar)",
    )

    driver.createTopics(topic, partitionNum=PARTITION_COUNT, replicationNum=1)

    create_connector(CONFIG_FILE)
    driver.startConnectorWaitTime()

    # -- Send --
    for p in range(PARTITION_COUNT):
        values = []
        for i in range(RECORDS_PER_PARTITION - 2):
            values.append(json.dumps({"fieldName": str(i)}).encode("utf-8"))

        if driver.testVersion == "2.5.1":
            values.append(
                json.dumps({"fieldName": str(RECORDS_PER_PARTITION - 1)}).encode(
                    "utf-8"
                )
            )
            values.append(
                json.dumps({"fieldName": str(RECORDS_PER_PARTITION)}).encode("utf-8")
            )
        else:
            values.append(None)
            values.append(b"")

        driver.sendBytesData(topic, values, [], partition=p)
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
        assert rows[p][0] == RECORDS_PER_PARTITION, (
            f"Partition {p}: expected {RECORDS_PER_PARTITION} unique offsets, got {rows[p][0]}"
        )
        assert rows[p][1] == p

    # -- Verify SnowflakeConnectorPushTime is populated --
    push_time_count = (
        driver.snowflake_conn.cursor()
        .execute(
            f"SELECT count(*) FROM {topic} "
            f"WHERE NOT is_null_value(record_metadata:SnowflakeConnectorPushTime)"
        )
        .fetchone()[0]
    )
    assert push_time_count == total_expected, (
        f"Empty ConnectorPushTime detected ({push_time_count}/{total_expected})"
    )
