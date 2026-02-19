import json
from time import sleep

FILE_NAME = "travis_correct_multiple_topic_to_one_table_snowpipe_streaming"
CONFIG_FILE = f"{FILE_NAME}.json"
TOPIC_COUNT = 3
PARTITION_COUNT = 3
RECORDS_PER_PARTITION = 1000


def test_multiple_topic_to_one_table_snowpipe_streaming(
    driver,
    name_salt,
    create_connector,
    snowflake_table,
    wait_for_rows,
):
    table = snowflake_table(
        FILE_NAME,
        f"CREATE OR REPLACE TABLE {FILE_NAME}{name_salt} "
        f"(record_metadata variant, field1 varchar)",
    )

    topics = []
    for i in range(TOPIC_COUNT):
        t = f"{FILE_NAME}{name_salt}{i}"
        driver.createTopics(t, partitionNum=PARTITION_COUNT, replicationNum=1)
        topics.append(t)

    create_connector(CONFIG_FILE)
    driver.startConnectorWaitTime()

    # -- Send --
    for topic in topics:
        for p in range(PARTITION_COUNT):
            values = [
                json.dumps({"field1": str(e)}).encode("utf-8")
                for e in range(RECORDS_PER_PARTITION)
            ]
            driver.sendBytesData(topic, values, [], partition=p)
            sleep(2)

    total_expected = RECORDS_PER_PARTITION * PARTITION_COUNT * TOPIC_COUNT

    # -- Verify row count --
    wait_for_rows(table, total_expected)

    # -- Verify no over-duplication (each offset+partition combo appears at most TOPIC_COUNT times) --
    dup = (
        driver.snowflake_conn.cursor()
        .execute(
            f'SELECT record_metadata:"offset"::string AS offset_no, '
            f'record_metadata:"partition"::string AS partition_no '
            f"FROM {table} GROUP BY offset_no, partition_no HAVING count(*) > {TOPIC_COUNT}"
        )
        .fetchone()
    )
    assert dup is None, f"Over-duplication detected: {dup}"

    # -- Verify unique offsets per partition --
    rows = (
        driver.snowflake_conn.cursor()
        .execute(
            f'SELECT count(DISTINCT record_metadata:"offset"::number) AS unique_offsets, '
            f'record_metadata:"partition"::number AS partition_no '
            f"FROM {table} GROUP BY partition_no ORDER BY partition_no"
        )
        .fetchall()
    )
    assert len(rows) == PARTITION_COUNT
    for p in range(PARTITION_COUNT):
        assert rows[p][0] == RECORDS_PER_PARTITION
        assert rows[p][1] == p

    # -- Verify all topics contributed to each partition --
    topic_rows = (
        driver.snowflake_conn.cursor()
        .execute(
            f'SELECT count(DISTINCT record_metadata:"topic"::string) AS topic_no, '
            f'record_metadata:"partition"::number AS partition_no '
            f"FROM {table} GROUP BY partition_no ORDER BY partition_no"
        )
        .fetchall()
    )
    assert len(topic_rows) == PARTITION_COUNT
    for p in range(PARTITION_COUNT):
        assert topic_rows[p][0] == TOPIC_COUNT
        assert topic_rows[p][1] == p

    # -- Cleanup extra Kafka topics (table/main topic handled by fixture) --
    for t in topics:
        driver.deleteTopic(t)
