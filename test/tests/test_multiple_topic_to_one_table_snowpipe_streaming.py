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
    create_connector_from_file,
    create_table,
    wait_for_rows,
):
    table = create_table(
        FILE_NAME.upper(),
        columns="(record_metadata variant, field1 varchar)",
    )

    topics = []
    for i in range(TOPIC_COUNT):
        t = f"{FILE_NAME}{name_salt}{i}"
        driver.createTopics(t, partitionNum=PARTITION_COUNT, replicationNum=1)
        topics.append(t)

    create_connector_from_file(CONFIG_FILE)
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
    wait_for_rows(table.name, total_expected)

    # -- Verify no over-duplication (each offset+partition combo appears at most TOPIC_COUNT times) --
    result = table.select(
        'record_metadata:"offset"::string AS offset_no, '
        'record_metadata:"partition"::string AS partition_no',
        f"GROUP BY offset_no, partition_no HAVING count(*) > {TOPIC_COUNT}",
    )
    assert not result, f"Over-duplication detected: {result[0]}"

    # -- Verify unique offsets per partition --
    rows = table.select(
        'count(DISTINCT record_metadata:"offset"::number) AS unique_offsets, '
        'record_metadata:"partition"::number AS partition_no',
        "GROUP BY partition_no ORDER BY partition_no",
    )
    assert len(rows) == PARTITION_COUNT
    for p in range(PARTITION_COUNT):
        assert rows[p]["UNIQUE_OFFSETS"] == RECORDS_PER_PARTITION
        assert rows[p]["PARTITION_NO"] == p

    # -- Verify all topics contributed to each partition --
    topic_rows = table.select(
        'count(DISTINCT record_metadata:"topic"::string) AS topic_no, '
        'record_metadata:"partition"::number AS partition_no',
        "GROUP BY partition_no ORDER BY partition_no",
    )
    assert len(topic_rows) == PARTITION_COUNT
    for p in range(PARTITION_COUNT):
        assert topic_rows[p]["TOPIC_NO"] == TOPIC_COUNT
        assert topic_rows[p]["PARTITION_NO"] == p

    # -- Cleanup extra Kafka topics (table/main topic handled by fixture) --
    for t in topics:
        driver.deleteTopic(t)
