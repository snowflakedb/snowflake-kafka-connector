import json
from time import sleep

FILE_NAME = "travis_correct_snowpipe_streaming_string_json"
CONFIG_FILE = f"{FILE_NAME}.json"
PARTITION_COUNT = 3
RECORDS_PER_PARTITION = 1000


def test_snowpipe_streaming_string_json(
    driver, create_connector_from_file, create_table, wait_for_rows
):
    table = create_table(
        FILE_NAME,
        columns="(record_metadata variant, fieldName varchar)",
    )
    topic = table.name

    driver.createTopics(topic, partitionNum=PARTITION_COUNT, replicationNum=1)

    create_connector_from_file(CONFIG_FILE)
    driver.startConnectorWaitTime()

    # -- Send --
    for p in range(PARTITION_COUNT):
        values = []
        for i in range(RECORDS_PER_PARTITION - 2):
            values.append(json.dumps({"fieldName": str(i)}).encode("utf-8"))

        values.append(None)
        values.append(b"")

        driver.sendBytesData(topic, values, [], partition=p)
        sleep(2)

    total_expected = RECORDS_PER_PARTITION * PARTITION_COUNT

    # -- Verify row count --
    wait_for_rows(table.name, total_expected)

    # -- Verify no duplicates --
    result = table.select(
        'record_metadata:"offset"::string AS offset_no, '
        'record_metadata:"partition"::string AS partition_no',
        "GROUP BY offset_no, partition_no HAVING count(*) > 1",
    )
    assert not result, f"Duplicate detected: {result[0]}"

    # -- Verify unique offsets per partition --
    rows = table.select(
        'count(DISTINCT record_metadata:"offset"::number) AS unique_offsets, '
        'record_metadata:"partition"::number AS partition_no',
        "GROUP BY partition_no ORDER BY partition_no",
    )
    assert len(rows) == PARTITION_COUNT
    for p in range(PARTITION_COUNT):
        assert rows[p]["UNIQUE_OFFSETS"] == RECORDS_PER_PARTITION, (
            f"Partition {p}: expected {RECORDS_PER_PARTITION} unique offsets, "
            f"got {rows[p]['UNIQUE_OFFSETS']}"
        )
        assert rows[p]["PARTITION_NO"] == p

    # -- Verify SnowflakeConnectorPushTime is populated --
    push_time_count = table.select(
        "count(*)",
        "WHERE NOT is_null_value(record_metadata:SnowflakeConnectorPushTime)",
    )[0]["COUNT(*)"]
    assert push_time_count == total_expected, (
        f"Empty ConnectorPushTime detected ({push_time_count}/{total_expected})"
    )
