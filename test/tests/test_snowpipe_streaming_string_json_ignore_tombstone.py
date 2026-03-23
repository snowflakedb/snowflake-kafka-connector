import json
from time import sleep

FILE_NAME = "test_snowpipe_streaming_string_json_ignore_tombstone"
CONFIG_FILE = f"{FILE_NAME}.json"
PARTITION_COUNT = 3
RECORDS_PER_PARTITION = 1000
# Both None and "" are treated as tombstones in streaming mode (community converters).
EXPECTED_PER_PARTITION = RECORDS_PER_PARTITION - 2

# TODO: KC v3 uses case-sensitive field names matching. But the column names are upper case by default.
LONG_FIELD = "NUMBERNUMBERNUMBERNUMBERNUMBERNUMBERNUMBERNUMBERNUMBERNUMBERNUMBERNUMBER"


def test_snowpipe_streaming_string_json_ignore_tombstone(
    driver,
    create_connector_from_file,
    create_table,
    wait_for_rows,
):
    """Verify Snowpipe Streaming with behavior.on.null.values=IGNORE across
    multiple partitions.

    Sends RECORDS_PER_PARTITION records per partition (including a None and ""
    tombstone in each).  Both are dropped by the connector, leaving
    (RECORDS_PER_PARTITION - 2) × PARTITION_COUNT rows.
    Verifies: no duplicates, unique offsets per partition.
    """
    table = create_table(
        FILE_NAME,
        columns=f'(record_metadata variant, "{LONG_FIELD}" varchar)',
    )
    topic = table.name

    driver.createTopics(topic, partitionNum=PARTITION_COUNT, replicationNum=1)

    config = create_connector_from_file(CONFIG_FILE)
    connector_name = config["name"]
    driver.startConnectorWaitTime()

    # -- Send --
    for p in range(PARTITION_COUNT):
        values = []
        for i in range(RECORDS_PER_PARTITION - 2):
            values.append(json.dumps({LONG_FIELD: str(i)}).encode("utf-8"))

        values.append(None)
        values.append(b"")  # community converters treat this as a tombstone

        driver.sendBytesData(topic, values, [], partition=p)
        sleep(2)

    total_expected = EXPECTED_PER_PARTITION * PARTITION_COUNT

    # -- Verify row count --
    wait_for_rows(table.name, total_expected, connector_name=connector_name)

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
        assert rows[p]["UNIQUE_OFFSETS"] == EXPECTED_PER_PARTITION, (
            f"Partition {p}: expected {EXPECTED_PER_PARTITION} unique offsets, "
            f"got {rows[p]['UNIQUE_OFFSETS']}"
        )
        assert rows[p]["PARTITION_NO"] == p
