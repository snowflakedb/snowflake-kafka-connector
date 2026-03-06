import json

FILE_NAME = "snowpipe_streaming_legacy_string_converter"
CONFIG_FILE = f"{FILE_NAME}.json"
RECORD_COUNT = 100


def test_snowpipe_streaming_legacy_string_converter(
    driver, name_salt, create_connector, snowflake_table, wait_for_rows
):
    """Verify that StringConverter is accepted when enable.schematization=false
    and that raw string payloads land in the legacy RECORD_CONTENT column.
    """
    topic = snowflake_table(
        FILE_NAME,
        f"CREATE OR REPLACE TABLE {FILE_NAME}{name_salt} "
        f"(RECORD_METADATA variant, RECORD_CONTENT variant)",
    )

    driver.createTopics(topic, partitionNum=1, replicationNum=1)

    create_connector(CONFIG_FILE)
    driver.startConnectorWaitTime()

    # -- Send raw string records --
    values = [f"hello world {i}".encode("utf-8") for i in range(RECORD_COUNT)]
    driver.sendBytesData(topic, values, [], partition=0)

    # -- Verify row count --
    wait_for_rows(topic, RECORD_COUNT)

    # -- Verify RECORD_CONTENT contains the string payload --
    row = (
        driver.snowflake_conn.cursor()
        .execute(
            f"SELECT RECORD_CONTENT, RECORD_METADATA FROM {topic} "
            f'WHERE RECORD_METADATA:"offset"::number = 0'
        )
        .fetchone()
    )
    assert row is not None, "Expected row with offset 0"

    content = str(row[0])
    assert "hello world 0" in content, (
        f"Expected 'hello world 0' in RECORD_CONTENT, got: {row[0]}"
    )

    metadata = json.loads(row[1])
    if isinstance(metadata, str):
        metadata = json.loads(metadata)
    for key in ("offset", "partition", "topic"):
        assert key in metadata, f"RECORD_METADATA missing '{key}': {row[1]}"
