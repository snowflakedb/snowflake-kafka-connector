import json

FILE_NAME = "snowpipe_streaming_legacy_string_json"
CONFIG_FILE = f"{FILE_NAME}.json"
RECORD_COUNT = 100


def test_snowpipe_streaming_legacy_string_json(
    driver, name_salt, create_connector, snowflake_table, wait_for_rows
):
    """Verify that enable.schematization=false wraps JSON records into the
    legacy RECORD_CONTENT / RECORD_METADATA VARIANT columns — the same
    table layout that KC v3 used by default.
    """
    topic = snowflake_table(
        FILE_NAME,
        f"CREATE OR REPLACE TABLE {FILE_NAME}{name_salt} "
        f"(RECORD_METADATA variant, RECORD_CONTENT variant)",
    )

    driver.createTopics(topic, partitionNum=1, replicationNum=1)

    create_connector(CONFIG_FILE)
    driver.startConnectorWaitTime()

    # -- Send JSON records --
    values = [
        json.dumps({"city": "Portland", "age": i}).encode("utf-8")
        for i in range(RECORD_COUNT)
    ]
    driver.sendBytesData(topic, values, [], partition=0)

    # -- Verify row count --
    wait_for_rows(topic, RECORD_COUNT)

    # -- Verify RECORD_CONTENT contains original JSON fields --
    row = (
        driver.snowflake_conn.cursor()
        .execute(
            f"SELECT RECORD_CONTENT, RECORD_METADATA FROM {topic} "
            f'WHERE RECORD_METADATA:"offset"::number = 0'
        )
        .fetchone()
    )
    assert row is not None, "Expected row with offset 0"

    content = json.loads(row[0])
    # VARIANT may store the payload as a JSON-encoded string (double-encoded)
    if isinstance(content, str):
        content = json.loads(content)
    assert content["city"] == "Portland", (
        f"Expected city=Portland in RECORD_CONTENT, got: {row[0]}"
    )
    assert content["age"] == 0, f"Expected age=0 in RECORD_CONTENT, got: {row[0]}"

    metadata = json.loads(row[1])
    if isinstance(metadata, str):
        metadata = json.loads(metadata)
    for key in ("offset", "partition", "topic"):
        assert key in metadata, f"RECORD_METADATA missing '{key}': {row[1]}"
