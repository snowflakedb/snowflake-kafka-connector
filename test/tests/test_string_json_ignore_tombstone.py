import json

FILE_NAME = "test_string_json_ignore_tombstone"
CONFIG_FILE = f"{FILE_NAME}.json"
RECORD_COUNT = 100
# KCv4 uses the standard JsonConverter (not the custom SF converter).
# JsonConverter converts both None (tombstone) and empty bytes b"" to null
# SchemaAndValue, and behavior.on.null.values=IGNORE drops both.
EXPECTED_ROWS = RECORD_COUNT - 2

OLD_VERSIONS = {"5.4.0", "5.3.0", "5.2.0", "2.4.0", "2.3.0", "2.2.0"}


def test_string_json_ignore_tombstone(
    driver, name_salt, create_connector, snowflake_table, wait_for_rows
):
    """Verify that behavior.on.null.values=IGNORE drops null-like records.

    With the standard JsonConverter, both None (explicit tombstone) and empty
    bytes (b"") are converted to null SchemaAndValue and therefore dropped
    when IGNORE is configured.  Also verifies headers on the first row.
    """
    topic = snowflake_table(
        FILE_NAME,
        f"CREATE OR REPLACE TABLE {FILE_NAME}{name_salt} "
        f'(record_metadata variant, "number" varchar)',
    )

    create_connector(CONFIG_FILE)
    driver.startConnectorWaitTime()

    # -- Build records: 98 normal + 1 tombstone (None) + 1 empty string --
    values: list[bytes | None] = [
        json.dumps({"number": str(i)}).encode("utf-8") for i in range(RECORD_COUNT - 2)
    ]

    # https://issues.apache.org/jira/browse/KAFKA-10477
    if driver.testVersion != "2.5.1":
        values.append(None)
    else:
        values.append(json.dumps({"number": str(RECORD_COUNT - 2)}).encode("utf-8"))

    # empty-string record — SF custom converters treat this as a normal record
    values.append(b"")

    headers = [("header1", "value1"), ("header2", "{}")]
    driver.sendBytesData(topic, values, [], 0, headers)

    # -- Verify row count --
    wait_for_rows(topic, EXPECTED_ROWS)

    # -- Verify first row content --
    row = (
        driver.snowflake_conn.cursor()
        .execute(f"SELECT * FROM {topic} LIMIT 1")
        .fetchone()
    )

    if driver.testVersion in OLD_VERSIONS:
        gold_meta = (
            r'{"CreateTime":\d*,"SnowflakeConnectorPushTime":\d*,'
            r'"headers":{"header1":"value1","header2":{}},'
            r'"offset":0,"partition":0,'
            r'"topic":"test_string_json_ignore_tombstone_\w*"}'
        )
    else:
        gold_meta = (
            r'{"CreateTime":\d*,"SnowflakeConnectorPushTime":\d*,'
            r'"headers":{"header1":"value1","header2":"[]"},'
            r'"offset":0,"partition":0,'
            r'"topic":"test_string_json_ignore_tombstone_\w*"}'
        )

    driver.regexMatchOneLine(row, gold_meta, r"0")
