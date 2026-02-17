import json

FILE_NAME = "travis_correct_string_json"
CONFIG_FILE = f"{FILE_NAME}.json"
RECORD_COUNT = 100

OLD_VERSIONS = {"5.4.0", "5.3.0", "5.2.0", "2.4.0", "2.3.0", "2.2.0"}


def _build_records(test_version: str) -> list[bytes | None]:
    """Build the list of values to produce.

    98 normal JSON records, then either a tombstone (None) or a workaround
    record for Kafka 2.5.1 (KAFKA-10477), then an empty-string record that
    Snowflake custom converters treat as a normal record.
    """
    records: list[bytes | None] = [
        json.dumps({"number": str(i)}).encode("utf-8") for i in range(RECORD_COUNT - 2)
    ]

    if test_version == "2.5.1":
        # https://issues.apache.org/jira/browse/KAFKA-10477
        records.append(
            json.dumps(
                {
                    "numbernumbernumbernumbernumbernumbernumber"
                    "numbernumbernumbernumbernumber": str(RECORD_COUNT)
                }
            ).encode("utf-8")
        )
    else:
        records.append(None)

    records.append(b"")
    return records


def test_string_json(
    driver, name_salt, create_connector, snowflake_table, wait_for_rows
):
    topic = snowflake_table(
        FILE_NAME,
        f"CREATE OR REPLACE TABLE {FILE_NAME}{name_salt} "
        f'(record_metadata variant, "number" varchar)',
    )

    create_connector(CONFIG_FILE)
    driver.startConnectorWaitTime()

    # -- Send --
    headers = [("header1", "value1"), ("header2", "{}")]
    records = _build_records(driver.testVersion)
    driver.sendBytesData(topic, records, [], 0, headers)

    # -- Verify row count --
    wait_for_rows(topic, RECORD_COUNT)

    # -- Verify first row content --
    row = driver.snowflake_conn.cursor().execute(f"SELECT * FROM {topic}").fetchone()

    if driver.testVersion in OLD_VERSIONS:
        gold_meta = r'{"CreateTime":\d*,"SnowflakeConnectorPushTime":\d*,"headers":{"header1":"value1","header2":{}},"offset":0,"partition":0,"topic":"travis_correct_string_json_\w*"}'
    else:
        gold_meta = r'{"CreateTime":\d*,"SnowflakeConnectorPushTime":\d*,"headers":{"header1":"value1","header2":"[]"},"offset":0,"partition":0,"topic":"travis_correct_string_json_\w*"}'

    driver.regexMatchOneLine(row, gold_meta, r"0")
