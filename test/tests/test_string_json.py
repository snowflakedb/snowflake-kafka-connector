import json

from lib.matchers import ANY_INT

FILE_NAME = "travis_correct_string_json"
CONFIG_FILE = f"{FILE_NAME}.json"
RECORD_COUNT = 100


def _build_records() -> list[bytes | None]:
    """Build the list of values to produce.

    98 normal JSON records, then a tombstone (None), then an empty-string record
    that Snowflake custom converters treat as a normal record.
    """
    records: list[bytes | None] = [
        json.dumps({"number": str(i)}).encode("utf-8") for i in range(RECORD_COUNT - 2)
    ]
    records.append(None)
    records.append(b"")
    return records


def test_string_json(
    driver,
    name_salt,
    connector_version,
    create_connector,
    snowflake_table,
    wait_for_rows,
):
    topic = snowflake_table(
        FILE_NAME,
        f"CREATE OR REPLACE TABLE {FILE_NAME}{name_salt} "
        f'(record_metadata variant, "NUMBER" varchar)',
    )

    create_connector(CONFIG_FILE)
    driver.startConnectorWaitTime()

    # -- Send --
    headers = [("header1", "value1"), ("header2", "{}")]
    records = _build_records()
    driver.sendBytesData(topic, records, [], 0, headers)

    # -- Verify row count --
    wait_for_rows(topic, RECORD_COUNT)

    # -- Verify first row content --
    record_metadata = json.loads(
        driver.snowflake_conn.cursor()
        .execute(f"SELECT record_metadata FROM {topic} LIMIT 1")
        .fetchone()[0]
    )

    match connector_version:
        case "v3":
            expected_header2 = []
        case "v4":
            expected_header2 = "[]"

    assert record_metadata == {
        "CreateTime": ANY_INT,
        "SnowflakeConnectorPushTime": ANY_INT,
        "headers": {
            "header1": "value1",
            "header2": expected_header2,
        },
        "offset": 0,
        "partition": 0,
        "topic": topic,
    }
