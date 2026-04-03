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
    create_connector_from_file,
    create_table,
    wait_for_rows,
):
    table = create_table(
        FILE_NAME.upper(),
        columns='(record_metadata variant, "NUMBER" varchar)',
    )
    topic = f"{FILE_NAME}{name_salt}"

    create_connector_from_file(CONFIG_FILE)
    driver.startConnectorWaitTime()

    # -- Send --
    headers = [("header1", "value1"), ("header2", "{}")]
    records = _build_records()
    driver.sendBytesData(topic, records, [], 0, headers)

    # -- Verify row count --
    wait_for_rows(table.name, RECORD_COUNT)

    # -- Verify first row content --
    # Snowflake does not guarantee row ordering without ORDER BY, so we must
    # select the specific record at offset 0 rather than relying on insertion order.
    rows = table.select("record_metadata", "WHERE record_metadata:offset::int = 0")
    record_metadata = json.loads(rows[0]["RECORD_METADATA"])

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
