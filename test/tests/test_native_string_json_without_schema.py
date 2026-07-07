import json

from lib.matchers import ANY_INT

FILE_NAME = "travis_correct_native_string_json_without_schema"
CONFIG_FILE = f"{FILE_NAME}.json"
RECORD_COUNT = 100


def test_native_string_json_without_schema(
    driver, name_salt, create_connector_from_file, create_table, wait_for_rows
):
    """Verify that an SMT (ReplaceField$Value blacklisting 'c2') drops the c2
    field before ingestion, leaving only the 'val' field.

    Connector config uses StringConverter key + JsonConverter value with a
    ReplaceField transform that removes 'c2'.
    """
    table = create_table(
        FILE_NAME.upper(),
        columns="(record_metadata variant, val varchar)",
    )
    topic = f"{FILE_NAME}{name_salt}"

    create_connector_from_file(CONFIG_FILE)
    driver.startConnectorWaitTime()

    # -- Send 100 records with 'val' and 'c2' (c2 will be dropped by SMT) --
    values = [
        json.dumps({"val": str(i), "c2": "Suppose to be dropped."}).encode("utf-8")
        for i in range(RECORD_COUNT)
    ]
    driver.sendBytesData(topic, values)

    # -- Verify row count --
    wait_for_rows(table.name, RECORD_COUNT)

    # -- Verify first row: only 'val' survives the SMT --
    # Snowflake does not guarantee row ordering without ORDER BY, so we must
    # select the specific record at offset 0 rather than relying on insertion order.
    row = table.select("*", "WHERE record_metadata:offset::int = 0")[0]

    assert json.loads(row["RECORD_METADATA"]) == {
        "CreateTime": ANY_INT,
        "SnowflakeConnectorPushTime": ANY_INT,
        "offset": 0,
        "partition": 0,
        "topic": topic,
    }
    assert row["VAL"] == "0"
