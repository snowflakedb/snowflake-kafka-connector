import json

from lib.matchers import ANY_INT

FILE_NAME = "travis_correct_json_json"
CONFIG_FILE = f"{FILE_NAME}.json"
RECORD_COUNT = 100


def test_json_json(
    driver, name_salt, create_connector_from_file, create_table, wait_for_rows
):
    table = create_table(
        FILE_NAME.upper(),
        columns='(record_metadata variant, "NUMBER" varchar)',
    )
    topic = f"{FILE_NAME}{name_salt}"

    create_connector_from_file(CONFIG_FILE)
    driver.startConnectorWaitTime()

    # -- Send --
    keys = [json.dumps({"number": str(i)}).encode("utf-8") for i in range(RECORD_COUNT)]
    values = [
        json.dumps({"number": str(i)}).encode("utf-8") for i in range(RECORD_COUNT)
    ]
    driver.sendBytesData(topic, values, keys)

    # -- Verify row count --
    wait_for_rows(table.name, RECORD_COUNT)

    # -- Verify first row content --
    record_metadata = json.loads(table.select_scalar("record_metadata"))

    assert record_metadata == {
        "SnowflakeConnectorPushTime": ANY_INT,
        "key": {"number": "0"},
        "offset": 0,
        "partition": 0,
    }
