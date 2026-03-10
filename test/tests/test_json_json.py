import json

from lib.matchers import ANY_INT

FILE_NAME = "travis_correct_json_json"
CONFIG_FILE = f"{FILE_NAME}.json"
RECORD_COUNT = 100


def test_json_json(driver, name_salt, create_connector, snowflake_table, wait_for_rows):
    topic = snowflake_table(
        FILE_NAME,
        f"CREATE OR REPLACE TABLE {FILE_NAME}{name_salt} "
        f'(record_metadata variant, "NUMBER" varchar)',
    )

    create_connector(CONFIG_FILE)
    driver.startConnectorWaitTime()

    # -- Send --
    keys = [json.dumps({"number": str(i)}).encode("utf-8") for i in range(RECORD_COUNT)]
    values = [
        json.dumps({"number": str(i)}).encode("utf-8") for i in range(RECORD_COUNT)
    ]
    driver.sendBytesData(topic, values, keys)

    # -- Verify row count --
    wait_for_rows(topic, RECORD_COUNT)

    # -- Verify first row content --
    record_metadata = json.loads(
        driver.snowflake_conn.cursor()
        .execute(f"SELECT record_metadata FROM {topic} LIMIT 1")
        .fetchone()[0]
    )

    assert record_metadata == {
        "SnowflakeConnectorPushTime": ANY_INT,
        "key": {"number": "0"},
        "offset": 0,
        "partition": 0,
    }
