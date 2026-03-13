import json

from lib.matchers import ANY_INT

FILE_NAME = "travis_correct_native_string_json_without_schema"
CONFIG_FILE = f"{FILE_NAME}.json"
RECORD_COUNT = 100


def test_native_string_json_without_schema(
    driver, name_salt, create_connector, snowflake_table, wait_for_rows
):
    """Verify that an SMT (ReplaceField$Value blacklisting 'c2') drops the c2
    field before ingestion, leaving only the 'val' field.

    Connector config uses StringConverter key + JsonConverter value with a
    ReplaceField transform that removes 'c2'.
    """
    topic = snowflake_table(
        FILE_NAME,
        f"CREATE OR REPLACE TABLE {FILE_NAME}{name_salt} "
        f"(record_metadata variant, val varchar)",
    )

    create_connector(CONFIG_FILE)
    driver.startConnectorWaitTime()

    # -- Send 100 records with 'val' and 'c2' (c2 will be dropped by SMT) --
    values = [
        json.dumps({"val": str(i), "c2": "Suppose to be dropped."}).encode("utf-8")
        for i in range(RECORD_COUNT)
    ]
    driver.sendBytesData(topic, values)

    # -- Verify row count --
    wait_for_rows(topic, RECORD_COUNT)

    # -- Verify first row: only 'val' survives the SMT --
    meta_str, val_str = (
        driver.snowflake_conn.cursor()
        .execute(f"SELECT * FROM {topic} LIMIT 1")
        .fetchone()
    )

    assert json.loads(meta_str) == {
        "CreateTime": ANY_INT,
        "SnowflakeConnectorPushTime": ANY_INT,
        "offset": 0,
        "partition": 0,
        "topic": topic,
    }
    assert val_str == "0"
