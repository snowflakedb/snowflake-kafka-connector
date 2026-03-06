import json

FILE_NAME = "travis_correct_native_string_json_without_schema"
CONFIG_FILE = f"{FILE_NAME}.json"
RECORD_COUNT = 100


def test_native_string_json_without_schema(
    driver, name_salt, create_connector, snowflake_table, wait_for_rows
):
    """Verify that an SMT (ReplaceField$Value blacklisting 'c2') drops the c2
    field before ingestion, leaving only the 'number' field.

    Connector config uses StringConverter key + JsonConverter value with a
    ReplaceField transform that removes 'c2'.
    """
    topic = snowflake_table(
        FILE_NAME,
        f"CREATE OR REPLACE TABLE {FILE_NAME}{name_salt} "
        f'(record_metadata variant, "number" varchar)',
    )

    create_connector(CONFIG_FILE)
    driver.startConnectorWaitTime()

    # -- Send 100 records with 'number' and 'c2' (c2 will be dropped by SMT) --
    values = [
        json.dumps({"number": str(i), "c2": "Suppose to be dropped."}).encode("utf-8")
        for i in range(RECORD_COUNT)
    ]
    driver.sendBytesData(topic, values)

    # -- Verify row count --
    wait_for_rows(topic, RECORD_COUNT)

    # -- Verify first row: only 'number' survives the SMT --
    row = (
        driver.snowflake_conn.cursor()
        .execute(f"SELECT * FROM {topic} LIMIT 1")
        .fetchone()
    )

    gold_meta = (
        r'{"CreateTime":\d*,"SnowflakeConnectorPushTime":\d*,'
        r'"offset":0,"partition":0,'
        r'"topic":"travis_correct_native_string_json_without_schema_\w*"}'
    )
    driver.regexMatchOneLine(row, gold_meta, r"0")
