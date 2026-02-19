import json

FILE_NAME = "travis_correct_json_json"
CONFIG_FILE = f"{FILE_NAME}.json"
RECORD_COUNT = 100


def test_json_json(driver, name_salt, create_connector, snowflake_table, wait_for_rows):
    topic = snowflake_table(
        FILE_NAME,
        f"CREATE OR REPLACE TABLE {FILE_NAME}{name_salt} "
        f'(record_metadata variant, "number" varchar)',
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
    row = (
        driver.snowflake_conn.cursor()
        .execute(f"SELECT * FROM {topic} LIMIT 1")
        .fetchone()
    )
    gold_meta = r'{"SnowflakeConnectorPushTime":\d*,"key":{"number":"0"},"offset":0,"partition":0}'
    driver.regexMatchOneLine(row, gold_meta, r"0")
