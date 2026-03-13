import json

from lib.matchers import ANY_INT, RegexMatch

FILE_NAME = "travis_correct_native_complex_smt"
CONFIG_FILE = f"{FILE_NAME}.json"
RECORD_COUNT = 100


def test_native_complex_smt(
    driver, name_salt, create_connector, snowflake_table, wait_for_rows
):
    """Verify a complex SMT chain: ValueToKey + ExtractField$Key + ReplaceField$Value.

    Connector config transforms:
      1. ValueToKey(fields=c1) — copies c1 into the key
      2. ExtractField$Key(field=c1) — extracts c1 as the key
      3. ReplaceField$Value(blacklist=c2) — drops c2 from the value

    After transforms, the key holds the c1 value and the value retains only c1.
    """
    topic = snowflake_table(
        FILE_NAME,
        f"CREATE OR REPLACE TABLE {FILE_NAME}{name_salt} "
        f"(record_metadata variant, c1 variant)",
    )

    create_connector(CONFIG_FILE)
    driver.startConnectorWaitTime()

    # -- Send 100 records --
    values = [
        json.dumps({"c1": {"int": str(i)}, "c2": "Suppose to be dropped."}).encode(
            "utf-8"
        )
        for i in range(RECORD_COUNT)
    ]
    driver.sendBytesData(topic, values)

    # -- Verify row count --
    wait_for_rows(topic, RECORD_COUNT)

    # -- Verify first row: key extracted, c2 dropped --
    meta_str, c1_str = (
        driver.snowflake_conn.cursor()
        .execute(f"SELECT * FROM {topic} LIMIT 1")
        .fetchone()
    )

    assert json.loads(meta_str) == {
        "CreateTime": ANY_INT,
        "SnowflakeConnectorPushTime": ANY_INT,
        "key": {"int": RegexMatch(r"\d+")},
        "offset": 0,
        "partition": 0,
        "topic": topic,
    }
    assert json.loads(c1_str) == {"int": RegexMatch(r"\d+")}
