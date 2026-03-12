"""Schema evolution: table dropped then recreated, schema evolution (port from v3 test_suit)."""
import json

FILE_NAME = "travis_correct_schema_evolution_drop_table"
CONFIG_FILE = f"{FILE_NAME}.json"
RECORD_NUM = 100

RECORD = {
    "PERFORMANCE_STRING": "Excellent",
    '"case_sensitive_PERFORMANCE_CHAR"': "A",
    "RATING_INT": 100,
}

GOLD_TYPE = {
    "PERFORMANCE_STRING": "VARCHAR",
    "case_sensitive_PERFORMANCE_CHAR": "VARCHAR",
    "RATING_INT": "NUMBER",
    "RECORD_METADATA": "VARIANT",
}


def test_schema_evolution_drop_table(
    driver, name_salt, create_connector, snowflake_table, wait_for_rows
):
    table = snowflake_table(
        FILE_NAME,
        f"CREATE OR REPLACE TABLE {FILE_NAME}{name_salt} (PERFORMANCE_STRING STRING). "
        f"ALTER TABLE {FILE_NAME}{name_salt} SET ENABLE_SCHEMA_EVOLUTION = true",
    )
    topic = f"{table}0"
    driver.createTopics(topic, partitionNum=1, replicationNum=1)

    create_connector(CONFIG_FILE)
    driver.startConnectorWaitTime()

    key = [json.dumps({"number": str(e)}).encode("utf-8") for e in range(RECORD_NUM)]
    value = [json.dumps(RECORD).encode("utf-8") for _ in range(RECORD_NUM)]
    driver.sendBytesData(topic, value, key)

    wait_for_rows(table, RECORD_NUM)

    # Verify schema after first batch
    rows = driver.snowflake_conn.cursor().execute(f"DESC TABLE {table}").fetchall()
    gold_columns = list(GOLD_TYPE.keys())
    for row in rows:
        gold_columns.remove(row[0])
        assert row[1].startswith(GOLD_TYPE[row[0]])
    assert not gold_columns

    # Recreate the table (simulate drop + recreate)
    driver.snowflake_conn.cursor().execute(
        f"CREATE OR REPLACE TABLE {table} (PERFORMANCE_STRING STRING, RECORD_METADATA VARIANT)"
    )
    driver.snowflake_conn.cursor().execute(
        f"ALTER TABLE {table} SET ENABLE_SCHEMA_EVOLUTION = true"
    )

    # Ingest another set of rows
    driver.sendBytesData(topic, value, key)

    wait_for_rows(table, RECORD_NUM * 2)

    rows = driver.snowflake_conn.cursor().execute(f"DESC TABLE {table}").fetchall()
    gold_columns = list(GOLD_TYPE.keys())
    for row in rows:
        gold_columns.remove(row[0])
        assert row[1].startswith(GOLD_TYPE[row[0]])
    assert not gold_columns

    count = (
        driver.snowflake_conn.cursor()
        .execute(f"SELECT count(*) FROM {table}")
        .fetchone()[0]
    )
    assert count == RECORD_NUM * 2

    driver.deleteTopic(topic)

