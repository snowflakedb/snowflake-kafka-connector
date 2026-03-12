"""Schema evolution: non-nullable JSON columns, evolved columns nullable (port from v3 test_suit)."""
import json

FILE_NAME = "travis_correct_schema_evolution_nonnullable_json"
CONFIG_FILE = f"{FILE_NAME}.json"
RECORD_NUM = 100
TOPIC_COUNT = 1

RECORDS = [{'"case_sensitive_PERFORMANCE_CHAR"': "A", "RATING_INT": 100}]

GOLD_TYPE = {
    "case_sensitive_PERFORMANCE_STRING": "VARCHAR",
    "case_sensitive_PERFORMANCE_CHAR": "VARCHAR",
    "RATING_INT": "NUMBER",
    "RECORD_METADATA": "VARIANT",
}


def test_schema_evolution_nonnullable_json(
    driver, name_salt, create_connector, snowflake_table, wait_for_rows
):
    # Initial table has one NOT NULL column; schema evolution adds nullable columns
    table = snowflake_table(
        FILE_NAME,
        f'CREATE OR REPLACE TABLE {FILE_NAME}{name_salt} '
        f'("case_sensitive_PERFORMANCE_STRING" STRING NOT NULL). '
        f"ALTER TABLE {FILE_NAME}{name_salt} SET ENABLE_SCHEMA_EVOLUTION = true",
    )

    driver.createTopics(f"{table}0", partitionNum=1, replicationNum=1)

    create_connector(CONFIG_FILE)
    driver.startConnectorWaitTime()

    key = [json.dumps({"number": str(e)}).encode("utf-8") for e in range(RECORD_NUM)]
    value = [json.dumps(RECORDS[0]).encode("utf-8") for _ in range(RECORD_NUM)]
    driver.sendBytesData(f"{table}0", value, key)

    wait_for_rows(table, RECORD_NUM)

    rows = driver.snowflake_conn.cursor().execute(f"DESC TABLE {table}").fetchall()
    gold_columns = list(GOLD_TYPE.keys())
    for row in rows:
        gold_columns.remove(row[0])
        assert row[1].startswith(GOLD_TYPE[row[0]])
        # All columns should be nullable (schema evolution adds nullable columns)
        assert row[3] == "Y", f"Column {row[0]} is non-nullable"
    assert not gold_columns

    count = (
        driver.snowflake_conn.cursor()
        .execute(f"SELECT count(*) FROM {table}")
        .fetchone()[0]
    )
    assert count == RECORD_NUM

    driver.deleteTopic(f"{table}0")

