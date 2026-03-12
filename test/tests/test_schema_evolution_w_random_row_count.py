"""Schema evolution: random row count (alter table invalidation mid-batch) (port from v3 test_suit)."""
import json
import random

FILE_NAME = "test_schema_evolution_w_random_row_count"
CONFIG_FILE = f"{FILE_NAME}.json"
TOPIC_COUNT = 2
FLUSH_RECORD_COUNT = 300

RECORDS = [
    {"PERFORMANCE_STRING": "Excellent", "PERFORMANCE_CHAR": "A", "RATING_INT": 100},
    {"PERFORMANCE_STRING": "Excellent", "RATING_DOUBLE": 0.99, "APPROVAL": True},
]

GOLD_TYPE = {
    "PERFORMANCE_STRING": "VARCHAR",
    "PERFORMANCE_CHAR": "VARCHAR",
    "RATING_INT": "NUMBER",
    "RATING_DOUBLE": "FLOAT",
    "APPROVAL": "BOOLEAN",
    "RECORD_METADATA": "VARIANT",
}


def test_schema_evolution_w_random_row_count(
    driver, name_salt, create_connector, snowflake_table, wait_for_rows
):
    initial_record_count = random.randrange(1, 300)
    record_num = initial_record_count + FLUSH_RECORD_COUNT

    table = snowflake_table(
        FILE_NAME,
        f"CREATE OR REPLACE TABLE {FILE_NAME}{name_salt} (PERFORMANCE_STRING STRING). "
        f"ALTER TABLE {FILE_NAME}{name_salt} SET ENABLE_SCHEMA_EVOLUTION = true",
    )

    for i in range(TOPIC_COUNT):
        driver.createTopics(f"{table}{i}", partitionNum=1, replicationNum=1)

    create_connector(CONFIG_FILE)
    driver.startConnectorWaitTime()

    for i in range(TOPIC_COUNT):
        key = [json.dumps({"number": str(e)}).encode("utf-8") for e in range(initial_record_count)]
        value = [json.dumps(RECORDS[i]).encode("utf-8") for _ in range(initial_record_count)]
        driver.sendBytesData(f"{table}{i}", value, key)
        key = [json.dumps({"number": str(e)}).encode("utf-8") for e in range(FLUSH_RECORD_COUNT)]
        value = [json.dumps(RECORDS[i]).encode("utf-8") for _ in range(FLUSH_RECORD_COUNT)]
        driver.sendBytesData(f"{table}{i}", value, key)

    wait_for_rows(table, TOPIC_COUNT * record_num)

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
    assert count == TOPIC_COUNT * record_num

    for i in range(TOPIC_COUNT):
        driver.deleteTopic(f"{table}{i}")

