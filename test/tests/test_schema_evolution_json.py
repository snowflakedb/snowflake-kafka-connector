"""Schema evolution: multiple topics, JSON, columns evolve (port from v3 test_suit)."""
import json

FILE_NAME = "travis_correct_schema_evolution_json"
CONFIG_FILE = f"{FILE_NAME}.json"
RECORD_NUM = 100
TOPIC_COUNT = 2

RECORDS = [
    {
        "PERFORMANCE_STRING": "Excellent",
        '"case_sensitive_PERFORMANCE_CHAR"': "A",
        "RATING_INT": 100,
    },
    {
        "PERFORMANCE_STRING": "Excellent",
        "RATING_DOUBLE": 0.99,
        "APPROVAL": True,
    },
]

GOLD_TYPE = {
    "PERFORMANCE_STRING": "VARCHAR",
    "case_sensitive_PERFORMANCE_CHAR": "VARCHAR",
    "RATING_INT": "NUMBER",
    "RATING_DOUBLE": "FLOAT",
    "APPROVAL": "BOOLEAN",
    "RECORD_METADATA": "VARIANT",
}


def test_schema_evolution_json(
    driver, name_salt, create_connector, snowflake_table, wait_for_rows
):
    table = snowflake_table(
        FILE_NAME,
        f"CREATE OR REPLACE TABLE {FILE_NAME}{name_salt} (PERFORMANCE_STRING STRING). "
        f"ALTER TABLE {FILE_NAME}{name_salt} SET ENABLE_SCHEMA_EVOLUTION = true",
    )

    for i in range(TOPIC_COUNT):
        driver.createTopics(f"{table}{i}", partitionNum=1, replicationNum=1)

    create_connector(CONFIG_FILE)
    driver.startConnectorWaitTime()

    # Send: 98 records + 2 tombstones per topic (tombstone ingestion enabled by default)
    for i in range(TOPIC_COUNT):
        key = []
        value = []
        for e in range(RECORD_NUM - 2):
            key.append(json.dumps({"number": str(e)}).encode("utf-8"))
            value.append(json.dumps(RECORDS[i]).encode("utf-8"))
        value.append(None)
        value.append(b"")
        key.append(json.dumps({"number": str(RECORD_NUM - 1)}).encode("utf-8"))
        key.append(json.dumps({"number": str(RECORD_NUM)}).encode("utf-8"))
        driver.sendBytesData(f"{table}{i}", value, key)

    wait_for_rows(table, TOPIC_COUNT * RECORD_NUM)

    rows = driver.snowflake_conn.cursor().execute(f"DESC TABLE {table}").fetchall()
    gold_columns = list(GOLD_TYPE.keys())
    for row in rows:
        gold_columns.remove(row[0])
        assert row[1].startswith(
            GOLD_TYPE[row[0]]
        ), f"Column {row[0]} has wrong type: got {row[1]}, expected {GOLD_TYPE[row[0]]}"
    assert not gold_columns, f"Columns not in table: {gold_columns}"

    count = (
        driver.snowflake_conn.cursor()
        .execute(f"SELECT count(*) FROM {table}")
        .fetchone()[0]
    )
    assert count == TOPIC_COUNT * RECORD_NUM, (
        f"Expected {TOPIC_COUNT * RECORD_NUM} rows, got {count}"
    )

    for i in range(TOPIC_COUNT):
        driver.deleteTopic(f"{table}{i}")

