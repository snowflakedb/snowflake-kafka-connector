"""Schema evolution: behavior.on.null.values=IGNORE, tombstones (port from v3 test_suit)."""
import json

FILE_NAME = "test_schema_evolution_json_ignore_tombstone"
CONFIG_FILE = f"{FILE_NAME}.json"
RECORD_NUM = 100
TOPIC_COUNT = 2
# With IGNORE tombstones, we send (RECORD_NUM - 2) data records + 2 tombstones per topic; only data rows ingested
EXPECTED_ROWS_PER_TOPIC = RECORD_NUM - 2

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


def test_schema_evolution_json_ignore_tombstone(
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

    for i in range(TOPIC_COUNT):
        key = []
        value = []
        for e in range(RECORD_NUM - 2):
            key.append(json.dumps({"number": str(e)}).encode("utf-8"))
            value.append(json.dumps(RECORDS[i]).encode("utf-8"))
        key.append(json.dumps({"number": str(RECORD_NUM - 1)}).encode("utf-8"))
        value.append(None)
        key.append(json.dumps({"number": str(RECORD_NUM)}).encode("utf-8"))
        value.append(b"")
        driver.sendBytesData(f"{table}{i}", value, key)

    wait_for_rows(table, TOPIC_COUNT * EXPECTED_ROWS_PER_TOPIC)

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
    assert count == TOPIC_COUNT * EXPECTED_ROWS_PER_TOPIC

    for i in range(TOPIC_COUNT):
        driver.deleteTopic(f"{table}{i}")

