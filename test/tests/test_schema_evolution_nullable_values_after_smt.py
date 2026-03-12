"""Schema evolution: SMT returns nulls, schema evolution (port from v3 test_suit)."""
import json

from snowflake.connector import DictCursor

FILE_NAME = "schema_evolution_nullable_values_after_smt"
CONFIG_FILE = f"{FILE_NAME}.json"
TOTAL_EVENTS = 200
EXPECTED_ROWS = 100  # only every-other event has optionalField

GOLD_TYPE = {
    "INDEX": {"type": "NUMBER(38,0)", "null?": "N"},
    "FROM_OPTIONAL_FIELD": {"type": "BOOLEAN", "null?": "Y"},
    "RECORD_METADATA": {"type": "VARIANT", "null?": "Y"},
}


def test_schema_evolution_nullable_values_after_smt(
    driver, name_salt, create_connector, snowflake_table, wait_for_rows
):
    topic = snowflake_table(
        FILE_NAME,
        f"CREATE OR REPLACE TABLE {FILE_NAME}{name_salt} (index number NOT NULL). "
        f"ALTER TABLE {FILE_NAME}{name_salt} SET ENABLE_SCHEMA_EVOLUTION = true",
    )

    driver.createTopics(topic, partitionNum=1, replicationNum=1)
    create_connector(CONFIG_FILE)
    driver.startConnectorWaitTime()

    values = []
    for idx in range(TOTAL_EVENTS):
        event = {"index": idx, "someKey": "someValue"}
        if idx % 2 == 0:
            event["optionalField"] = {"index": idx, "from_optional_field": True}
        values.append(json.dumps(event).encode("utf-8"))

    driver.sendBytesData(topic, values)

    wait_for_rows(topic, EXPECTED_ROWS)

    # Verify table schema (types and nullability)
    res = (
        driver.snowflake_conn.cursor(DictCursor)
        .execute(f"DESCRIBE TABLE {topic}")
        .fetchall()
    )
    assert len(res) == len(GOLD_TYPE)
    for col in res:
        name = col["name"]
        expected = GOLD_TYPE[name]
        assert expected["type"] == col["type"], (
            f"Invalid type for {name}: expected {expected['type']}, got {col['type']}"
        )
        assert expected["null?"] == col["null?"], (
            f"Invalid nullability for {name}: expected {expected['null?']}, got {col['null?']}"
        )

    # Verify content
    rows = (
        driver.snowflake_conn.cursor(DictCursor)
        .execute(
            f"SELECT index, from_optional_field, "
            f"record_metadata:offset::number AS offset FROM {topic}"
        )
        .fetchall()
    )
    parsed = [
        {"index": r["INDEX"], "from_optional_field": r["FROM_OPTIONAL_FIELD"], "offset": r["OFFSET"]}
        for r in rows
    ]
    expected = [
        {"index": idx, "from_optional_field": True, "offset": idx}
        for idx in range(0, TOTAL_EVENTS, 2)
    ]
    assert parsed == expected

