import json

from snowflake.connector import DictCursor

FILE_NAME = "nullable_values_after_smt"
CONFIG_FILE = f"{FILE_NAME}.json"
TOTAL_EVENTS = 200
EXPECTED_ROWS = 100  # only every-other event has optionalField


def test_nullable_values_after_smt(
    driver, name_salt, create_connector, snowflake_table, wait_for_rows
):
    topic = snowflake_table(
        FILE_NAME,
        f"CREATE OR REPLACE TABLE {FILE_NAME}{name_salt} "
        f"(index number, from_optional_field boolean, record_metadata variant)",
    )

    create_connector(CONFIG_FILE)
    driver.startConnectorWaitTime()

    # -- Send --
    # The connector config has an SMT that extracts only the optionalField sub-object.
    # Events without optionalField are dropped (behavior.on.null.values = IGNORE).
    values = []
    for idx in range(TOTAL_EVENTS):
        event = {"index": idx, "someKey": "someValue"}
        if idx % 2 == 0:
            event["optionalField"] = {"index": idx, "from_optional_field": True}
        values.append(json.dumps(event).encode("utf-8"))

    driver.sendBytesData(topic, values)

    # -- Verify row count --
    wait_for_rows(topic, EXPECTED_ROWS)

    # -- Verify content --
    rows = (
        driver.snowflake_conn.cursor(DictCursor)
        .execute(
            f"SELECT index, from_optional_field, "
            f"record_metadata:offset::number AS offset "
            f"FROM {topic}"
        )
        .fetchall()
    )

    parsed = [
        {
            "index": r["INDEX"],
            "from_optional_field": r["FROM_OPTIONAL_FIELD"],
            "offset": r["OFFSET"],
        }
        for r in rows
    ]
    expected = [
        {"index": idx, "from_optional_field": True, "offset": idx}
        for idx in range(0, TOTAL_EVENTS, 2)
    ]
    assert parsed == expected
