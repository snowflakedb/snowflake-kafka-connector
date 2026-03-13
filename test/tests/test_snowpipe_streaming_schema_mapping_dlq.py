import json

import pytest

FILE_NAME = "snowpipe_streaming_schema_mapping_dlq"
CONFIG_FILE = f"{FILE_NAME}.json"
RECORDS_PER_TYPE = 10

# Correct records are ingested; incorrect records go to DLQ
EXPECTED_IN_TABLE = RECORDS_PER_TYPE  # only correct records
EXPECTED_IN_DLQ = 2 * RECORDS_PER_TYPE  # two types of incorrect records


@pytest.mark.skip(reason="Requires client-side validation")
def test_snowpipe_streaming_schema_mapping_dlq(
    driver, name_salt, create_connector, snowflake_table, wait_for_rows
):
    """Verify that schema mapping errors route failing records to the DLQ
    while correct records are ingested normally.

    Three types of records are sent:
      1. Incorrect: string value in a NUMBER column (not parseable)
      2. Incorrect: array where an object is expected
      3. Correct: proper types

    Only type (3) should land in the table. Types (1) and (2) go to DLQ.
    """
    topic = snowflake_table(
        FILE_NAME,
        f"CREATE OR REPLACE TABLE {FILE_NAME}{name_salt} "
        f"(PERFORMANCE_STRING STRING, RATING_INT NUMBER, RECORD_METADATA VARIANT)",
    )

    config = create_connector(CONFIG_FILE)
    driver.startConnectorWaitTime()

    # -- Send incorrect data (string in NUMBER column) --
    incorrect_record = {"PERFORMANCE_STRING": "Excellent", "RATING_INT": "NO-a-NO"}
    _send_records(driver, topic, incorrect_record, RECORDS_PER_TYPE)

    # -- Send incorrect data (array instead of object) --
    another_incorrect = [{"PERFORMANCE_STRING": "Excellent", "RATING_INT": 100}]
    _send_records(driver, topic, another_incorrect, RECORDS_PER_TYPE)

    # -- Send correct data --
    correct_record = {"PERFORMANCE_STRING": "Excellent", "RATING_INT": 100}
    _send_records(driver, topic, correct_record, RECORDS_PER_TYPE)

    # -- Verify correct records landed in table --
    wait_for_rows(topic, EXPECTED_IN_TABLE)

    # -- Build column index map --
    col_info = driver.snowflake_conn.cursor().execute(f"DESC TABLE {topic}").fetchall()

    col_map = {}
    for idx, col in enumerate(col_info):
        col_map[col[0]] = idx

    # -- Verify DLQ received failing records --
    offsets_in_dlq = driver.consume_messages_dlq(config, 0, EXPECTED_IN_DLQ - 1)
    assert offsets_in_dlq == EXPECTED_IN_DLQ, (
        f"Expected {EXPECTED_IN_DLQ} records in DLQ, got {offsets_in_dlq}"
    )

    # -- Verify content of ingested rows --
    row = (
        driver.snowflake_conn.cursor()
        .execute(f"SELECT * FROM {topic} LIMIT 1")
        .fetchone()
    )

    for field, gold in {"PERFORMANCE_STRING": "Excellent", "RATING_INT": 100}.items():
        idx = col_map[field]
        actual = row[idx]
        if isinstance(actual, str):
            assert "".join(actual.split()) == gold, (
                f"Column {field}: expected {gold!r}, got {actual!r}"
            )
        else:
            assert actual == gold, f"Column {field}: expected {gold!r}, got {actual!r}"


def _send_records(driver, topic, record, count):
    keys = [json.dumps({"number": str(i)}).encode("utf-8") for i in range(count)]
    values = [json.dumps(record).encode("utf-8") for _ in range(count)]
    driver.sendBytesData(topic, values, keys)
