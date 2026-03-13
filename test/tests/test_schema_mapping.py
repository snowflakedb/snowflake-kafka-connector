import datetime
import json

import pytest

FILE_NAME = "travis_correct_schema_mapping"
CONFIG_FILE = f"{FILE_NAME}.json"
RECORD_COUNT = 100

RECORD = {
    "PERFORMANCE_STRING": "Excellent",
    # KCv3 used embedded quotes ('"case_sensitive..."') because the custom
    # SnowflakeJsonConverter stripped them.  The standard JsonConverter
    # preserves the key as-is, so we omit the embedded quotes.
    "case_sensitive_PERFORMANCE_CHAR": "A",
    "PERFORMANCE_HEX": "FFFFFFFF",
    "RATING_INT": 100,
    "RATING_DOUBLE": 0.99,
    "APPROVAL": True,
    "APPROVAL_DATE": "2022-06-15",
    "APPROVAL_TIME": "23:59:59.999999",
    "INFO_ARRAY": ["HELLO", "WORLD"],
    "INFO": {"TREE_1": "APPLE", "TREE_2": "PINEAPPLE"},
    "INFO_OBJECT": {"TREE_1": "APPLE", "TREE_2": "PINEAPPLE"},
}

GOLD_VALUES = {
    "PERFORMANCE_STRING": "Excellent",
    "case_sensitive_PERFORMANCE_CHAR": "A",
    "PERFORMANCE_HEX": b"\xff\xff\xff\xff",
    "RATING_INT": 100,
    "RATING_DOUBLE": 0.99,
    "APPROVAL": True,
    "APPROVAL_DATE": datetime.date(2022, 6, 15),
    "APPROVAL_TIME": datetime.time(23, 59, 59, 999999),
    "INFO_ARRAY": r'["HELLO","WORLD"]',
    "INFO": r'{"TREE_1":"APPLE","TREE_2":"PINEAPPLE"}',
    "INFO_OBJECT": r'{"TREE_1":"APPLE","TREE_2":"PINEAPPLE"}',
}


@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
def test_schema_mapping(
    driver, name_salt, create_connector, snowflake_table, wait_for_rows
):
    """Verify that each data type maps to the correct Snowflake column type
    and that RECORD_METADATA is automatically added.

    Tests STRING, CHAR, BINARY, NUMBER, DOUBLE, BOOLEAN, DATE, TIME,
    ARRAY, VARIANT, and OBJECT columns.
    """
    topic = snowflake_table(
        FILE_NAME,
        f"CREATE OR REPLACE TABLE {FILE_NAME}{name_salt} ("
        f"PERFORMANCE_STRING STRING, "
        f'"case_sensitive_PERFORMANCE_CHAR" CHAR, '
        f"PERFORMANCE_HEX BINARY, "
        f"RATING_INT NUMBER, "
        f"RATING_DOUBLE DOUBLE, "
        f"APPROVAL BOOLEAN, "
        f"APPROVAL_DATE DATE, "
        f"APPROVAL_TIME TIME, "
        f"INFO_ARRAY ARRAY, "
        f"INFO VARIANT, "
        f"INFO_OBJECT OBJECT, "
        f"RECORD_METADATA VARIANT"
        f")",
    )

    # TODO: SNOW-3236195: RowValidator uppercases unquoted column names via
    # LiteralQuoteUtils.unquoteColumnName(), but DESCRIBE TABLE preserves case for
    # quoted columns (e.g. "case_sensitive_PERFORMANCE_CHAR"). This causes a false
    # structural error. Fix by normalizing both sides in RowValidator.
    create_connector(
        CONFIG_FILE,
        config_overrides={"snowflake.client.validation.enabled": "false"},
    )
    driver.startConnectorWaitTime()

    # -- Send --
    keys = [json.dumps({"number": str(i)}).encode("utf-8") for i in range(RECORD_COUNT)]
    values = [json.dumps(RECORD).encode("utf-8") for _ in range(RECORD_COUNT)]
    driver.sendBytesData(topic, values, keys)

    # -- Verify row count --
    wait_for_rows(topic, RECORD_COUNT)

    # -- Build column index map --
    col_info = driver.snowflake_conn.cursor().execute(f"DESC TABLE {topic}").fetchall()

    col_map = {}
    for idx, col in enumerate(col_info):
        col_map[col[0]] = idx

    # -- Verify content of first row --
    row = (
        driver.snowflake_conn.cursor()
        .execute(f"SELECT * FROM {topic} LIMIT 1")
        .fetchone()
    )

    for field, gold in GOLD_VALUES.items():
        idx = col_map[field]
        actual = row[idx]
        if isinstance(actual, str):
            # Remove formatting whitespace added by Snowflake
            assert "".join(actual.split()) == gold, (
                f"Column {field}: expected {gold!r}, got {actual!r}"
            )
        else:
            assert actual == gold, f"Column {field}: expected {gold!r}, got {actual!r}"
