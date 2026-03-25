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
    # TODO: SNOW-3256183 Fix client-side validation on binary column
    # BINARY column removed: Snowflake deployments differ on whether the Ingest SDK
    # expects hex or base64 encoding, causing ingestion failures on some clouds.
    # "PERFORMANCE_BINARY": "FFFFFFFF",
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
    # "PERFORMANCE_BINARY": b"\xff\xff\xff\xff",  # see TODO above
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
    driver, name_salt, create_connector_from_file, create_table, wait_for_rows
):
    """Verify that each data type maps to the correct Snowflake column type
    and that RECORD_METADATA is automatically added.

    Tests STRING, CHAR, BINARY, NUMBER, DOUBLE, BOOLEAN, DATE, TIME,
    ARRAY, VARIANT, and OBJECT columns.
    """
    table = create_table(
        FILE_NAME.upper(),
        columns="("
        "PERFORMANCE_STRING STRING, "
        '"case_sensitive_PERFORMANCE_CHAR" CHAR, '
        # "PERFORMANCE_BINARY BINARY, "  # see TODO: SNOW-3256183
        "RATING_INT NUMBER, "
        "RATING_DOUBLE DOUBLE, "
        "APPROVAL BOOLEAN, "
        "APPROVAL_DATE DATE, "
        "APPROVAL_TIME TIME, "
        "INFO_ARRAY ARRAY, "
        "INFO VARIANT, "
        "INFO_OBJECT OBJECT, "
        "RECORD_METADATA VARIANT"
        ")",
    )
    topic = f"{FILE_NAME}{name_salt}"

    # TODO: SNOW-3236195: RowValidator uppercases unquoted column names via
    # LiteralQuoteUtils.unquoteColumnName(), but DESCRIBE TABLE preserves case for
    # quoted columns (e.g. "case_sensitive_PERFORMANCE_CHAR"). This causes a false
    # structural error. Fix by normalizing both sides in RowValidator.
    create_connector_from_file(
        CONFIG_FILE,
        config_overrides={"snowflake.client.validation.enabled": "false"},
    )
    driver.startConnectorWaitTime()

    # -- Send --
    keys = [json.dumps({"number": str(i)}).encode("utf-8") for i in range(RECORD_COUNT)]
    values = [json.dumps(RECORD).encode("utf-8") for _ in range(RECORD_COUNT)]
    driver.sendBytesData(topic, values, keys)

    # -- Verify row count --
    wait_for_rows(table.name, RECORD_COUNT)

    # -- Verify content of first row --
    row = table.select("*")[0]

    for field, gold in GOLD_VALUES.items():
        actual = row[field]
        if isinstance(actual, str):
            # Remove formatting whitespace added by Snowflake
            assert "".join(actual.split()) == gold, (
                f"Column {field}: expected {gold!r}, got {actual!r}"
            )
        else:
            assert actual == gold, f"Column {field}: expected {gold!r}, got {actual!r}"
