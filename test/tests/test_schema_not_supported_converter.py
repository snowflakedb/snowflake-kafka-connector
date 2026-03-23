import json
import time

FILE_NAME = "travis_correct_schema_not_supported_converter"
CONFIG_FILE = f"{FILE_NAME}.json"

RECORD = {
    "PERFORMANCE_STRING": "Excellent",
    '"case_sensitive_PERFORMANCE_CHAR"': "A",
    "PERFORMANCE_HEX": "FFFFFFFF",
    "RATING_INT": 100,
    "RATING_DOUBLE": 0.99,
    "APPROVAL": "true",
    "APPROVAL_DATE": "2022-06-15",
    "APPROVAL_TIME": "23:59:59.999999",
    "INFO_ARRAY": ["HELLO", "WORLD"],
    "INFO": {"TREE_1": "APPLE", "TREE_2": "PINEAPPLE"},
    "INFO_OBJECT": {"TREE_1": "APPLE", "TREE_2": "PINEAPPLE"},
}


def test_schema_not_supported_converter(
    driver, create_connector_from_file, create_table
):
    table = create_table(
        FILE_NAME,
        columns='(PERFORMANCE_STRING STRING, "case_sensitive_PERFORMANCE_CHAR" CHAR, '
        "PERFORMANCE_HEX BINARY, RATING_INT NUMBER, RATING_DOUBLE DOUBLE, "
        "APPROVAL BOOLEAN, APPROVAL_DATE DATE, APPROVAL_TIME TIME, "
        "INFO_ARRAY ARRAY, INFO VARIANT, INFO_OBJECT OBJECT)",
    )
    topic = table.name

    create_connector_from_file(CONFIG_FILE)
    driver.startConnectorWaitTime()

    # -- Send --
    keys = [json.dumps({"number": str(i)}).encode("utf-8") for i in range(100)]
    values = [json.dumps(RECORD).encode("utf-8") for _ in range(100)]
    driver.sendBytesData(topic, values, keys)

    # -- Verify: nothing should be ingested with unsupported converters --
    time.sleep(30)
    count = table.select_scalar("count(*)")
    assert count == 0, (
        f"Expected 0 rows but got {count}; unsupported converter should reject all records"
    )
