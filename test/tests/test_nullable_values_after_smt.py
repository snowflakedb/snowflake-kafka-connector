import json

FILE_NAME = "nullable_values_after_smt"
CONFIG_FILE = f"{FILE_NAME}.json"
TOTAL_EVENTS = 200
EXPECTED_ROWS = 100  # only every-other event has optionalField


def test_nullable_values_after_smt(
    driver,
    name_salt,
    create_connector_from_file,
    create_table,
    wait_for_rows,
):
    table = create_table(
        FILE_NAME.upper(),
        columns="(index number, from_optional_field boolean, record_metadata variant)",
    )
    topic = f"{FILE_NAME}{name_salt}"

    create_connector_from_file(CONFIG_FILE)
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
    wait_for_rows(table.name, EXPECTED_ROWS)

    # -- Verify content --
    # ORDER BY offset: Snowflake does not guarantee row order without it, and
    # the assertion below compares against an offset-ordered expected list.
    rows = table.select(
        "index, from_optional_field, record_metadata:offset::number AS offset",
        "ORDER BY offset",
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
