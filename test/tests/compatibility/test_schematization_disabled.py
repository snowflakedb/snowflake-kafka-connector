import json

from lib.config_migration import V3_CONFIG_TEMPLATE
from lib.fixtures.table import Table
from lib.driver import KafkaDriver


def test_compatibility_schematization_disabled_complex(
    driver: KafkaDriver, create_connector, create_topics, wait_for_rows
):
    """Nested JSON data lands as queryable VARIANT in RECORD_CONTENT.

    Table is NOT pre-created — the connector auto-creates it.
    KCv3 auto-creates with (RECORD_METADATA VARIANT, RECORD_CONTENT VARIANT).
    KCv4 auto-creates with both columns as VARIANT when schematization=off.

    Runs for both v3 and v4 to verify compatibility.
    """
    topic = create_topics(["schematization_disabled_complex"], with_tables=False)[0]

    connector = create_connector(
        v3_config={
            **V3_CONFIG_TEMPLATE,
            "topics": topic,
            "key.converter": "org.apache.kafka.connect.storage.StringConverter",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter.schemas.enable": "false",
            "snowflake.enable.schematization": "false",
        }
    )
    driver.startConnectorWaitTime()

    values = [
        # 0: nested object with arrays
        json.dumps(
            {
                "user": {"name": "Alice", "scores": [1, 2, 3]},
                "tags": ["a", "b"],
                "count": 42,
            }
        ).encode("utf-8"),
        # 1: deeply nested
        json.dumps({"a": {"b": {"c": {"d": "deep"}}}}).encode("utf-8"),
        # 2: flat object
        json.dumps({"city": "Hsinchu", "age": 30}).encode("utf-8"),
    ]
    record_count = len(values)
    driver.sendBytesData(topic, values, [], partition=0)

    table = Table(driver, topic.upper())
    wait_for_rows(table.name, record_count, connector_name=connector.name)

    # Verify RECORD_CONTENT column exists and is VARIANT
    schema = table.schema(as_dict=True)
    col_schema = next(c for c in schema if c["name"] == "RECORD_CONTENT")
    assert col_schema["type"] == "VARIANT"

    # Verify nested object with arrays (offset 0)
    assert table.select(
        """
        RECORD_CONTENT:user.name::string AS user_name,
        RECORD_CONTENT:user.scores[0]::number AS first_score,
        RECORD_CONTENT:tags[0]::string AS first_tag,
        RECORD_CONTENT:count::number AS cnt
    """,
        'WHERE RECORD_METADATA:"offset"::number = 0',
    ) == [
        {
            "USER_NAME": "Alice",
            "FIRST_SCORE": 1,
            "FIRST_TAG": "a",
            "CNT": 42,
        }
    ]

    # Verify deeply nested (offset 1)
    assert table.select(
        """
        RECORD_CONTENT:a.b.c.d::string AS val
    """,
        'WHERE RECORD_METADATA:"offset"::number = 1',
    ) == [
        {
            "VAL": "deep",
        }
    ]

    # Verify flat object (offset 2)
    assert table.select(
        """
        RECORD_CONTENT:city::string AS city,
        RECORD_CONTENT:age::number AS age
    """,
        'WHERE RECORD_METADATA:"offset"::number = 2',
    ) == [
        {
            "CITY": "Hsinchu",
            "AGE": 30,
        }
    ]

    assert table.select_scalar("count(*)") == record_count


def test_compatibility_schematization_disabled_primitive(
    driver: KafkaDriver, create_connector, create_topics, wait_for_rows
):
    """Bare strings via StringConverter land as VARIANT in RECORD_CONTENT.

    Table is NOT pre-created — the connector auto-creates it.
    Verifies that primitive (non-JSON) payloads are stored as VARIANT,
    not inferred as VARCHAR by schema evolution.

    Runs for both v3 and v4 to verify compatibility.
    """
    topic = create_topics(["schematization_disabled_primitive"], with_tables=False)[0]

    connector = create_connector(
        v3_config={
            **V3_CONFIG_TEMPLATE,
            "topics": topic,
            "key.converter": "org.apache.kafka.connect.storage.StringConverter",
            "value.converter": "org.apache.kafka.connect.storage.StringConverter",
            "snowflake.enable.schematization": "false",
        }
    )
    driver.startConnectorWaitTime()

    values = [
        b"hello world",
        b"42",
        b"true",
    ]
    driver.sendBytesData(topic, values, [], partition=0)

    table = Table(driver, topic.upper())
    wait_for_rows(table.name, len(values), connector_name=connector.name)

    schema = table.schema(as_dict=True)
    col_schema = next(c for c in schema if c["name"] == "RECORD_CONTENT")
    assert col_schema["type"] == "VARIANT"

    rows = table.select(
        "RECORD_CONTENT::string AS content",
        'ORDER BY RECORD_METADATA:"offset"::number',
    )
    assert rows[0]["CONTENT"] == "hello world"
    assert rows[1]["CONTENT"] == "42"
    assert rows[2]["CONTENT"] == "true"

    assert table.select_scalar("count(*)") == len(values)
