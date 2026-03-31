"""E2E tests for Kafka Connector v4 iceberg Avro ingestion (via Schema Registry).

v4-only, confluent-only (requires Schema Registry for AvroConverter).

Tests the same schematization x validation matrix as the JSON iceberg tests,
but uses Avro-encoded records with Schema Registry.
"""

import json
import logging

import pytest
from confluent_kafka import avro

from lib.config_migration import V4_CONFIG_TEMPLATE
from lib.driver import KafkaDriver
from lib.matchers import ANY_INT

logger = logging.getLogger(__name__)

VALUE_SCHEMA = avro.loads(
    """
{
    "type": "record",
    "name": "iceberg_avro_value",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "body_temperature", "type": "double"},
        {"name": "name", "type": "string"}
    ]
}
"""
)

KEY_SCHEMA = avro.loads(
    """
{
    "type": "record",
    "name": "iceberg_avro_key",
    "fields": [
        {"name": "id", "type": "int"}
    ]
}
"""
)

RECORD_COUNT = 100


def _avro_connector_config(topic: str, schematization: bool, validation: bool) -> dict:
    config = {
        **V4_CONFIG_TEMPLATE,
        "tasks.max": "1",
        "key.converter": "io.confluent.connect.avro.AvroConverter",
        "key.converter.schema.registry.url": "CONFLUENT_SCHEMA_REGISTRY",
        "value.converter": "io.confluent.connect.avro.AvroConverter",
        "value.converter.schema.registry.url": "CONFLUENT_SCHEMA_REGISTRY",
        "snowflake.enable.schematization": str(schematization).lower(),
        "snowflake.validation": "client_side" if validation else "server_side",
        "topics": topic,
        "jmx": "true",
    }
    return config


@pytest.mark.iceberg
@pytest.mark.confluent_only
@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
@pytest.mark.parametrize(
    "schematization", [True, False], ids=["schema=on", "schema=off"]
)
@pytest.mark.parametrize("validation", [True, False], ids=["compat", "ht"])
def test_iceberg_avro_ingestion(
    driver: KafkaDriver,
    create_iceberg_table,
    create_topics,
    create_connector,
    wait_for_rows,
    validation: bool,
    schematization: bool,
):
    """Avro SR ingestion into an iceberg table — 2x2 matrix (validation x schematization).

    ``schema=off`` (bag-of-bits): table has RECORD_METADATA VARIANT, RECORD_CONTENT
    VARIANT.  Avro fields land in RECORD_CONTENT.

    ``schema=on`` (typed columns): table pre-declares ID, BODY_TEMPERATURE, NAME
    columns.  AvroConverter provides a Kafka Connect schema so the connector maps
    fields to the pre-declared columns directly.
    """
    val_tag = "compat" if validation else "ht"
    sch_tag = "s1" if schematization else "s0"
    base_name = f"iceberg_av_{val_tag}_{sch_tag}"

    if schematization:
        columns = (
            "(RECORD_METADATA VARIANT, ID BIGINT, BODY_TEMPERATURE DOUBLE, NAME TEXT)"
        )
    else:
        columns = "(RECORD_METADATA VARIANT, RECORD_CONTENT VARIANT)"

    table = create_iceberg_table(base_name, columns=columns, cleanup_topic=False)
    topic = create_topics([base_name], with_tables=False)[0]

    create_connector(
        v4_config=_avro_connector_config(
            topic, schematization=schematization, validation=validation
        )
    )
    driver.startConnectorWaitTime()

    keys = [{"id": i} for i in range(RECORD_COUNT)]
    values = [
        {"id": i, "body_temperature": 36.6, "name": "Steve"}
        for i in range(RECORD_COUNT)
    ]
    driver.sendAvroSRData(topic, values, VALUE_SCHEMA, keys, KEY_SCHEMA, partition=0)

    wait_for_rows(table.name, RECORD_COUNT)

    if not schematization:
        rows = table.select(
            "PARSE_JSON(RECORD_CONTENT):id::NUMBER            AS ID, "
            "PARSE_JSON(RECORD_CONTENT):body_temperature::FLOAT AS BODY_TEMPERATURE, "
            "PARSE_JSON(RECORD_CONTENT):name::STRING           AS NAME, "
            "PARSE_JSON(RECORD_METADATA):offset::NUMBER        AS OFFSET, "
            "PARSE_JSON(RECORD_METADATA):partition::NUMBER     AS PARTITION, "
            "PARSE_JSON(RECORD_METADATA):topic::STRING         AS TOPIC",
            "ORDER BY PARSE_JSON(RECORD_METADATA):offset::NUMBER LIMIT 1",
        )
        assert rows, "Expected at least one row"
        row = rows[0]
        assert row["ID"] == 0, f"Expected id=0, got {row['ID']}"
        assert abs(float(row["BODY_TEMPERATURE"]) - 36.6) < 0.01, (
            f"Expected body_temperature≈36.6, got {row['BODY_TEMPERATURE']}"
        )
        assert row["NAME"] == "Steve", f"Expected name='Steve', got {row['NAME']}"
        assert row["OFFSET"] == 0, f"Expected offset=0, got {row['OFFSET']}"
        assert row["PARTITION"] == 0, f"Expected partition=0, got {row['PARTITION']}"
        assert row["TOPIC"] == topic, f"Expected topic={topic!r}, got {row['TOPIC']!r}"
    else:
        rows = table.select(
            '"ID", "BODY_TEMPERATURE", "NAME", '
            "PARSE_JSON(RECORD_METADATA):offset::NUMBER    AS OFFSET, "
            "PARSE_JSON(RECORD_METADATA):partition::NUMBER AS PARTITION, "
            "PARSE_JSON(RECORD_METADATA):topic::STRING     AS TOPIC",
            "ORDER BY PARSE_JSON(RECORD_METADATA):offset::NUMBER LIMIT 1",
        )
        assert rows, "Expected at least one row"
        row = rows[0]
        assert row["ID"] == 0, f"Expected id=0, got {row['ID']}"
        assert abs(float(row["BODY_TEMPERATURE"]) - 36.6) < 0.01, (
            f"Expected body_temperature≈36.6, got {row['BODY_TEMPERATURE']}"
        )
        assert row["NAME"] == "Steve", f"Expected name='Steve', got {row['NAME']}"
        assert row["OFFSET"] == 0, f"Expected offset=0, got {row['OFFSET']}"
        assert row["PARTITION"] == 0, f"Expected partition=0, got {row['PARTITION']}"
        assert row["TOPIC"] == topic, f"Expected topic={topic!r}, got {row['TOPIC']!r}"

    # Verify RECORD_METADATA contains key (Avro key schema → key field in metadata)
    meta_rows = table.select(
        "PARSE_JSON(RECORD_METADATA) AS META",
        "ORDER BY PARSE_JSON(RECORD_METADATA):offset::NUMBER LIMIT 1",
    )
    metadata = json.loads(meta_rows[0]["META"])
    assert metadata["offset"] == 0
    assert metadata["partition"] == 0
    assert metadata["topic"] == topic
    assert metadata["SnowflakeConnectorPushTime"] == ANY_INT
