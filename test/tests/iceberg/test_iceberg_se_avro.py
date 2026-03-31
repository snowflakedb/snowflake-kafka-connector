"""Iceberg schema evolution E2E tests — Avro format (via Schema Registry).

Tests client-side SE with Avro-encoded records: the connector detects new
columns from the Avro schema and issues ``ALTER ICEBERG TABLE ADD COLUMN``.

v4-only, confluent-only.
"""

import logging

import pytest
from confluent_kafka import avro

from lib.config_migration import V4_CONFIG_TEMPLATE
from lib.driver import KafkaDriver

logger = logging.getLogger(__name__)

WAVE1_SCHEMA = avro.loads(
    """
{
    "type": "record",
    "name": "wave1",
    "fields": [
        {"name": "CITY", "type": "string"},
        {"name": "AGE", "type": "int"}
    ]
}
"""
)

WAVE2_SCHEMA = avro.loads(
    """
{
    "type": "record",
    "name": "wave2",
    "fields": [
        {"name": "CITY", "type": "string"},
        {"name": "AGE", "type": "int"},
        {"name": "COUNTRY", "type": "string"}
    ]
}
"""
)


def _avro_se_connector_config(topic: str) -> dict:
    return {
        **V4_CONFIG_TEMPLATE,
        "tasks.max": "1",
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "io.confluent.connect.avro.AvroConverter",
        "value.converter.schema.registry.url": "CONFLUENT_SCHEMA_REGISTRY",
        "snowflake.enable.schematization": "true",
        "snowflake.validation": "client_side",
        "topics": topic,
        "jmx": "true",
    }


@pytest.mark.iceberg
@pytest.mark.schema_evolution
@pytest.mark.confluent_only
@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
def test_iceberg_se_avro_add_column(
    driver: KafkaDriver,
    create_iceberg_table,
    create_topics,
    create_connector,
    wait_for_rows,
):
    """Iceberg SE with Avro — connector adds columns from evolving Avro schemas.

    Table starts with RECORD_METADATA VARIANT + CITY TEXT.  Wave 1 sends Avro
    records with ``{CITY, AGE}`` — connector SE adds AGE.  Wave 2 uses an
    evolved Avro schema with ``{CITY, AGE, COUNTRY}`` — connector SE adds COUNTRY.

    Avro has an explicit schema so the connector knows the exact type for each
    new column (unlike JSON where types are inferred from values).
    """
    base_name = "iceberg_se_avro"
    table = create_iceberg_table(
        base_name,
        columns="(RECORD_METADATA VARIANT, CITY TEXT) ENABLE_SCHEMA_EVOLUTION = TRUE",
        cleanup_topic=False,
    )
    topic = create_topics([base_name], with_tables=False)[0]

    create_connector(v4_config=_avro_se_connector_config(topic))
    driver.startConnectorWaitTime()

    wave1_count = 100
    wave1_values = [{"CITY": "Hsinchu", "AGE": i} for i in range(wave1_count)]
    driver.sendAvroSRData(topic, wave1_values, WAVE1_SCHEMA, partition=0)

    wait_for_rows(table.name, wave1_count)

    cols = {row[0] for row in table.schema()}
    assert "AGE" in cols, (
        f"Expected connector SE to add AGE column after wave 1, got: {cols}"
    )

    wave2_count = 50
    wave2_values = [
        {"CITY": "Taipei", "AGE": 100 + i, "COUNTRY": "TW"} for i in range(wave2_count)
    ]
    driver.sendAvroSRData(topic, wave2_values, WAVE2_SCHEMA, partition=0)

    wait_for_rows(table.name, wave1_count + wave2_count)

    cols = {row[0] for row in table.schema()}
    assert "COUNTRY" in cols, (
        f"Expected connector SE to add COUNTRY column after wave 2, got: {cols}"
    )

    rows = table.select('"CITY", "COUNTRY"', "WHERE \"CITY\" = 'Taipei' LIMIT 1")
    assert rows, "Expected at least one wave-2 row with CITY = 'Taipei'"
    assert rows[0]["CITY"] == "Taipei"
    assert rows[0]["COUNTRY"] == "TW", (
        f"Expected COUNTRY='TW', got {rows[0]['COUNTRY']!r}"
    )

    null_country_count = table.select("COUNT(*) AS CNT", 'WHERE "COUNTRY" IS NULL')[0][
        "CNT"
    ]
    assert null_country_count == wave1_count, (
        f"Expected {wave1_count} rows with NULL COUNTRY, got {null_country_count}"
    )
