"""Iceberg schema evolution E2E tests — Avro format (via Schema Registry).

Tests server-side SE with Avro-encoded records: the connector forwards rows
with new columns and the SSv2 server adds them via ALTER ICEBERG TABLE ADD COLUMN.
Managed-Iceberg SE is server-side only (the connector rejects client-side
validation for Iceberg + SE), so this uses ``validation=server_side``.

v4-only, confluent-only.
"""

import logging

import pytest
from confluent_kafka import avro

from lib.config_migration import V4_CONFIG_TEMPLATE
from lib.driver import KafkaDriver
from lib.fixtures.table import ICEBERG_EXTERNAL_VOLUME, IcebergTable

logger = logging.getLogger(__name__)

WAVE1_SCHEMA = avro.loads(
    """
{
    "type": "record",
    "name": "iceberg_se_avro_record",
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
    "name": "iceberg_se_avro_record",
    "fields": [
        {"name": "CITY", "type": "string"},
        {"name": "AGE", "type": "int"},
        {"name": "COUNTRY", "type": ["null", "string"], "default": null}
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
        "snowflake.validation": "server_side",
        "snowflake.autocreate.table.type": "iceberg",
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
    """Iceberg SE with Avro — server adds columns as evolving Avro schemas arrive.

    Table starts with RECORD_METADATA VARIANT + CITY TEXT. Wave 1 sends Avro
    records with ``{CITY, AGE}`` — server-side SE adds AGE. Wave 2 uses an
    evolved Avro schema with ``{CITY, AGE, COUNTRY}`` — server-side SE adds COUNTRY.

    AGE/COUNTRY are primitive columns, which server-side SE supports. Uses
    ``validation=server_side`` (the supported path for managed-Iceberg + SE).
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


# Two distinct Avro schemas feeding one table — mirrors FDN test_se_avro_sr,
# including a NaN float to exercise FLOAT handling on Iceberg.
AVRO_SR_SCHEMAS = [
    avro.loads(
        """
{
    "type": "record",
    "name": "iceberg_se_avro_sr_0",
    "fields": [
        {"name": "PERFORMANCE_CHAR", "type": "string"},
        {"name": "PERFORMANCE_STRING", "type": "string"},
        {"name": "RATING_INT", "type": "int"}
    ]
}
"""
    ),
    avro.loads(
        """
{
    "type": "record",
    "name": "iceberg_se_avro_sr_1",
    "fields": [
        {"name": "RATING_DOUBLE", "type": "float"},
        {"name": "PERFORMANCE_STRING", "type": "string"},
        {"name": "APPROVAL", "type": "boolean"},
        {"name": "SOME_FLOAT_NAN", "type": "float"}
    ]
}
"""
    ),
]

AVRO_SR_RECORDS = [
    {"PERFORMANCE_STRING": "Excellent", "PERFORMANCE_CHAR": "A", "RATING_INT": 100},
    {
        "PERFORMANCE_STRING": "Excellent",
        "RATING_DOUBLE": 0.99,
        "APPROVAL": True,
        "SOME_FLOAT_NAN": float("nan"),
    },
]

AVRO_SR_GOLD_TYPES = {
    "PERFORMANCE_STRING": ("VARCHAR",),
    "PERFORMANCE_CHAR": ("VARCHAR",),
    "RATING_INT": ("NUMBER",),
    # Iceberg server-side SE infers NUMBER(38,2) for a finite float value (like JSON),
    # unlike FDN which infers FLOAT.
    "RATING_DOUBLE": ("NUMBER",),
    "APPROVAL": ("BOOLEAN",),
    # NaN is not valid JSON; the SSv2 NDJSON path serializes it as the string "NaN",
    # so server-side SE infers VARCHAR for this column (finite floats become NUMBER).
    "SOME_FLOAT_NAN": ("VARCHAR", "FLOAT", "DOUBLE", "REAL", "NUMBER"),
}


@pytest.mark.iceberg
@pytest.mark.schema_evolution
@pytest.mark.confluent_only
@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
def test_iceberg_se_avro_sr_multi_schema(
    driver: KafkaDriver,
    name_salt: str,
    create_connector,
    wait_for_rows,
):
    """Two Avro-SR schemas auto-create + evolve one iceberg table (server-side).

    Server-side mirror of FDN ``test_se_avro_sr``. The table does not exist; the
    connector auto-creates the metadata-only iceberg table (no ICEBERG_VERSION ->
    default format v2) and server-side SE adds every column from both Avro schemas,
    including a NaN-valued FLOAT.
    """
    record_count = 100
    base = f"iceberg_se_avro_sr{name_salt}"
    table_name = base.upper()
    topics = [f"{base}{i}" for i in range(2)]
    for t in topics:
        driver.createTopics(t, partitionNum=1, replicationNum=1)

    connector = create_connector(
        v4_config={
            **V4_CONFIG_TEMPLATE,
            "tasks.max": "1",
            "topics": ",".join(topics),
            "snowflake.topic2table.map": ",".join(f"{t}:{table_name}" for t in topics),
            "key.converter": "org.apache.kafka.connect.storage.StringConverter",
            "value.converter": "io.confluent.connect.avro.AvroConverter",
            "value.converter.schema.registry.url": "CONFLUENT_SCHEMA_REGISTRY",
            "snowflake.enable.schematization": "true",
            "snowflake.validation": "server_side",
            "snowflake.autocreate.table.type": "iceberg",
            "snowflake.iceberg.create.table.options": f"EXTERNAL_VOLUME='{ICEBERG_EXTERNAL_VOLUME}'",
            "snowflake.compatibility.enable.column.identifier.normalization": "true",
            "jmx": "true",
        }
    )
    table = IcebergTable(driver, table_name)
    try:
        driver.startConnectorWaitTime()

        for i, topic in enumerate(topics):
            values = [AVRO_SR_RECORDS[i]] * record_count
            driver.sendAvroSRData(
                topic, values, AVRO_SR_SCHEMAS[i], key=[], key_schema="", partition=0
            )

        wait_for_rows(
            table_name, record_count * len(topics), connector_name=connector.name
        )

        cols = {
            row[0]: row[1]
            for row in driver.snowflake_conn.cursor()
            .execute(f"DESCRIBE TABLE {table_name}")
            .fetchall()
        }
        for col_name, prefix in AVRO_SR_GOLD_TYPES.items():
            assert col_name in cols, (
                f"Missing column {col_name}, got: {list(cols.keys())}"
            )
            assert cols[col_name].startswith(prefix), (
                f"Column {col_name}: expected {prefix}, got {cols[col_name]}"
            )
    finally:
        table.drop()
