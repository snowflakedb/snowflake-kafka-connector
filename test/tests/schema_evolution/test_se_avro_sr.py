"""Schema evolution with Avro Schema Registry data.

Migrated from v3 ``TestSchemaEvolutionAvroSR``.

Two topics with different Avro schemas feed into the same table.
The connector should evolve the table to accommodate all columns
from both schemas.
"""

import pytest
from confluent_kafka import avro

from lib.config_migration import V4_CONFIG_TEMPLATE

RECORD_COUNT = 100

VALUE_SCHEMAS = [
    avro.loads("""
    {
        "type": "record",
        "name": "value_schema_0",
        "fields": [
            {"name": "PERFORMANCE_CHAR", "type": "string"},
            {"name": "PERFORMANCE_STRING", "type": "string"},
            {"name": "RATING_INT", "type": "int"}
        ]
    }
    """),
    avro.loads("""
    {
        "type": "record",
        "name": "value_schema_1",
        "fields": [
            {"name": "RATING_DOUBLE", "type": "float"},
            {"name": "PERFORMANCE_STRING", "type": "string"},
            {"name": "APPROVAL", "type": "boolean"},
            {"name": "SOME_FLOAT_NAN", "type": "float"}
        ]
    }
    """),
]

RECORDS = [
    {
        "PERFORMANCE_STRING": "Excellent",
        "PERFORMANCE_CHAR": "A",
        "RATING_INT": 100,
    },
    {
        "PERFORMANCE_STRING": "Excellent",
        "RATING_DOUBLE": 0.99,
        "APPROVAL": True,
        "SOME_FLOAT_NAN": float("nan"),
    },
]

GOLD_TYPES = {
    "PERFORMANCE_STRING": "VARCHAR",
    "PERFORMANCE_CHAR": "VARCHAR",
    "RATING_INT": "NUMBER",
    "RATING_DOUBLE": "FLOAT",
    "APPROVAL": "BOOLEAN",
    "SOME_FLOAT_NAN": "FLOAT",
    "RECORD_METADATA": "VARIANT",
}


@pytest.mark.schema_evolution
@pytest.mark.confluent_only
@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
def test_se_avro_sr(
    driver,
    connector_version,
    name_salt,
    create_connector,
    snowflake_table,
    wait_for_rows,
):
    """v3 with SNOWPIPE_STREAMING cannot auto-create the table for Avro SR
    data with topic2table.map, and pre-created tables trigger pipe
    invalidation on ALTER TABLE.  Restricted to v4 (auto-creation works).
    """
    table_name = f"se_avro_sr{name_salt}"

    topics = [f"{table_name}{i}" for i in range(2)]
    for t in topics:
        driver.createTopics(t, partitionNum=1, replicationNum=1)

    connector = create_connector(
        v4_config={
            **V4_CONFIG_TEMPLATE,
            "topics": ",".join(topics),
            "snowflake.topic2table.map": ",".join(f"{t}:{table_name}" for t in topics),
            "tasks.max": "1",
            "key.converter": "org.apache.kafka.connect.storage.StringConverter",
            "value.converter": "io.confluent.connect.avro.AvroConverter",
            "value.converter.schema.registry.url": "CONFLUENT_SCHEMA_REGISTRY",
            "value.converter.schemas.enable": "false",
            "errors.tolerance": "none",
            "errors.log.enable": "true",
            "snowflake.validation": "client_side",
        }
    )
    connector_name = connector.name
    driver.startConnectorWaitTime()

    for i, topic in enumerate(topics):
        values = [RECORDS[i]] * RECORD_COUNT
        driver.sendAvroSRData(
            topic, values, VALUE_SCHEMAS[i], key=[], key_schema="", partition=0
        )

    wait_for_rows(table_name, RECORD_COUNT * len(topics), connector_name=connector_name)

    cols = {
        row[0]: row[1]
        for row in driver.snowflake_conn.cursor()
        .execute(f"DESCRIBE TABLE {table_name}")
        .fetchall()
    }
    for col_name, expected_prefix in GOLD_TYPES.items():
        assert col_name in cols, f"Missing column {col_name}, got: {list(cols.keys())}"
        assert cols[col_name].startswith(expected_prefix), (
            f"Column {col_name}: expected type starting with {expected_prefix}, "
            f"got {cols[col_name]}"
        )
