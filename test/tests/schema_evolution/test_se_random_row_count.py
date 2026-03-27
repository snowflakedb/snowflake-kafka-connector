"""Schema evolution with random initial batch size.

Migrated from v3 ``TestSchemaEvolutionWithRandomRowCount``.

The initial batch size is randomised (1–299) so that the ALTER TABLE
for schema evolution can trigger at different points relative to the
flush boundary (300 records).  This catches timing-related edge cases
in the schema evolution path.
"""

import json
import random

import pytest

from lib.config_migration import V4_CONFIG_TEMPLATE

FLUSH_BATCH = 300

RECORDS = [
    {"PERFORMANCE_STRING": "Excellent", "PERFORMANCE_CHAR": "A", "RATING_INT": 100},
    {"PERFORMANCE_STRING": "Excellent", "RATING_DOUBLE": 0.99, "APPROVAL": True},
]

GOLD_TYPES = {
    "PERFORMANCE_STRING": "VARCHAR",
    "PERFORMANCE_CHAR": "VARCHAR",
    "RATING_INT": "NUMBER",
    "RATING_DOUBLE": "FLOAT",
    "APPROVAL": "BOOLEAN",
    "RECORD_METADATA": "VARIANT",
}


@pytest.mark.schema_evolution
@pytest.mark.compatibility
def test_se_random_row_count(
    driver,
    connector_version,
    name_salt,
    create_connector,
    snowflake_table,
    wait_for_rows,
):
    initial_batch = random.randrange(1, 300)
    record_count = initial_batch + FLUSH_BATCH

    table_name = f"se_random_row_count{name_salt}"
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
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter.schemas.enable": "false",
            "errors.tolerance": "none",
            "errors.log.enable": "true",
            "snowflake.validation": "client_side",
        }
    )
    connector_name = connector.name
    driver.startConnectorWaitTime()

    for i, topic in enumerate(topics):
        for batch_size in (initial_batch, FLUSH_BATCH):
            keys = [
                json.dumps({"number": str(e)}).encode("utf-8")
                for e in range(batch_size)
            ]
            values = [json.dumps(RECORDS[i]).encode("utf-8") for _ in range(batch_size)]
            driver.sendBytesData(topic, values, keys)

    wait_for_rows(table_name, record_count * len(topics), connector_name=connector_name)

    cols = {
        row[0]: row[1]
        for row in driver.snowflake_conn.cursor()
        .execute(f"DESCRIBE TABLE {table_name}")
        .fetchall()
    }
    for col_name, expected_prefix in GOLD_TYPES.items():
        assert col_name in cols, f"Missing column {col_name}, got: {list(cols.keys())}"
        assert cols[col_name].startswith(expected_prefix), (
            f"Column {col_name}: expected {expected_prefix}, got {cols[col_name]}"
        )
