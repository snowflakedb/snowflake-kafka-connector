"""Schema evolution with auto table creation (JSON).

Migrated from v3 ``TestSchemaEvolutionWithAutoTableCreationJson``.

The table does NOT exist initially.  The connector auto-creates it
from RECORD_METADATA, then schema evolution adds the remaining
columns from the record payload.  Two topics with different schemas
test that all columns end up in one table.
"""

import json

import pytest

from lib.config_migration import V4_CONFIG_TEMPLATE

INITIAL_BATCH = 12
FLUSH_BATCH = 300
RECORD_COUNT = INITIAL_BATCH + FLUSH_BATCH

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
def test_se_auto_table_creation_json(
    driver,
    connector_version,
    name_salt,
    create_connector,
    wait_for_rows,
):
    table_name = f"se_auto_table_creation_json{name_salt}"
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
        for batch_size in (INITIAL_BATCH, FLUSH_BATCH):
            keys = [
                json.dumps({"number": str(e)}).encode("utf-8")
                for e in range(batch_size)
            ]
            values = [json.dumps(RECORDS[i]).encode("utf-8") for _ in range(batch_size)]
            driver.sendBytesData(topic, values, keys)

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
            f"Column {col_name}: expected {expected_prefix}, got {cols[col_name]}"
        )
