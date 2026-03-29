"""Schema evolution with NOT NULL columns.

Migrated from v3 ``TestSchemaEvolutionNonNullableJson``.

The table starts with a NOT NULL column.  Records arrive without that
column but with new columns.  Schema evolution must:
  - Add the new columns as nullable
  - All evolved columns should be nullable (verified via DESCRIBE)
"""

import json

import pytest

from lib.config_migration import V4_CONFIG_TEMPLATE

RECORD_COUNT = 100

RECORD = {
    "PERFORMANCE_CHAR": "A",
    "RATING_INT": 100,
}

GOLD_TYPES = {
    "PERFORMANCE_STRING": "VARCHAR",
    "PERFORMANCE_CHAR": "VARCHAR",
    "RATING_INT": "NUMBER",
    "RECORD_METADATA": "VARIANT",
}


@pytest.mark.schema_evolution
@pytest.mark.compatibility
def test_se_nonnullable_json(
    driver,
    connector_version,
    name_salt,
    create_connector,
    snowflake_table,
    wait_for_rows,
):
    topic = f"se_nonnullable_json{name_salt}"
    table_name = topic.upper()

    driver.snowflake_conn.cursor().execute(
        f"CREATE OR REPLACE TABLE {table_name} "
        f"(RECORD_METADATA VARIANT, PERFORMANCE_STRING STRING NOT NULL) "
        f"ENABLE_SCHEMA_EVOLUTION = TRUE"
    )
    driver.createTopics(topic, partitionNum=1, replicationNum=1)

    connector = create_connector(
        v4_config={
            **V4_CONFIG_TEMPLATE,
            "topics": topic,
            "snowflake.topic2table.map": f"{topic}:{table_name}",
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

    keys = [json.dumps({"number": str(i)}).encode("utf-8") for i in range(RECORD_COUNT)]
    values = [json.dumps(RECORD).encode("utf-8") for _ in range(RECORD_COUNT)]
    driver.sendBytesData(topic, values, keys)

    wait_for_rows(table_name, RECORD_COUNT, connector_name=connector_name)

    rows = (
        driver.snowflake_conn.cursor()
        .execute(f"DESCRIBE TABLE {table_name}")
        .fetchall()
    )

    cols = {}
    for row in rows:
        col_name, col_type, _kind, nullable = row[0], row[1], row[2], row[3]
        cols[col_name] = col_type
        assert nullable == "Y", (
            f"Column {col_name} should be nullable after schema evolution, "
            f"but null?={nullable}"
        )

    for col_name, expected_prefix in GOLD_TYPES.items():
        assert col_name in cols, f"Missing column {col_name}, got: {list(cols.keys())}"
        assert cols[col_name].startswith(expected_prefix), (
            f"Column {col_name}: expected {expected_prefix}, got {cols[col_name]}"
        )
