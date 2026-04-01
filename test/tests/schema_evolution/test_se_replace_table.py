"""Schema evolution recovery after CREATE OR REPLACE TABLE.

Migrated from v3 ``TestSchemaEvolutionDropTable``.

Sends records so the table evolves new columns, then replaces the
table with CREATE OR REPLACE TABLE.  The connector should detect the
channel invalidation, re-open the channel, and re-evolve the schema
from scratch.
"""

import json

import pytest

from lib.config_migration import V4_CONFIG_TEMPLATE

RECORD_COUNT = 100

RECORD = {
    "PERFORMANCE_STRING": "Excellent",
    "PERFORMANCE_CHAR": "A",
    "RATING_INT": 100,
}

GOLD_TYPES = {
    "PERFORMANCE_STRING": "VARCHAR",
    "PERFORMANCE_CHAR": "VARCHAR",
    "RATING_INT": "NUMBER",
    "RECORD_METADATA": "VARIANT",
}


def _assert_schema(driver, table_name):
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


def _send_records(driver, topic, count):
    keys = [json.dumps({"number": str(i)}).encode("utf-8") for i in range(count)]
    values = [json.dumps(RECORD).encode("utf-8") for _ in range(count)]
    driver.sendBytesData(topic, values, keys)


@pytest.mark.schema_evolution
@pytest.mark.compatibility
@pytest.mark.parametrize("connector_version", ["v3"], indirect=True)
def test_se_replace_table(
    driver,
    connector_version,
    name_salt,
    create_connector,
    snowflake_table,
    wait_for_rows,
):
    """CREATE OR REPLACE TABLE mid-stream invalidates v4 streaming channels.
    SSv2 SDK does not surface pipe invalidation through isClosed().
    Restricted to v3.
    """
    topic = f"se_replace_table{name_salt}"
    table_name = topic.upper()

    driver.snowflake_conn.cursor().execute(
        f"CREATE OR REPLACE TABLE {table_name} "
        f"(RECORD_METADATA VARIANT) "
        f"ENABLE_SCHEMA_EVOLUTION = TRUE"
    )
    driver.createTopics(topic, partitionNum=1, replicationNum=1)

    create_connector(
        v4_config={
            **V4_CONFIG_TEMPLATE,
            "topics": topic,
            "tasks.max": "1",
            "key.converter": "org.apache.kafka.connect.storage.StringConverter",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter.schemas.enable": "false",
            "errors.tolerance": "all",
            "errors.log.enable": "true",
            "snowflake.validation": "client_side",
        }
    )
    driver.startConnectorWaitTime()

    # Wave 1: ingest and verify schema evolution
    _send_records(driver, topic, RECORD_COUNT)
    wait_for_rows(table_name, RECORD_COUNT)
    _assert_schema(driver, table_name)

    # Replace the table (simulating an ops incident)
    driver.snowflake_conn.cursor().execute(
        f"CREATE OR REPLACE TABLE {table_name} "
        f"(RECORD_METADATA VARIANT) "
        f"ENABLE_SCHEMA_EVOLUTION = TRUE"
    )

    # Wave 2: connector should re-evolve the missing columns
    _send_records(driver, topic, RECORD_COUNT)
    wait_for_rows(table_name, RECORD_COUNT)
    _assert_schema(driver, table_name)
