"""Schema evolution with multiple topics and a mid-stream table drop.

Migrated from v3 ``TestSchemaEvolutionMultiTopicDropTable``.

Two topics with different schemas feed into one table.  After the
first wave is ingested the table is dropped and recreated.  The
connector must recover and re-evolve columns from both topics.
"""

import json

import pytest

from lib.config_migration import V4_CONFIG_TEMPLATE

RECORD_COUNT = 100

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
    },
]

GOLD_TYPES = {
    "PERFORMANCE_STRING": "VARCHAR",
    "PERFORMANCE_CHAR": "VARCHAR",
    "RATING_INT": "NUMBER",
    "RATING_DOUBLE": "FLOAT",
    "APPROVAL": "BOOLEAN",
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


def _send_all(driver, topics, count):
    for i, topic in enumerate(topics):
        keys = [json.dumps({"number": str(e)}).encode("utf-8") for e in range(count)]
        values = [json.dumps(RECORDS[i]).encode("utf-8") for _ in range(count)]
        driver.sendBytesData(topic, values, keys)


@pytest.mark.schema_evolution
@pytest.mark.compatibility
@pytest.mark.parametrize("connector_version", ["v3"], indirect=True)
def test_se_multi_topic_drop_table(
    driver,
    connector_version,
    name_salt,
    create_connector,
    snowflake_table,
    wait_for_rows,
):
    """DROP TABLE mid-stream invalidates v4 streaming pipes even with
    snowflake.validation=client_side.  Restricted to v3.
    """
    table_name = f"se_multi_topic_drop_table{name_salt}"
    topics = [f"{table_name}{i}" for i in range(2)]

    for t in topics:
        driver.createTopics(t, partitionNum=1, replicationNum=1)

    create_connector(
        v4_config={
            **V4_CONFIG_TEMPLATE,
            "topics": ",".join(topics),
            "snowflake.topic2table.map": ",".join(f"{t}:{table_name}" for t in topics),
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

    # Wave 1
    _send_all(driver, topics, RECORD_COUNT)
    wait_for_rows(table_name, RECORD_COUNT * len(topics))
    _assert_schema(driver, table_name)

    # Drop and recreate
    driver.snowflake_conn.cursor().execute(
        f"CREATE OR REPLACE TABLE {table_name} "
        f"(RECORD_METADATA VARIANT) "
        f"ENABLE_SCHEMA_EVOLUTION = TRUE"
    )

    # Wave 2
    _send_all(driver, topics, RECORD_COUNT)
    wait_for_rows(table_name, RECORD_COUNT * len(topics))
    _assert_schema(driver, table_name)
