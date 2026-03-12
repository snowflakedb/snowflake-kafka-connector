"""Schema evolution: Avro SR logical types (port from v3 test_suit)."""
from decimal import Decimal

import pytest
from confluent_kafka import avro

FILE_NAME = "travis_correct_schema_evolution_avro_sr_logical_types"
CONFIG_FILE = f"{FILE_NAME}.json"
RECORD_NUM = 100
TOPIC_COUNT = 2

RECORDS = [
    {
        "PERFORMANCE_STRING": "Excellent",
        "TIME_MILLIS": 10,
        "DATE": 11,
        "TIMESTAMP_MILLIS": 12,
        "DECIMAL": Decimal("4.0"),
    },
    {"PERFORMANCE_STRING": "Excellent", "RATING_DOUBLE": 0.99},
]

VALUE_SCHEMA_STR = [
    """
    {
        "type":"record",
        "name":"value_schema_0",
        "fields":[
            {"name":"PERFORMANCE_STRING","type":"string"},
            {"name":"TIME_MILLIS","type":{"type":"int","logicalType":"time-millis"}},
            {"name":"DATE","type":{"type":"int","logicalType":"date"}},
            {"name":"DECIMAL","type":{"type":"bytes","logicalType":"decimal", "precision":4, "scale":2}},
            {"name":"TIMESTAMP_MILLIS","type":{"type":"long","logicalType":"timestamp-millis"}}
        ]
    }
    """,
    """
    {
        "type":"record",
        "name":"value_schema_1",
        "fields":[
            {"name":"RATING_DOUBLE","type":"float"},
            {"name":"PERFORMANCE_STRING","type":"string"}
        ]
    }
    """,
]

GOLD_TYPE = {
    "PERFORMANCE_STRING": "VARCHAR",
    "RATING_DOUBLE": "FLOAT",
    "TIME_MILLIS": "TIME(6)",
    "DATE": "DATE",
    "TIMESTAMP_MILLIS": "TIMESTAMP_NTZ(6)",
    "DECIMAL": "VARCHAR",
    "RECORD_METADATA": "VARIANT",
}


@pytest.mark.confluent_only
def test_schema_evolution_avro_sr_logical_types(
    driver, connector_version, name_salt, create_connector, snowflake_table, wait_for_rows
):
    if connector_version == "v3":
        pytest.skip("v3 plugin conflicts with Schema Registry classloading")
    table = snowflake_table(
        FILE_NAME,
        f"CREATE OR REPLACE TABLE {FILE_NAME}{name_salt} (PERFORMANCE_STRING STRING). "
        f"ALTER TABLE {FILE_NAME}{name_salt} SET ENABLE_SCHEMA_EVOLUTION = true",
    )

    for i in range(TOPIC_COUNT):
        driver.createTopics(f"{table}{i}", partitionNum=1, replicationNum=1)

    create_connector(CONFIG_FILE)
    driver.startConnectorWaitTime()

    value_schemas = [avro.loads(str(s)) for s in VALUE_SCHEMA_STR]
    for i in range(TOPIC_COUNT):
        values = [RECORDS[i]] * RECORD_NUM
        driver.sendAvroSRData(
            f"{table}{i}", values, value_schemas[i], key=[], key_schema="", partition=0
        )

    wait_for_rows(table, TOPIC_COUNT * RECORD_NUM)

    rows = driver.snowflake_conn.cursor().execute(f"DESC TABLE {table}").fetchall()
    gold_columns = list(GOLD_TYPE.keys())
    for row in rows:
        gold_columns.remove(row[0])
        assert row[1].startswith(
            GOLD_TYPE[row[0]]
        ), f"Column {row[0]} has wrong type: got {row[1]}, expected {GOLD_TYPE[row[0]]}"
    assert not gold_columns, f"Columns not in table: {gold_columns}"

    count = (
        driver.snowflake_conn.cursor()
        .execute(f"SELECT count(*) FROM {table}")
        .fetchone()[0]
    )
    assert count == TOPIC_COUNT * RECORD_NUM

    for i in range(TOPIC_COUNT):
        driver.deleteTopic(f"{table}{i}")

