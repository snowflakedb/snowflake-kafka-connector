"""Schema evolution: auto table creation + Avro SR (port from v3 test_suit)."""
import pytest
from confluent_kafka import avro

FILE_NAME = "travis_correct_schema_evolution_w_auto_table_creation_avro_sr"
CONFIG_FILE = f"{FILE_NAME}.json"
TOPIC_COUNT = 2
INITIAL_RECORD_COUNT = 12
FLUSH_RECORD_COUNT = 300
RECORD_NUM = INITIAL_RECORD_COUNT + FLUSH_RECORD_COUNT

RECORDS = [
    {"PERFORMANCE_STRING": "Excellent", "PERFORMANCE_CHAR": "A", "RATING_INT": 100},
    {"PERFORMANCE_STRING": "Excellent", "RATING_DOUBLE": 0.99, "APPROVAL": True},
]

VALUE_SCHEMA_STR = [
    """
    {
        "type":"record",
        "name":"value_schema_0",
        "fields":[
            {"name":"PERFORMANCE_STRING","type":"string"},
            {"name":"PERFORMANCE_CHAR","type":"string"},
            {"name":"RATING_INT","type":"int"}
        ]
    }
    """,
    """
    {
        "type":"record",
        "name":"value_schema_1",
        "fields":[
            {"name":"PERFORMANCE_STRING","type":"string"},
            {"name":"RATING_DOUBLE","type":"float"},
            {"name":"APPROVAL","type":"boolean"}
        ]
    }
    """,
]

GOLD_TYPE = {
    "PERFORMANCE_STRING": "VARCHAR",
    "PERFORMANCE_CHAR": "VARCHAR",
    "RATING_INT": "NUMBER",
    "RATING_DOUBLE": "FLOAT",
    "APPROVAL": "BOOLEAN",
    "RECORD_METADATA": "VARIANT",
}


@pytest.mark.confluent_only
def test_schema_evolution_w_auto_table_creation_avro_sr(
    driver, connector_version, name_salt, create_connector, wait_for_rows
):
    if connector_version == "v3":
        pytest.skip("v3 plugin conflicts with Schema Registry classloading")
    table = FILE_NAME + name_salt
    for i in range(TOPIC_COUNT):
        driver.createTopics(f"{table}{i}", partitionNum=1, replicationNum=1)

    create_connector(CONFIG_FILE)
    driver.startConnectorWaitTime()

    value_schemas = [avro.loads(s) for s in VALUE_SCHEMA_STR]
    for i in range(TOPIC_COUNT):
        values = [RECORDS[i]] * INITIAL_RECORD_COUNT
        driver.sendAvroSRData(
            f"{table}{i}", values, value_schemas[i], key=[], key_schema="", partition=0
        )
        values = [RECORDS[i]] * FLUSH_RECORD_COUNT
        driver.sendAvroSRData(
            f"{table}{i}", values, value_schemas[i], key=[], key_schema="", partition=0
        )

    wait_for_rows(table, TOPIC_COUNT * RECORD_NUM)

    rows = driver.snowflake_conn.cursor().execute(f"DESC TABLE {table}").fetchall()
    gold_columns = list(GOLD_TYPE.keys())
    for row in rows:
        gold_columns.remove(row[0])
        assert row[1].startswith(GOLD_TYPE[row[0]])
    assert not gold_columns

    count = (
        driver.snowflake_conn.cursor()
        .execute(f"SELECT count(*) FROM {table}")
        .fetchone()[0]
    )
    assert count == TOPIC_COUNT * RECORD_NUM

    for i in range(TOPIC_COUNT):
        driver.deleteTopic(f"{table}{i}")

