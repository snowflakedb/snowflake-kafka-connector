import pytest
from confluent_kafka import avro
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from time import sleep

FILE_NAME = "travis_correct_auto_table_creation_topic2table"
CONFIG_FILE = f"{FILE_NAME}.json"
TOPIC_COUNT = 2
RECORD_COUNT = 100

VALUE_SCHEMA_STRS = [
    """
    {
        "type":"record",
        "name":"value_schema_0",
        "fields":[
            {"name":"id","type":"int"},
            {"name":"approval","type":"boolean"},
            {"name":"info_map","type":{"type":"map","values":"string"}}
        ]
    }
    """,
    """
    {
        "type":"record",
        "name":"value_schema_1",
        "fields":[
            {"name":"id","type":"int"},
            {"name":"first_name","type":"string"},
            {"name":"rating","type":"float"}
        ]
    }
    """,
]

VALUE_SCHEMAS = [avro.loads(s) for s in VALUE_SCHEMA_STRS]

GOLD_SCHEMA = {
    "ID": "NUMBER",
    "FIRST_NAME": "VARCHAR",
    "RATING": "NUMBER",  # KCv4 maps Avro float to Snowflake NUMBER
    "APPROVAL": "BOOLEAN",
    "INFO_MAP": "VARIANT",
    "RECORD_METADATA": "VARIANT",
}

RECORDS = [
    {
        "id": 100,
        "approval": "true",
        "info_map": {"TREE_1": "APPLE", "TREE_2": "PINEAPPLE"},
    },
    {"id": 100, "first_name": "Zekai", "rating": 0.99},
]


@pytest.mark.confluent_only
def test_auto_table_creation_topic2table(
    driver, name_salt, create_connector, wait_for_rows
):
    """Verify auto table creation with two topics mapped to one table.

    Two Avro schemas are registered for two topics.  Both topics map to
    the same Snowflake table via topic2table.map.  The connector should
    auto-create the table with the union of all fields.
    """
    table = f"{FILE_NAME}{name_salt}"
    topics = [f"{table}{i}" for i in range(TOPIC_COUNT)]

    # Register schemas and create Kafka topics
    sr_client = SchemaRegistryClient({"url": driver.schemaRegistryAddress})
    for i, topic in enumerate(topics):
        sr_client.register_schema(
            f"{topic}-value", Schema(VALUE_SCHEMA_STRS[i], "AVRO")
        )
        driver.createTopics(topic, partitionNum=1, replicationNum=1)

    try:
        create_connector(CONFIG_FILE)
        driver.startConnectorWaitTime()

        # -- Send --
        for i, topic in enumerate(topics):
            values = [RECORDS[i] for _ in range(RECORD_COUNT)]
            driver.sendAvroSRData(topic, values, VALUE_SCHEMAS[i])
            sleep(2)

        # -- Verify total row count (both topics → one table) --
        wait_for_rows(table, RECORD_COUNT * TOPIC_COUNT)

        # -- Verify auto-created table schema (union of both schemas) --
        col_info = (
            driver.snowflake_conn.cursor().execute(f"DESC TABLE {table}").fetchall()
        )

        col_names = []
        for col in col_info:
            col_names.append(col[0])
            sf_type = col[1]
            if "(" in sf_type:
                sf_type = sf_type[: sf_type.find("(")]
            assert GOLD_SCHEMA[col[0]] == sf_type, (
                f"Column {col[0]}: expected type {GOLD_SCHEMA[col[0]]}, got {sf_type}"
            )

        for expected_col in GOLD_SCHEMA:
            assert expected_col in col_names, f"Missing column {expected_col}"
    finally:
        for topic in topics:
            driver.deleteTopic(topic)
