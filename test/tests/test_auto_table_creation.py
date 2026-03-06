import pytest
from confluent_kafka import avro
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from time import sleep

FILE_NAME = "travis_correct_auto_table_creation"
CONFIG_FILE = f"{FILE_NAME}.json"
RECORD_COUNT = 100

VALUE_SCHEMA_STR = """
{
    "type":"record",
    "name":"value_schema",
    "fields":[
        {"name":"id","type":"int"},
        {"name":"first_name","type":"string"},
        {"name":"rating","type":"float"},
        {"name":"approval","type":"boolean"},
        {"name":"info_map","type":{"type":"map","values":"string"}}
    ]
}
"""

VALUE_SCHEMA = avro.loads(VALUE_SCHEMA_STR)

GOLD_SCHEMA = {
    "ID": "NUMBER",
    "FIRST_NAME": "VARCHAR",
    "RATING": "NUMBER",  # KCv4 maps Avro float to Snowflake NUMBER
    "APPROVAL": "BOOLEAN",
    "INFO_MAP": "VARIANT",
    "RECORD_METADATA": "VARIANT",
}

RECORD = {
    "id": 100,
    "first_name": "Zekai",
    "rating": 0.99,
    "approval": "true",
    "info_map": {"TREE_1": "APPLE", "TREE_2": "PINEAPPLE"},
}


@pytest.mark.confluent_only
def test_auto_table_creation(driver, name_salt, create_connector, wait_for_rows):
    """Verify auto table creation with Avro Schema Registry.

    The table is NOT pre-created — the connector should auto-create it
    based on the registered Avro schema.  Verifies column types match
    the expected schema.
    """
    topic = f"{FILE_NAME}{name_salt}"

    # Register schema with Schema Registry
    sr_client = SchemaRegistryClient({"url": driver.schemaRegistryAddress})
    sr_client.register_schema(f"{topic}-value", Schema(VALUE_SCHEMA_STR, "AVRO"))

    # Create Kafka topic (but NOT the Snowflake table)
    driver.createTopics(topic, partitionNum=1, replicationNum=1)

    try:
        create_connector(CONFIG_FILE)
        driver.startConnectorWaitTime()

        # -- Send --
        values = [RECORD for _ in range(RECORD_COUNT)]
        driver.sendAvroSRData(topic, values, VALUE_SCHEMA)
        sleep(2)

        # -- Verify row count --
        wait_for_rows(topic, RECORD_COUNT)

        # -- Verify auto-created table schema --
        col_info = (
            driver.snowflake_conn.cursor().execute(f"DESC TABLE {topic}").fetchall()
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
        driver.deleteTopic(topic)
