import json

import pytest
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer

from lib.matchers import ANY_INT

FILE_NAME = "travis_correct_confluent_protobuf_protobuf"
CONFIG_FILE = f"{FILE_NAME}.json"
RECORD_COUNT = 100


def _build_sensor(sensor_pb2):
    sensor = sensor_pb2.SensorReading()
    sensor.dateTime = 1234
    sensor.reading = 321.321
    sensor.device.deviceID = "555-4321"
    sensor.device.enabled = True
    sensor.float_val = 4321.4321
    sensor.int32_val = (1 << 31) - 1
    sensor.sint32_val = (1 << 31) - 1
    sensor.sint64_val = (1 << 63) - 1
    sensor.uint32_val = (1 << 32) - 1
    sensor.bytes_val = b"\xde\xad"
    sensor.double_array_val.extend([1 / 3, 32.21, 434324321])
    sensor.uint64_val = (1 << 64) - 1
    return sensor


@pytest.mark.confluent_only
def test_confluent_protobuf_protobuf(
    sensor_pb2, driver, name_salt, create_connector, snowflake_table, wait_for_rows
):
    topic = snowflake_table(
        FILE_NAME,
        f"CREATE OR REPLACE TABLE {FILE_NAME}{name_salt} "
        f"(record_metadata variant, record_content variant)",
    )

    create_connector(CONFIG_FILE)
    driver.startConnectorWaitTime()

    # -- Send via schema-registry-backed protobuf producer --
    sr_client = SchemaRegistryClient({"url": driver.schemaRegistryAddress})
    key_ser = ProtobufSerializer(sensor_pb2.SensorReading, sr_client)
    val_ser = ProtobufSerializer(sensor_pb2.SensorReading, sr_client)
    producer = SerializingProducer(
        {
            "bootstrap.servers": driver.kafkaAddress,
            "key.serializer": key_ser,
            "value.serializer": val_ser,
        }
    )

    sensor = _build_sensor(sensor_pb2)
    for _ in range(RECORD_COUNT):
        producer.produce(topic, sensor, sensor)
        producer.poll(0)
    producer.flush()

    # -- Verify row count --
    wait_for_rows(topic, RECORD_COUNT)

    # -- Verify first row content --
    row = (
        driver.snowflake_conn.cursor()
        .execute(f"SELECT record_metadata, record_content FROM {topic} LIMIT 1")
        .fetchone()
    )

    sensor_dict = {
        "bytes_val": "3q0=",
        "dateTime": 1234,
        "device": {"deviceID": "555-4321", "enabled": True},
        "double_array_val": [0.3333333333333333, 32.21, 4.343243210000000e08],
        "float_val": 4321.432,
        "int32_val": 2147483647,
        "reading": 321.321,
        "sint32_val": 2147483647,
        "sint64_val": 9223372036854775807,
        "uint32_val": 4294967295,
        "uint64_val": -1,
    }

    record_metadata = json.loads(row[0])
    assert record_metadata == {
        "CreateTime": ANY_INT,
        "SnowflakeConnectorPushTime": ANY_INT,
        "key": sensor_dict,
        "offset": ANY_INT,
        "partition": ANY_INT,
        "topic": topic,
    }

    record_content = json.loads(row[1])
    assert record_content == sensor_dict
