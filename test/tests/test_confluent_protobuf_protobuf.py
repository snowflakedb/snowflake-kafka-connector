import pytest
import test_data.sensor_pb2 as sensor_pb2
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer

FILE_NAME = "travis_correct_confluent_protobuf_protobuf"
CONFIG_FILE = f"{FILE_NAME}.json"
RECORD_COUNT = 100


def _build_sensor():
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
    driver, name_salt, create_connector, snowflake_table, wait_for_rows
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

    sensor = _build_sensor()
    for _ in range(RECORD_COUNT):
        producer.produce(topic, sensor, sensor)
        producer.poll(0)
    producer.flush()

    # -- Verify row count --
    wait_for_rows(topic, RECORD_COUNT)

    # -- Verify first row content --
    row = (
        driver.snowflake_conn.cursor()
        .execute(f"SELECT * FROM {topic} LIMIT 1")
        .fetchone()
    )

    gold_meta = (
        r'{"CreateTime":\d*,"SnowflakeConnectorPushTime":\d*,"key":{"bytes_val":"3q0=","dateTime":1234,"device":'
        r'{"deviceID":"555-4321","enabled":true},"double_array_val":'
        r'[0.3333333333333333,32.21,4.343243210000000e+08],"float_val":4321.432,'
        r'"int32_val":2147483647,"reading":321.321,"sint32_val":2147483647,"sint64_val":9223372036854775807,'
        r'"uint32_val":4294967295,"uint64_val":-1},"offset":\d*,"partition":\d*,"topic":"travis_correct_confluent_protobuf_protobuf_\w*"}'
    )
    gold_content = (
        r'{"bytes_val":"3q0=","dateTime":1234,"device":{"deviceID":"555-4321","enabled":true},"double_array_val":'
        r'[0.3333333333333333,32.21,4.343243210000000e+08],"float_val":4321.432,"int32_val":2147483647,'
        r'"reading":321.321,"sint32_val":2147483647,"sint64_val":9223372036854775807,"uint32_val":4294967295,"uint64_val":-1}'
    )
    driver.regexMatchOneLine(row, gold_meta, gold_content)
