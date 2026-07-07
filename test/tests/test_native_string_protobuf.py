import json

from lib.matchers import ANY_INT

FILE_NAME = "travis_correct_native_string_protobuf"
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


def test_native_string_protobuf(
    sensor_pb2,
    driver,
    name_salt,
    create_connector_from_file,
    create_table,
    wait_for_rows,
):
    table = create_table(
        FILE_NAME.upper(),
        columns="(record_metadata variant, record_content variant)",
    )
    topic = f"{FILE_NAME}{name_salt}"

    create_connector_from_file(CONFIG_FILE)
    driver.startConnectorWaitTime()

    # -- Send --
    sensor = _build_sensor(sensor_pb2)
    values = [sensor.SerializeToString() for _ in range(RECORD_COUNT)]
    driver.sendBytesData(topic, values)

    # -- Verify row count --
    wait_for_rows(table.name, RECORD_COUNT)

    # -- Verify first row content --
    # Snowflake does not guarantee row ordering without ORDER BY, so we must
    # select the specific record at offset 0 rather than relying on insertion order.
    row = table.select(
        "record_metadata, record_content", "WHERE record_metadata:offset::int = 0"
    )[0]

    record_metadata = json.loads(row["RECORD_METADATA"])
    assert record_metadata == {
        "CreateTime": ANY_INT,
        "SnowflakeConnectorPushTime": ANY_INT,
        "offset": 0,
        "partition": 0,
        "topic": topic,
    }

    record_content = json.loads(row["RECORD_CONTENT"])
    assert record_content == {
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
        "uint64_val": 18446744073709551615,
    }
