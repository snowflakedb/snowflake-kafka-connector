import test_data.sensor_pb2 as sensor_pb2

FILE_NAME = "travis_correct_native_string_protobuf"
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


def test_native_string_protobuf(
    driver, name_salt, create_connector, snowflake_table, wait_for_rows
):
    topic = snowflake_table(
        FILE_NAME,
        f"CREATE OR REPLACE TABLE {FILE_NAME}{name_salt} "
        f"(record_metadata variant, record_content variant)",
    )

    create_connector(CONFIG_FILE)
    driver.startConnectorWaitTime()

    # -- Send --
    sensor = _build_sensor()
    values = [sensor.SerializeToString() for _ in range(RECORD_COUNT)]
    driver.sendBytesData(topic, values)

    # -- Verify row count --
    wait_for_rows(topic, RECORD_COUNT)

    # -- Verify first row content --
    row = (
        driver.snowflake_conn.cursor()
        .execute(f"SELECT * FROM {topic} LIMIT 1")
        .fetchone()
    )

    gold_meta = (
        r'{"CreateTime":\d*,"SnowflakeConnectorPushTime":\d*,"offset":0,"partition":0,'
        r'"topic":"travis_correct_native_string_protobuf_\w*"}'
    )
    gold_content = (
        r'{"bytes_val":"3q0=","dateTime":1234,"device":{"deviceID":"555-4321","enabled":true},'
        r'"double_array_val":[0.3333333333333333,32.21,4.343243210000000e+08],'
        r'"float_val":4321.432,"int32_val":2147483647,"reading":321.321,"sint32_val":2147483647,'
        r'"sint64_val":9223372036854775807,"uint32_val":4294967295,"uint64_val":18446744073709551615}'
    )
    driver.regexMatchOneLine(row, gold_meta, gold_content)
