from test_suit.test_utils import RetryableError, NonRetryableError
import test_data.sensor_pb2 as sensor_pb2

class TestNativeStringProtobuf:
    def __init__(self, driver, nameSalt):
        self.driver = driver
        self.fileName = "travis_correct_native_string_protobuf"
        self.topic = self.fileName + nameSalt

        self.sensor = sensor_pb2.SensorReading()
        self.sensor.dateTime = 1234
        self.sensor.reading = 321.321
        self.sensor.device.deviceID = "555-4321"
        self.sensor.device.enabled = True

        self.sensor.float_val = 4321.4321
        self.sensor.int32_val = (1 << 31) - 1
        self.sensor.sint32_val = (1 << 31) - 1
        self.sensor.sint64_val = (1 << 63) - 1
        self.sensor.uint32_val = (1 << 32) - 1

        self.sensor.bytes_val = b'\xDE\xAD'
        self.sensor.double_array_val.extend([1/3, 32.21, 434324321])
        self.sensor.uint64_val = (1 << 64) - 1

    def getConfigFileName(self):
        return self.fileName + ".json"

    def send(self):
        value = []
        for e in range(100):
            value.append(self.sensor.SerializeToString())
        self.driver.sendBytesData(self.topic, value)

    def verify(self, round):
        res = self.driver.snowflake_conn.cursor().execute(
            "SELECT count(*) FROM {}".format(self.topic)).fetchone()[0]
        if res == 0:
            raise RetryableError()
        elif res != 100:
            raise NonRetryableError("Number of record in table is different from number of record sent")

        # validate content of line 1
        res = self.driver.snowflake_conn.cursor().execute(
            "Select * from {} limit 1".format(self.topic)).fetchone()

        # "schema_id" is lost since they are using native avro converter
        goldMeta = r'{"CreateTime":\d*,"offset":0,"partition":0,' \
                   r'"topic":"travis_correct_native_string_protobuf....."}'
        goldContent = r'{"bytes_val":"3q0=","dateTime":1234,"device":{"deviceID":"555-4321","enabled":true},' \
                      r'"double_array_val":[0.3333333333333333,32.21,4.343243210000000e+08],' \
                      r'"float_val":4321.432,"int32_val":2147483647,"reading":321.321,"sint32_val":2147483647,' \
                      r'"sint64_val":9223372036854775807,"uint32_val":4294967295,"uint64_val":18446744073709551615}'
        self.driver.regexMatchOneLine(res, goldMeta, goldContent)

        self.driver.verifyStageIsCleaned(self.topic)

    def clean(self):
        self.driver.cleanTableStagePipe(self.topic)