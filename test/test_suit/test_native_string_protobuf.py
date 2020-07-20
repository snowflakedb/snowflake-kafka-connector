from test_suit.test_utils import RetryableError, NonRetryableError
import test_data.sensor_pb2 as sensor_pb2

class TestNativeStringProtobuf:
    def __init__(self, driver, nameSalt):
        self.driver = driver
        self.topic = "travis_correct_native_string_protobuf" + nameSalt

        self.sensor = sensor_pb2.SensorReading()
        self.sensor.dateTime = 1234
        self.sensor.reading = 321.321
        self.sensor.device.deviceID = "555-4321"
        self.sensor.device.enabled = True

    def send(self):
        value = []
        for e in range(100):
            value.append(self.sensor.SerializeToString())
        self.driver.sendBytesData(self.topic, value)

    def verify(self):
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
        goldContent = r'{"dateTime":1234,"device":{"deviceID":"555-4321","enabled":true},"reading":321.321}'
        self.driver.regexMatchOneLine(res, goldMeta, goldContent)

    def clean(self):
        self.driver.cleanTableStagePipe(self.topic)