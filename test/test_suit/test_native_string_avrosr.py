from test_suit.test_utils import RetryableError, NonRetryableError
from confluent_kafka import avro


class TestNativeStringAvrosr:
    def __init__(self, driver, nameSalt):
        self.driver = driver
        self.topic = "travis_correct_native_string_avrosr" + nameSalt

        ValueSchemaStr = """
        {
            "type":"record",
            "name":"value_schema",
            "fields":[
                {"name":"id","type":"int"},
                {"name":"firstName","type":"string"},
                {"name":"time","type":"int"}
            ]
        }
        """
        self.valueSchema = avro.loads(ValueSchemaStr)

    def send(self):
        print("\n=== Sending TestNativeStringAvrosr data ===")
        value = []
        for e in range(100):
            value.append({"id": 0, "firstName": "abc0", "time": 1835})
        self.driver.sendAvroSRData(self.topic, value, self.valueSchema)
        print("=== Done ===")

    def verify(self):
        self.driver.verifyWithRetry(self.verifyRetry)
        print("=== TestNativeStringAvrosr passed ===")

    def clean(self):
        self.driver.cleanTableStagePipe(self.topic)
        
    def verifyRetry(self):
        res = self.driver.snowflake_conn.cursor().execute(
            "SELECT count(*) FROM {}".format(self.topic)).fetchone()[0]
        if res == 0:
            raise RetryableError()
        elif res != 100:
            raise NonRetryableError()
