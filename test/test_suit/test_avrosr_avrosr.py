from test_suit.test_utils import RetryableError, NonRetryableError
from confluent_kafka import avro


class TestAvrosrAvrosr:
    def __init__(self, driver, nameSalt):
        self.driver = driver
        self.topic = "travis_correct_avrosr_avrosr" + nameSalt

        KeySchemaStr = """
        {
            "type":"record",
            "name":"key_schema",
            "fields":[
                {"name":"id","type":"int"}
            ]
        } 
        """

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
        self.keySchema = avro.loads(KeySchemaStr)
        self.valueSchema = avro.loads(ValueSchemaStr)

    def send(self):
        print("\n=== Sending TestAvrosrAvrosr data ===")
        value = []
        key = []
        for e in range(100):
            # avro data must follow the schema defined in ValueSchemaStr
            key.append({"id": 0})
            value.append({"id": 0, "firstName": "abc0", "time": 1835})
        self.driver.sendAvroSRData(
            self.topic, value, self.valueSchema, key, self.keySchema)
        print("=== Done ===")

    def verify(self):
        self.driver.verifyWithRetry(self.verifyRetry)
        print("=== TestAvrosrAvrosr passed ===")

    def clean(self):
        self.driver.cleanTableStagePipe(self.topic)
        
    def verifyRetry(self):
        res = self.driver.snowflake_conn.cursor().execute(
            "SELECT count(*) FROM {}".format(self.topic)).fetchone()[0]
        if res == 0:
            raise RetryableError()
        elif res != 100:
            raise NonRetryableError()
