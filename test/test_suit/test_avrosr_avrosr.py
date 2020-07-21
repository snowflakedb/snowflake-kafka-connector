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
        value = []
        key = []
        for e in range(100):
            # avro data must follow the schema defined in ValueSchemaStr
            key.append({"id": e})
            value.append({"id": e, "firstName": "abc0", "time": 1835})
        self.driver.sendAvroSRData(
            self.topic, value, self.valueSchema, key, self.keySchema)

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
        goldMeta = r'{"CreateTime":\d*,"key":{"id":0},"key_schema_id":\d*,"offset":0,"partition":0,"schema_id":\d*,' \
                   r'"topic":"travis_correct_avrosr_avrosr....."}'
        goldContent = r'{"firstName":"abc0","id":0,"time":1835}'
        self.driver.regexMatchOneLine(res, goldMeta, goldContent)

    def clean(self):
        self.driver.cleanTableStagePipe(self.topic)