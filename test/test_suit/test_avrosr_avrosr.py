from test_suit.test_utils import RetryableError, NonRetryableError
from confluent_kafka import avro
from test_suit.base_e2e import BaseE2eTest


class TestAvrosrAvrosr(BaseE2eTest):
    def __init__(self, driver, nameSalt):
        self.driver = driver
        self.fileName = "travis_correct_avrosr_avrosr"
        self.topic = self.fileName + nameSalt

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
                {"name":"time","type":"int"},
                {"name":"someFloat","type":"float"},
                {"name":"someFloatNaN","type":"float"},
                {"name":"someFloatPositiveInfinity","type":"float"},
                {"name":"someFloatNegativeInfinity","type":"float"},
                {"name":"someDouble","type":"double"},
                {"name":"someDoubleNaN","type":"double"},
                {"name":"someDoublePositiveInfinity","type":"double"},
                {"name":"someDoubleNegativeInfinity","type":"double"}
            ]
        }
        """
        self.keySchema = avro.loads(KeySchemaStr)
        self.valueSchema = avro.loads(ValueSchemaStr)

    def getConfigFileName(self):
        return self.fileName + ".json"

    def send(self):
        value = []
        key = []
        for e in range(100):
            # avro data must follow the schema defined in ValueSchemaStr
            key.append({"id": e})
            value.append({
                "id": e,
                "firstName": "abc0",
                "time": 1835,
                "someFloat": 21.37,
                "someFloatNaN": "NaN",
                "someFloatPositiveInfinity": "inf",
                "someFloatNegativeInfinity": "-inf",
                "someDouble": 15.10,
                "someDoubleNaN": "NaN",
                "someDoublePositiveInfinity": "inf",
                "someDoubleNegativeInfinity": "-inf"
            })
        self.driver.sendAvroSRData(
            self.topic, value, self.valueSchema, key, self.keySchema)

    def verify(self, round):
        res = self.driver.select_number_of_records(self.topic)
        if res == 0:
            raise RetryableError()
        elif res != 100:
            raise NonRetryableError("Number of record in table is different from number of record sent")
        # validate content of line 1
        res = self.driver.snowflake_conn.cursor().execute(
            "Select * from {} limit 1".format(self.topic)).fetchone()
        goldMeta = r'{"CreateTime":\d*,"key":{"id":0},"key_schema_id":\d*,"offset":0,"partition":0,"schema_id":\d*,' \
                   r'"topic":"travis_correct_avrosr_avrosr_\w*"}'
        goldContent = r'{"firstName":"abc0","id":0,"someDouble":15.1,"someDoubleNaN":"NaN","someDoubleNegativeInfinity":"-Infinity","someDoublePositiveInfinity":"Infinity","someFloat":21.37,"someFloatNaN":"NaN","someFloatNegativeInfinity":"-Infinity","someFloatPositiveInfinity":"Infinity","time":1835}'
        self.driver.regexMatchOneLine(res, goldMeta, goldContent)

        self.driver.verifyStageIsCleaned(self.topic)

    def clean(self):
        self.driver.cleanTableStagePipe(self.topic)