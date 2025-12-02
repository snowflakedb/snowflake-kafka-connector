from snowflake.connector import DictCursor
from test_suit.test_utils import RetryableError, NonRetryableError
from confluent_kafka import avro
from test_suit.base_e2e import BaseE2eTest


class TestStringAvrosr(BaseE2eTest):
    def __init__(self, driver, nameSalt):
        self.driver = driver
        self.fileName = "travis_correct_string_avrosr"
        self.topic = self.fileName + nameSalt
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
        self.tableName = self.fileName + nameSalt
        self.driver.snowflake_conn.cursor().execute(f"""create or replace table {self.tableName} (record_metadata variant, id number, firstName varchar, time number)""")

    def getConfigFileName(self):
        return self.fileName + ".json"

    def send(self):
        value = []
        for e in range(100):
            value.append({"id": e, "firstName": "abc0", "time": 1835})
        self.driver.sendAvroSRData(self.topic, value, self.valueSchema)

    def verify(self, round):
        res = self.driver.select_number_of_records(self.topic)
        if res == 0:
            raise RetryableError()
        elif res != 100:
            raise NonRetryableError("Number of record in table is different from number of record sent")

        # validate content of line 1
        res = self.driver.snowflake_conn.cursor(DictCursor).execute(
            "Select * from {} limit 1".format(self.topic)).fetchone()
        goldMeta = r'{"CreateTime":\d*,"SnowflakeConnectorPushTime":\d*,"offset":0,"partition":0,"topic":"travis_correct_string_avrosr_\w*"}'
        assert res['ID'] == 0
        assert res['FIRSTNAME'] == "abc0"
        assert res['TIME'] == 1835
        self.driver.regexMatchMeta(res['RECORD_METADATA'], goldMeta)



    def clean(self):
        self.driver.cleanTableStagePipe(self.topic)