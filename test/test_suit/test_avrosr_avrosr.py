from snowflake.connector import DictCursor
from test_suit.test_utils import RetryableError, NonRetryableError
from confluent_kafka import avro
from test_suit.base_e2e import BaseE2eTest


class TestAvrosrAvrosr(BaseE2eTest):
    def __init__(self, driver, nameSalt):
        self.driver = driver
        self.fileName = "travis_correct_avrosr_avrosr"
        self.topic = self.fileName + nameSalt
        self.tableName = self.fileName + nameSalt

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
        self.driver.snowflake_conn.cursor().execute(f"""create or replace table {self.tableName} (record_metadata variant, id number,
        firstName varchar, time number, someFloat number, someFloatNaN varchar, someFloatPositiveInfinity varchar, someFloatNegativeInfinity varchar,
          someDouble number, someDoubleNaN varchar, someDoublePositiveInfinity varchar, someDoubleNegativeInfinity varchar )""")


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
        res = self.driver.snowflake_conn.cursor(DictCursor).execute(
            "Select * from {} limit 1".format(self.topic)).fetchone()
        goldMeta = r'{"CreateTime":\d*,"SnowflakeConnectorPushTime":\d*,"headers":{},"key":{"id":0},"offset":0,"partition":0,"topic":"travis_correct_avrosr_avrosr_\w*"}'
        assert res['ID'] == 0
        assert res['FIRSTNAME'] == "abc0"
        assert res['TIME'] == 1835
        assert res['SOMEFLOAT'] == 21
        assert res['SOMEFLOATNAN'] == 'NaN'
        assert res['SOMEFLOATPOSITIVEINFINITY'] == 'Inf'
        assert res['SOMEFLOATNEGATIVEINFINITY'] == '-Inf'
        assert res['SOMEDOUBLE'] == 15
        assert res['SOMEDOUBLENAN'] == 'NaN'
        assert res['SOMEDOUBLEPOSITIVEINFINITY'] == 'Inf'
        assert res['SOMEDOUBLENEGATIVEINFINITY'] == '-Inf'
        self.driver.regexMatchMeta(res['RECORD_METADATA'], goldMeta)


def clean(self):
        self.driver.cleanTableStagePipe(self.topic)