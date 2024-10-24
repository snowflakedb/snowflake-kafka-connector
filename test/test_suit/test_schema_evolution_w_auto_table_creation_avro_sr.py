from confluent_kafka import avro
from test_suit.test_utils import NonRetryableError
from test_suit.base_e2e import BaseE2eTest


# test if the table is updated with the correct column
# add test if all the records from different topics safely land in the table
# the table is suppose to be created with only RECORD_METADATA in the beginning
# while the rest of columns should be handled by schema evolution
class TestSchemaEvolutionWithAutoTableCreationAvroSR(BaseE2eTest):
    def __init__(self, driver, nameSalt):
        self.driver = driver
        self.fileName = "travis_correct_schema_evolution_w_auto_table_creation_avro_sr"
        self.topics = []
        self.table = self.fileName + nameSalt

        # records
        self.initialRecordCount = 12
        self.flushRecordCount = 300
        self.recordNum = self.initialRecordCount + self.flushRecordCount

        for i in range(2):
            self.topics.append(self.table + str(i))

        self.records = []

        self.records.append({
            'PERFORMANCE_STRING': 'Excellent',
            'PERFORMANCE_CHAR': 'A',
            'RATING_INT': 100
        })

        self.records.append({
            'PERFORMANCE_STRING': 'Excellent',
            'RATING_DOUBLE': 0.99,
            'APPROVAL': True
        })

        self.ValueSchemaStr = []

        self.ValueSchemaStr.append("""
        {
            "type":"record",
            "name":"value_schema_0",
            "fields":[
                {"name":"PERFORMANCE_STRING","type":"string"},
                {"name":"PERFORMANCE_CHAR","type":"string"},
                {"name":"RATING_INT","type":"int"}
            ]
        }
        """)

        self.ValueSchemaStr.append("""
        {
            "type":"record",
            "name":"value_schema_1",
            "fields":[
                {"name":"PERFORMANCE_STRING","type":"string"},
                {"name":"RATING_DOUBLE","type":"float"},
                {"name":"APPROVAL","type":"boolean"}
            ]
        }
        """)

        self.gold_type = {
            'PERFORMANCE_STRING': 'VARCHAR',
            'PERFORMANCE_CHAR': 'VARCHAR',
            'RATING_INT': 'NUMBER',
            'RATING_DOUBLE': 'FLOAT',
            'APPROVAL': 'BOOLEAN',
            'RECORD_METADATA': 'VARIANT'
        }

        self.gold_columns = [columnName for columnName in self.gold_type]

        self.valueSchema = []

        for valueSchemaStr in self.ValueSchemaStr:
            self.valueSchema.append(avro.loads(valueSchemaStr))

    def getConfigFileName(self):
        return self.fileName + ".json"

    def send(self):
        for i, topic in enumerate(self.topics):
            # send initial batch
            value = []
            for _ in range(self.initialRecordCount):
                value.append(self.records[i])
            self.driver.sendAvroSRData(topic, value, self.valueSchema[i], key=[], key_schema="", partition=0)

            # send second batch that should flush
            value = []
            for _ in range(self.flushRecordCount):
                value.append(self.records[i])
            self.driver.sendAvroSRData(topic, value, self.valueSchema[i], key=[], key_schema="", partition=0)

    def verify(self, round):
        rows = self.driver.snowflake_conn.cursor().execute(
            "desc table {}".format(self.table)).fetchall()
        res_col = {}

        for index, row in enumerate(rows):
            self.gold_columns.remove(row[0])
            if not row[1].startswith(self.gold_type[row[0]]):
                raise NonRetryableError("Column {} has the wrong type. got: {}, expected: {}".format(row[0], row[1],
                                                                                                     self.gold_type[
                                                                                                         row[0]]))
            res_col[row[0]] = index

        print("Columns not in table: ", self.gold_columns)

        for columnName in self.gold_columns:
            raise NonRetryableError("Column {} was not created".format(columnName))

        res = self.driver.snowflake_conn.cursor().execute(
            "SELECT count(*) FROM {}".format(self.table)).fetchone()[0]
        if res != len(self.topics) * self.recordNum:
            print("Number of record expected: {}, got: {}".format(len(self.topics) * self.recordNum, res))
            raise NonRetryableError("Number of record in table is different from number of record sent")

    def clean(self):
        self.driver.cleanTableStagePipe(self.table)
