from decimal import Decimal

from confluent_kafka import avro
from test_suit.test_utils import NonRetryableError
from test_suit.base_e2e import BaseE2eTest


# test if the table is updated with the correct column
# add test if all the records from different topics safely land in the table
class TestSchemaEvolutionAvroSRLogicalTypes(BaseE2eTest):
    def __init__(self, driver, nameSalt):
        self.driver = driver
        self.fileName = "travis_correct_schema_evolution_avro_sr_logical_types"
        self.topics = []
        self.table = self.fileName + nameSalt
        self.recordNum = 100

        for i in range(2):
            self.topics.append(self.table + str(i))

        self.driver.snowflake_conn.cursor().execute(
            "Create or replace table {} (PERFORMANCE_STRING STRING)".format(self.table))

        self.driver.snowflake_conn.cursor().execute(
            "alter table {} set ENABLE_SCHEMA_EVOLUTION = true".format(self.table))

        self.records = []

        self.records.append({
            'PERFORMANCE_STRING': 'Excellent',
            'TIME_MILLIS': 10,
            'DATE': 11,
            'TIMESTAMP_MILLIS': 12,
            'DECIMAL': Decimal(4.0)
        })

        self.records.append({
            'PERFORMANCE_STRING': 'Excellent',
            'RATING_DOUBLE': 0.99,
        })

        self.ValueSchemaStr = []

        self.ValueSchemaStr.append("""
        {
            "type":"record",
            "name":"value_schema_0",
            "fields":[
                {"name":"PERFORMANCE_STRING","type":"string"},
                {"name":"TIME_MILLIS","type":{"type":"int","logicalType":"time-millis"}},
                {"name":"DATE","type":{"type":"int","logicalType":"date"}},
                {"name":"DECIMAL","type":{"type":"bytes","logicalType":"decimal", "precision":4, "scale":2}},
                {"name":"TIMESTAMP_MILLIS","type":{"type":"long","logicalType":"timestamp-millis"}}
            ]
        }
        """)

        self.ValueSchemaStr.append("""
        {
            "type":"record",
            "name":"value_schema_1",
            "fields":[
                {"name":"RATING_DOUBLE","type":"float"},
                {"name":"PERFORMANCE_STRING","type":"string"}
            ]
        }
        """)

        self.gold_type = {
            'PERFORMANCE_STRING': 'VARCHAR',
            'RATING_DOUBLE': 'FLOAT',
            'TIME_MILLIS': 'TIME(6)',
            'DATE': 'DATE',
            'TIMESTAMP_MILLIS': 'TIMESTAMP_NTZ(6)',
            'DECIMAL': 'VARCHAR',
            'RECORD_METADATA': 'VARIANT'
        }

        self.gold_columns = [columnName for columnName in self.gold_type]

        self.valueSchema = []

        for valueSchemaStr in self.ValueSchemaStr:
            self.valueSchema.append(avro.loads(str(valueSchemaStr)))

    def getConfigFileName(self):
        return self.fileName + ".json"

    def send(self):
        for i, topic in enumerate(self.topics):
            value = []
            for _ in range(self.recordNum):
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
