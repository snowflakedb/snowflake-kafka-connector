import json
import random

from test_suit.test_utils import NonRetryableError
from test_suit.base_e2e import BaseE2eTest


# test if the ingestion works when the schematization alter table invalidation happens
# halfway through a batch
class TestSchemaEvolutionWithRandomRowCount(BaseE2eTest):
    def __init__(self, driver, nameSalt):
        self.driver = driver
        self.fileName = "test_schema_evolution_w_random_row_count"
        self.topics = []
        self.table = self.fileName + nameSalt

        # records
        self.initialRecordCount = random.randrange(1,300)
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

        self.gold_type = {
            'PERFORMANCE_STRING': 'VARCHAR',
            'PERFORMANCE_CHAR': 'VARCHAR',
            'RATING_INT': 'NUMBER',
            'RATING_DOUBLE': 'FLOAT',
            'APPROVAL': 'BOOLEAN',
            'RECORD_METADATA': 'VARIANT'
        }

        self.gold_columns = [columnName for columnName in self.gold_type]

    def getConfigFileName(self):
        return self.fileName + ".json"

    def send(self):
        print("Got random record count of {}".format(str(self.initialRecordCount)))

        for i, topic in enumerate(self.topics):
            # send initial batch
            key = []
            value = []
            for e in range(self.initialRecordCount):
                key.append(json.dumps({'number': str(e)}).encode('utf-8'))
                value.append(json.dumps(self.records[i]).encode('utf-8'))
            self.driver.sendBytesData(topic, value, key)

            # send second batch that should flush
            key = []
            value = []
            for e in range(self.flushRecordCount):
                key.append(json.dumps({'number': str(e)}).encode('utf-8'))
                value.append(json.dumps(self.records[i]).encode('utf-8'))
            self.driver.sendBytesData(topic, value, key)

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
