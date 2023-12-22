import json
import random

from test_suit.test_utils import NonRetryableError


# test if the table is updated with the correct column
# add test if all the records from different topics safely land in the table
class TestSchemaEvolutionMissingOffsets:
    def __init__(self, driver, nameSalt):
        self.driver = driver
        self.fileName = "test_schema_evolution_missingoffsets"
        self.topics = []
        self.table = self.fileName + nameSalt
        self.recordNum = 100 # 100 total records sent
        self.lastRecordOffset = 120 # 20 records missing

        for i in range(2):
            self.topics.append(self.table + str(i))

        self.driver.snowflake_conn.cursor().execute(
            "Create or replace table {} (PERFORMANCE_STRING STRING)".format(self.table))

        self.driver.snowflake_conn.cursor().execute(
            "alter table {} set ENABLE_SCHEMA_EVOLUTION = true".format(self.table))

        self.records = []

        self.records.append({
            'PERFORMANCE_STRING': 'Excellent',
            '"case_sensitive_PERFORMANCE_CHAR"': 'A',
            'RATING_INT': 100
        })

        self.records.append({
            'PERFORMANCE_STRING': 'Excellent',
            'RATING_DOUBLE': 0.99,
            'APPROVAL': True
        })

        self.gold_type = {
            'PERFORMANCE_STRING': 'VARCHAR',
            'case_sensitive_PERFORMANCE_CHAR': 'VARCHAR',
            'RATING_INT': 'NUMBER',
            'RATING_DOUBLE': 'FLOAT',
            'APPROVAL': 'BOOLEAN',
            'RECORD_METADATA': 'VARIANT'
        }

        self.gold_columns = [columnName for columnName in self.gold_type]

    def getConfigFileName(self):
        return self.fileName + ".json"

    def send(self):
        for i, topic in enumerate(self.topics):
            key = []
            value = []

            # first 50 are consecutive (0-49)
            for e in range(50):
                key.append(json.dumps({'number': str(e)}).encode('utf-8'))
                value.append(json.dumps(self.records[i]).encode('utf-8'))

            # add two tombstone records (50, 51)
            # append tombstone except for 2.5.1 due to this bug: https://issues.apache.org/jira/browse/KAFKA-10477
            if self.driver.testVersion == '2.5.1':
                value.append(json.dumps(self.records[i]).encode('utf-8'))
                value.append(json.dumps(self.records[i]).encode('utf-8'))
            else:
                value.append(None)
                value.append("") # community converters treat this as a tombstone
            key.append(json.dumps({'number': str(self.recordNum - 1)}).encode('utf-8'))
            key.append(json.dumps({'number': str(self.recordNum)}).encode('utf-8'))

            # generate random missing offsets
            missingOffsets = random.sample(range(52, self.lastRecordOffset), 20)

            # next 48 are not consecutive (52-99)
            for e in range(52, self.lastRecordOffset - 1):
                if e not in missingOffsets:
                    key.append(json.dumps({'number': str(e)}).encode('utf-8'))
                    value.append(json.dumps(self.records[i]).encode('utf-8'))

            self.driver.sendBytesData(topic, value, key)

        # wait and send one record to each topic to kick off time flush
        sleep(10)
        for i, topic in enumerate(self.topics):
            key = []
            value = []
            key.append(json.dumps({'number': str(self.flushRecordCount)}).encode('utf-8'))
            value.append(json.dumps(self.records[i]).encode('utf-8'))
            self.driver.sendBytesData(topic, value, key)

        # wait for ingestion to complete
        sleep(10)

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
#         self.driver.cleanTableStagePipe(self.table)
        print("not deleting table")
