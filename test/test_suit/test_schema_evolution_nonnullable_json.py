import json

from test_suit.test_utils import NonRetryableError
from test_suit.base_e2e import BaseE2eTest


# test if the table is updated with the correct column
class TestSchemaEvolutionNonNullableJson(BaseE2eTest):
    def __init__(self, driver, nameSalt):
        self.driver = driver
        self.fileName = "travis_correct_schema_evolution_nonnullable_json"
        self.topics = []
        self.table = self.fileName + nameSalt
        self.recordNum = 100
        self.topicNum = 1

        for i in range(self.topicNum):
            self.topics.append(self.table + str(i))

        self.driver.snowflake_conn.cursor().execute(
            'Create or replace table {} ("case_sensitive_PERFORMANCE_STRING" STRING not null)'.format(self.table))

        self.driver.snowflake_conn.cursor().execute(
            "alter table {} set ENABLE_SCHEMA_EVOLUTION = true".format(self.table))

        self.records = []

        self.records.append({
            '"case_sensitive_PERFORMANCE_CHAR"': 'A',
            'RATING_INT': 100
        })

        self.gold_type = {
            'case_sensitive_PERFORMANCE_STRING': 'VARCHAR',
            'case_sensitive_PERFORMANCE_CHAR': 'VARCHAR',
            'RATING_INT': 'NUMBER',
            'RECORD_METADATA': 'VARIANT'
        }

        self.gold_columns = [columnName for columnName in self.gold_type]

    def getConfigFileName(self):
        return self.fileName + ".json"

    def send(self):
        for i, topic in enumerate(self.topics):
            key = []
            value = []
            for e in range(self.recordNum):
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
            if row[3] != 'Y':
                raise NonRetryableError("Column {} is non-nullable".format(row[0]))

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
