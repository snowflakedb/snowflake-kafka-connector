import json
from time import sleep

from test_suit.test_utils import NonRetryableError
from test_suit.base_e2e import BaseE2eTest


# test if the table is updated with the correct column, and if the table is
# recreated and updated after it's being dropped
class TestSchemaEvolutionDropTable(BaseE2eTest):
    def __init__(self, driver, nameSalt):
        self.driver = driver
        self.fileName = "travis_correct_schema_evolution_drop_table"
        self.table = self.fileName + nameSalt
        self.recordNum = 100
        self.topic = self.table + "0"

        self.driver.snowflake_conn.cursor().execute(
            "Create or replace table {} (PERFORMANCE_STRING STRING)".format(self.table))
        self.driver.snowflake_conn.cursor().execute(
            "alter table {} set ENABLE_SCHEMA_EVOLUTION = true".format(self.table))

        self.record = {
            'PERFORMANCE_STRING': 'Excellent',
            '"case_sensitive_PERFORMANCE_CHAR"': 'A',
            'RATING_INT': 100
        }

        self.gold_type = {
            'PERFORMANCE_STRING': 'VARCHAR',
            'case_sensitive_PERFORMANCE_CHAR': 'VARCHAR',
            'RATING_INT': 'NUMBER',
            'RECORD_METADATA': 'VARIANT'
        }

        self.gold_columns = [columnName for columnName in self.gold_type]

    def getConfigFileName(self):
        return self.fileName + ".json"

    def send(self):
        key = []
        value = []
        for e in range(self.recordNum):
            key.append(json.dumps({'number': str(e)}).encode('utf-8'))
            value.append(json.dumps(self.record).encode('utf-8'))
        self.driver.sendBytesData(self.topic, value, key)

        # Sleep for some time and then verify the rows are ingested
        sleep(120)
        self.verify("0")

        # Recreate the table
        self.driver.snowflake_conn.cursor().execute(
            "Create or replace table {} (PERFORMANCE_STRING STRING, RECORD_METADATA VARIANT)".format(self.table))
        self.driver.snowflake_conn.cursor().execute(
            "alter table {} set ENABLE_SCHEMA_EVOLUTION = true".format(self.table))

        # Ingest another set of rows
        self.driver.sendBytesData(self.topic, value, key)

    def verify(self, round):
        rows = self.driver.snowflake_conn.cursor().execute(
            "desc table {}".format(self.table)).fetchall()
        res_col = {}

        gold_columns_copy = self.gold_columns.copy()

        for index, row in enumerate(rows):
            gold_columns_copy.remove(row[0])
            if not row[1].startswith(self.gold_type[row[0]]):
                raise NonRetryableError("Column {} has the wrong type. got: {}, expected: {}".format(row[0], row[1],
                                                                                                     self.gold_type[
                                                                                                         row[0]]))
            res_col[row[0]] = index

        print("Columns not in table: ", gold_columns_copy)

        for columnName in gold_columns_copy:
            raise NonRetryableError("Column {} was not created".format(columnName))

        res = self.driver.snowflake_conn.cursor().execute(
            "SELECT count(*) FROM {}".format(self.table)).fetchone()[0]
        if res != self.recordNum:
            print("Number of record expected: {}, got: {}".format(self.recordNum, res))
            raise NonRetryableError("Number of record in table is different from number of record sent")

    def clean(self):
        self.driver.cleanTableStagePipe(self.table)
