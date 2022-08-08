from test_suit.test_utils import RetryableError, NonRetryableError
import json

# test if each type of data fit into the right column with the right type
# also test if the metadata column is automatically added
class TestSchemaMapping:
    def __init__(self, driver, nameSalt):
        self.driver = driver
        self.fileName = "travis_correct_schema_mapping"
        self.topic = self.fileName + nameSalt

        print("Initializting" + self.fileName)

        self.driver.snowflake_conn.cursor().execute(
            "Create or replace table {} (PERFORMANCE_STRING STRING, PERFORMANCE_CHAR CHAR, PERFORMANCE_HEX BINARY, RATING_INT NUMBER, RATING_DOUBLE DOUBLE, APPROVAL BOOLEAN, APPROVAL_DATE DATE, APPROVAL_TIME TIME, INFO_ARRAY ARRAY, INFO VARIANT)".format(self.topic))

        self.record = {
            'PERFORMANCE_STRING': 'Excellent',
            'PERFORMANCE_CHAR': 'A',
            'PERFORMANCE_HEX': 'FFFFFFFF',
            'RATING_INT': 100,
            'RATING_DOUBLE': 0.99,
            'APPROVAL': 'true',
            'APPROVAL_DATE': '15-Jun-2022',
            'APPROVAL_TIME': '23:59:59.999999999',
            'INFO_ARRAY': ['HELLO', 'WORLD'],
            'INFO': {
                'TREE_1': 'APPLE',
                'TREE_2': 'PINEAPPLE'
            }
        }
        self.record_literal = r"{'PERFORMANCE_STRING': 'Excellent', 'PERFORMANCE_CHAR': 'A','PERFORMANCE_HEX': 'FFFFFFFF','RATING_INT': 100,'RATING_DOUBLE': 0.99,'APPROVAL': 'true','APPROVAL_DATE': '15-Jun-2022','APPROVAL_TIME': '23:59:59.999999999','INFO_ARRAY': ['HELLO', 'WORLD'],'INFO': {'TREE_1': 'APPLE','TREE_2': 'PINEAPPLE'}}"

    def getConfigFileName(self):
        return self.fileName + ".json"

    def send(self):
        key = []
        value = []
        for e in range(100):
            key.append(json.dumps({'number': str(e)}).encode('utf-8'))
            value.append(json.dumps(self.record).encode('utf-8'))
        self.driver.sendBytesData(self.topic, value, key)

    def verify(self, round):
        rows = self.driver.snowflake_conn.cursor().execute(
            "desc table {}".format(self.topic)).fetchall()

        metadata_exist = False
        for row in rows:
            if row[0] == 'RECORD_METADATA':
                metadata_exist = True
        if not metadata_exist:
            raise NonRetryableError("Metadata column was not created")

        res = self.driver.snowflake_conn.cursor().execute(
            "SELECT count(*) FROM {}".format(self.topic)).fetchone()[0]
        if res == 0:
            raise RetryableError()
        elif res != 100:
            raise NonRetryableError("Number of record in table is different from number of record sent")

        # validate content of line 1
        print("")
        res = self.driver.snowflake_conn.cursor().execute(
            "Select * from {} limit 1".format(self.topic)).fetchone()
        res_col_info = self.driver.snowflake_conn.cursor().execute(
            "Select * from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME = '{}'".format(self.topic)).fetchall();
        res_col = {}
        for col in res_col_info:
            res_col[col[3]] = col[4] - 1
        # res_col maps column names to column index

        goldMeta = r'{"key":{"number":"0"},"offset":0,"partition":0}'
        self.record['RECORD_METADATA'] = goldMeta
        self.driver.regexMatchOneLineSchematized(res, res_col, self.record)

    def clean(self):
        self.driver.cleanTableStagePipe(self.topic)
