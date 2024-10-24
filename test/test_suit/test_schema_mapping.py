from test_suit.test_utils import RetryableError, NonRetryableError
import json
import datetime
from test_suit.base_e2e import BaseE2eTest


# test if each type of data fit into the right column with the right type
# also test if the metadata column is automatically added
class TestSchemaMapping(BaseE2eTest):
    def __init__(self, driver, nameSalt):
        self.driver = driver
        self.fileName = "travis_correct_schema_mapping"
        self.topic = self.fileName + nameSalt

        self.driver.snowflake_conn.cursor().execute(
            'Create or replace table {} (PERFORMANCE_STRING STRING, "case_sensitive_PERFORMANCE_CHAR" CHAR, PERFORMANCE_HEX BINARY, RATING_INT NUMBER, RATING_DOUBLE DOUBLE, APPROVAL BOOLEAN, APPROVAL_DATE DATE, APPROVAL_TIME TIME, INFO_ARRAY ARRAY, INFO VARIANT, INFO_OBJECT OBJECT)'.format(self.topic))

        self.record = {
            'PERFORMANCE_STRING': 'Excellent',
            '"case_sensitive_PERFORMANCE_CHAR"': 'A',
            'PERFORMANCE_HEX': 'FFFFFFFF',
            'RATING_INT': 100,
            'RATING_DOUBLE': 0.99,
            'APPROVAL': 'true',
            'APPROVAL_DATE': '2022-06-15',
            'APPROVAL_TIME': '23:59:59.999999',
            'INFO_ARRAY': ['HELLO', 'WORLD'],
            'INFO': {
                'TREE_1': 'APPLE',
                'TREE_2': 'PINEAPPLE'
            },
            'INFO_OBJECT': {
                'TREE_1': 'APPLE',
                'TREE_2': 'PINEAPPLE'
            }
        }

        self.gold = {
            'PERFORMANCE_STRING': 'Excellent',
            'case_sensitive_PERFORMANCE_CHAR': 'A',
            'PERFORMANCE_HEX': b'\xff\xff\xff\xff',
            'RATING_INT': 100,
            'RATING_DOUBLE': 0.99,
            'APPROVAL': True,
            'APPROVAL_DATE': datetime.date(2022, 6, 15),
            'APPROVAL_TIME': datetime.time(23, 59, 59, 999999),
            'INFO_ARRAY': r'["HELLO","WORLD"]',
            'INFO': r'{"TREE_1":"APPLE","TREE_2":"PINEAPPLE"}',
            'INFO_OBJECT': r'{"TREE_1":"APPLE","TREE_2":"PINEAPPLE"}'
        }

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
        res_col = {}

        metadata_exist = False
        for index, row in enumerate(rows):
            if row[0] == 'RECORD_METADATA':
                metadata_exist = True
            res_col[row[0]] = index
        if not metadata_exist:
            raise NonRetryableError("Metadata column was not created")

        res = self.driver.select_number_of_records(self.topic)
        if res == 0:
            raise RetryableError()
        elif res != 100:
            raise NonRetryableError("Number of record in table is different from number of record sent")

        # validate content of line 1
        print("")
        res = self.driver.snowflake_conn.cursor().execute(
            "Select * from {} limit 1".format(self.topic)).fetchone()

        for field in res_col:
            print("Field:", field)
            if field == "RECORD_METADATA":
                continue
            if type(res[res_col[field]]) == str:
                # removing the formating created by sf
                assert ''.join(res[res_col[field]].split()) == self.gold[field], f"expected:{self.gold[field]}, actual:{res[res_col[field]]}"
            else:
                assert res[res_col[field]] == self.gold[field], f"expected:{self.gold[field]}, actual:{res[res_col[field]]}"

    def clean(self):
        self.driver.cleanTableStagePipe(self.topic)
