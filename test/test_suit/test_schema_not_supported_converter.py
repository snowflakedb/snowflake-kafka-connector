import json

from test_suit.test_utils import NonRetryableError
from test_suit.base_e2e import BaseE2eTest



# test if each type of data fit into the right column with the right type
# also test if the metadata column is automatically added
class TestSchemaNotSupportedConverter(BaseE2eTest):
    def __init__(self, driver, nameSalt):
        self.driver = driver
        self.fileName = "travis_correct_schema_not_supported_converter"
        self.topic = self.fileName + nameSalt

        self.driver.snowflake_conn.cursor().execute(
            'Create or replace table {} (PERFORMANCE_STRING STRING, "case_sensitive_PERFORMANCE_CHAR" CHAR, PERFORMANCE_HEX BINARY, RATING_INT NUMBER, RATING_DOUBLE DOUBLE, APPROVAL BOOLEAN, APPROVAL_DATE DATE, APPROVAL_TIME TIME, INFO_ARRAY ARRAY, INFO VARIANT, INFO_OBJECT OBJECT)'.format(
                self.topic))

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
        res = self.driver.select_number_of_records(self.topic)
        if res != 0:
            raise NonRetryableError("Nothing should be ingested with not supported converters.")

    def clean(self):
        self.driver.cleanTableStagePipe(self.topic)
