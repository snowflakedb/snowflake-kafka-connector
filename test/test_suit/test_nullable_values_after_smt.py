import json
from snowflake.connector import DictCursor
from test_suit.test_utils import NonRetryableError, RetryableError
from test_suit.base_e2e import BaseE2eTest


# Testing behavior for behavior.on.null.values = IGNORE and SMTs that can return null values.
# Uses a Snowpipe based connector.
class TestNullableValuesAfterSmt(BaseE2eTest):

    def __init__(self, driver, nameSalt):
        self.driver = driver
        self.fileName = 'nullable_values_after_smt'
        self.table = self.fileName + nameSalt
        self.topic = self.table

        self.total_events = 200

    def getConfigFileName(self):
        return self.fileName + '.json'

    def send(self):
        value = []
        for idx in range(self.total_events):
            event = { 'index': idx, 'someKey': 'someValue' }

            if idx % 2 == 0: # Only every other event contains optionalField.
                optional_value = { 'index': idx, 'from_optional_field': True }
                event.update({ 'optionalField': optional_value })

            value.append(json.dumps(event).encode('utf-8'))

        self.driver.sendBytesData(self.topic, value)

    def verify(self, round):
        cur = self.driver.snowflake_conn.cursor(DictCursor)
        res = cur.execute(f'select record_content, record_metadata:offset::number as offset from {self.table}').fetchall()

        if len(res) == 0:
            raise RetryableError()
        elif len(res) != 100:
            raise NonRetryableError('Number of record in table is different from number of expected records')

        # Originally RECORD_CONTENT is returned as a json string.
        parsed_res = [{'RECORD_CONTENT': json.loads(rec['RECORD_CONTENT']), 'OFFSET': rec['OFFSET']} for rec in res]

        expected_idx = range(0, self.total_events, 2) # Only every other event is going to be ingested.
        expected_res = [{'RECORD_CONTENT': {'index': idx, 'from_optional_field': True}, 'OFFSET': idx} for idx in expected_idx]

        if expected_res != parsed_res:
            raise NonRetryableError(f"Invalid result values. Expected: {expected_res}, got: {parsed_res}")

        self.driver.verifyStageIsCleaned(self.topic)

    def clean(self):
        self.driver.cleanTableStagePipe(self.topic, self.table)
