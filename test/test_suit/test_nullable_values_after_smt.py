import json
from snowflake.connector import DictCursor
from test_suit.test_utils import NonRetryableError, RetryableError


# Testing behavior for behavior.on.null.values = IGNORE and SMTs that can return null values.
# Uses a Snowpipe based connector.
class TestNullableValuesAfterSmt:

    def __init__(self, driver, nameSalt):
        self.driver = driver
        self.fileName = 'nullable_values_after_smt'
        self.table = self.fileName + nameSalt
        self.topic = self.table

    def getConfigFileName(self):
        return self.fileName + '.json'

    def send(self):
        value = []
        for idx in range(200):
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

        idx = 0
        for rec in res:
            expected_content = { 'index': idx, 'from_optional_field': True }

            if json.loads(rec['RECORD_CONTENT']) != expected_content:
                raise NonRetryableError(f"Invalid index value. Expected: {expected_content}, got: {rec['RECORD_CONTENT']}")

            if rec['OFFSET'] != idx:
                raise NonRetryableError(f"Invalid offset value. Expected: {idx}, got: {rec['OFFSET']}")

            idx += 2  # Only every other event is going to be ingested.

        self.driver.verifyStageIsCleaned(self.topic)

    def clean(self):
        self.driver.cleanTableStagePipe(self.topic, self.table)
