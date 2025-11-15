import json
from snowflake.connector import DictCursor
from test_suit.test_utils import NonRetryableError, RetryableError
from test_suit.base_e2e import BaseE2eTest


# Testing behavior for behavior.on.null.values = IGNORE and SMTs that can return null values.
class TestNullableValuesAfterSmt(BaseE2eTest):

    def __init__(self, driver, nameSalt):
        self.driver = driver
        self.fileName = 'nullable_values_after_smt'
        self.table = self.fileName + nameSalt
        self.topic = self.table
        self.total_events = 200
        self.driver.create_table(self.table)
        # the configuration contains SMT only content of optionalField is sent to our connector after SMT, that is why the created table has only three columns
        self.driver.snowflake_conn.cursor().execute(f"create or replace table {self.table} (index number, from_optional_field boolean, record_metadata variant)")

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
        # the configuration contains SMT only content of optionalField is sent to our connector after SMT, that is why the select has only three columns
        res = cur.execute(f'select index, from_optional_field, record_metadata:offset::number as offset from {self.table}').fetchall()

        if len(res) == 0:
            raise RetryableError()
        elif len(res) != 100:
            raise NonRetryableError('Number of record in table is different from number of expected records')

        parsed_res = [{'index': rec['INDEX'], 'from_optional_field': rec['FROM_OPTIONAL_FIELD'], 'offset': rec['OFFSET']} for rec in res]

        expected_idx = range(0, self.total_events, 2) # Only every other event is going to be ingested.
        expected_res = [{'index': idx, 'from_optional_field': True, 'offset': idx} for idx in expected_idx]

        if expected_res != parsed_res:
            raise NonRetryableError(f"Invalid result values. Expected: {expected_res}, got: {parsed_res}")



    def clean(self):
        self.driver.cleanTableStagePipe(self.topic, self.table)
