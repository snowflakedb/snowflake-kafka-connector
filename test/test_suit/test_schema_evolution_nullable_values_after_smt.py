import json
from snowflake.connector import DictCursor
from test_suit.test_utils import NonRetryableError, RetryableError


# Testing behavior for behavior.on.null.values = IGNORE and SMTs that can return null values.
# With enabled schematization.
class TestSchemaEvolutionNullableValuesAfterSmt:

    def __init__(self, driver, nameSalt):
        self.driver = driver
        self.fileName = 'schema_evolution_nullable_values_after_smt'
        self.table = self.fileName + nameSalt
        self.topic = self.table

        self.total_events = 200

        self.driver.snowflake_conn.cursor().execute(
            f'create or replace table {self.table} (index number not null)')

        self.driver.snowflake_conn.cursor().execute(
            f'alter table {self.table} set enable_schema_evolution = true')

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
        res = cur.execute(f'select index, from_optional_field, record_metadata:offset::number as offset from {self.table}').fetchall()

        if len(res) == 0:
            raise RetryableError()
        elif len(res) != 100:
            raise NonRetryableError('Number of record in table is different from number of expected records')

        expected_idx = range(0, self.total_events, 2) # Only every other event is going to be ingested.
        expected_res = [{'INDEX': idx, 'FROM_OPTIONAL_FIELD': True, 'OFFSET': idx} for idx in expected_idx]

        if expected_res != res:
            raise NonRetryableError(f"Invalid result values. Expected: {expected_res}, got: {res}")

    def clean(self):
        self.driver.cleanTableStagePipe(self.topic, self.table)
