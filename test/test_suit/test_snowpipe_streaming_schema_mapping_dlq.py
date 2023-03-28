from test_suit.test_utils import RetryableError, NonRetryableError
import json
import datetime

# Test if incorrect data with a schematized column gets send to DLQ
# It sends a String to a column with number data type - Expectation is that we send it to DLQ
# This test is only running in kafka versions > 2.6.1 since DLQ apis are available only in later versions
# Check createKafkaRecordErrorReporter in Java code
class TestSnowpipeStreamingSchemaMappingDLQ:
    def __init__(self, driver, nameSalt):
        self.driver = driver
        self.fileName = "snowpipe_streaming_schema_mapping_dlq"
        self.topic = self.fileName + nameSalt
        self.recordNum = 10

        self.expected_record_count_in_table = 0
        self.expected_record_count_in_dlq = self.recordNum

        # Create or replace is performed just after
        self.driver.snowflake_conn.cursor().execute(
            'Create or replace table {} (PERFORMANCE_STRING STRING, RATING_INT NUMBER)'.format(self.topic))

        # record we send to snowflake
        # RATING_INT is an integer but we send a string which is not parse-able from ingest SDK
        self.record = {
            'PERFORMANCE_STRING': 'Excellent',
            'RATING_INT': "NO-a-NO"
        }

        # output expected
        self.gold = {
            'PERFORMANCE_STRING': 'Excellent'
        }

    def getConfigFileName(self):
        return self.fileName + ".json"

    def send(self):
        key = []
        value = []
        for e in range(self.recordNum):
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

        # no data should be inserted
        res = self.driver.snowflake_conn.cursor().execute(
            "SELECT count(*) FROM {}".format(self.topic)).fetchone()[0]
        if res != 0:
            raise RetryableError()

        if self.driver.get_kafka_version() == "5.5.11" or self.driver.get_kafka_version() == "2.5.1":
            print("Data cannot be verified in DLQ in old versions. Putting Data from Kafka Connect to DLQ is only supported in versions >= 2.6")
        else:
            # Last offset number in dlq is one less than count of messages sent
            offsets_in_dlq = self.driver.consume_messages_dlq(self.fileName, 0, self.expected_record_count_in_dlq - 1)

            if offsets_in_dlq == self.expected_record_count_in_dlq:
                print("Offsets in DLQ:{}".format(str(offsets_in_dlq)))
            else:
                raise NonRetryableError("Offsets count found in DLQ:{}".format(offsets_in_dlq))

    def clean(self):
        self.driver.cleanTableStagePipe(self.topic)
