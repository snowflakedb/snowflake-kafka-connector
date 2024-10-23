from test_suit.test_utils import RetryableError, NonRetryableError
import json
import datetime
from test_suit.base_e2e import BaseE2eTest

# Test if incorrect data with a schematized column gets send to DLQ
# It sends a String to a column with number data type - Expectation is that we send it to DLQ
# This test is only running in kafka versions > 2.6.1 since DLQ apis are available only in later versions
# Check createKafkaRecordErrorReporter in Java code
class TestSnowpipeStreamingSchemaMappingDLQ(BaseE2eTest):
    def __init__(self, driver, nameSalt):
        self.driver = driver
        self.fileName = "snowpipe_streaming_schema_mapping_dlq"
        self.topic = self.fileName + nameSalt
        # Both for correct and incorrect data
        self.recordNum = 10

        self.expected_record_count_in_table = 0
        # 2 types of incorrect records are sent
        self.expected_record_count_in_dlq = 2 * self.recordNum

        # Create or replace is performed just after
        self.driver.snowflake_conn.cursor().execute(
            'Create or replace table {} (PERFORMANCE_STRING STRING, RATING_INT NUMBER)'.format(self.topic))

        # record we send to snowflake
        # RATING_INT is an integer but we send a string which is not parse-able from ingest SDK
        self.incorrect_kafka_record = {
            'PERFORMANCE_STRING': 'Excellent',
            'RATING_INT': "NO-a-NO"
        }

        # record containing list can't be schematized
        self.another_incorrect_kafka_record = [{
            'PERFORMANCE_STRING': 'Excellent',
            'RATING_INT': 100
        }]

        self.correct_kafka_record = {
            'PERFORMANCE_STRING': 'Excellent',
            'RATING_INT': 100
        }

        self.gold = {
            'PERFORMANCE_STRING': 'Excellent',
            'RATING_INT': 100
        }

    def getConfigFileName(self):
        return self.fileName + ".json"

    def send(self):
        # Send incorrect data and send correct data, in serialized fashion
        self.__sendRecord(self.incorrect_kafka_record, self.recordNum)
        self.__sendRecord(self.another_incorrect_kafka_record, self.recordNum)
        self.__sendRecord(self.correct_kafka_record, self.recordNum)

    def __sendRecord(self, record, number_of_records):
        key = []
        value = []
        for e in range(number_of_records):
            key.append(json.dumps({'number': str(e)}).encode('utf-8'))
            value.append(json.dumps(record).encode('utf-8'))
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

        # recordNum of records should be inserted
        res = self.driver.select_number_of_records(self.topic)
        if res == 0:
            raise RetryableError()
        elif res != self.recordNum:
            raise NonRetryableError("Number of record in table is different from number of record sent")
        else:
            print("Found required:{} offsets in target table:{}".format(self.recordNum, self.topic))

        if self.driver.get_kafka_version() == "5.5.11" or self.driver.get_kafka_version() == "2.5.1":
            print("Data cannot be verified in DLQ in old versions. Putting Data from Kafka Connect to DLQ is only supported in versions >= 2.6")
        else:
            # Last offset number in dlq self.recordNum - 1
            offsets_in_dlq = self.driver.consume_messages_dlq(self.fileName, 0, self.expected_record_count_in_dlq - 1)

            if offsets_in_dlq == self.expected_record_count_in_dlq:
                print("Correct Offset found in DLQ:{}".format(str(offsets_in_dlq)))
            else:
                raise NonRetryableError("Offsets count found in DLQ:{}".format(offsets_in_dlq))

        # Correct Data should be present regardless
        # validate content of line 1
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
