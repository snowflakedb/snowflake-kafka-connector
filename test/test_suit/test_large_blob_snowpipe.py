from test_suit.test_utils import RetryableError, NonRetryableError
import json
import random
import string
from test_suit.base_e2e import BaseE2eTest


class TestLargeBlobSnowpipe(BaseE2eTest):
    def __init__(self, driver, nameSalt):
        self.driver = driver
        self.fileName = "travis_correct_large_blob_snowpipe"
        self.topic = self.fileName + nameSalt
        self.recordCount = 5
        self.blobSize = 20 * 1024 * 1024  # 20 MiB

    def getConfigFileName(self):
        return self.fileName + ".json"

    def _generateLargeBlob(self, recordId):
        chunkSize = 100 * 1024  # 100KiB chunks
        largeBlobData = []

        for i in range(0, self.blobSize, chunkSize):
            chunk = ''.join(random.choices(
                string.ascii_letters + string.digits,
                k=chunkSize
            ))
            largeBlobData.append(chunk)

        record = {
            'record_id': recordId,
            'timestamp': '2025-09-12T00:00:00Z',
            'large_blob_data': largeBlobData,
        }

        jsonString = json.dumps(record)

        return jsonString.encode('utf-8')

    def send(self):
        print(f"Generating and sending {self.recordCount} large JSON blobs (~{self.blobSize // (1024 * 1024)} MiB each)")
        values = []

        for recordId in range(self.recordCount):
            blobData = self._generateLargeBlob(recordId)
            values.append(blobData)

        self.driver.sendBytesData(self.topic, values, [], 0)

    def verify(self, round):
        res = self.driver.select_number_of_records(self.topic)
        if res < self.recordCount:
            print("Not all records arrived yet, will retry")
            raise RetryableError()
        elif res > self.recordCount:
            raise NonRetryableError(f"Expected {self.recordCount} records but found {res}")

        print(f"Found expected {res} records in table")

        cursor = self.driver.snowflake_conn.cursor()

        records = cursor.execute(
            f"SELECT RECORD_CONTENT:record_id::int AS record_id FROM {self.topic}"
        ).fetchall()

        actualRecordIds = set([record[0] for record in records])
        expectedRecordIds = set(range(self.recordCount))

        if actualRecordIds != expectedRecordIds:
            raise NonRetryableError(f"Record ID mismatch: expected {expectedRecordIds}, got {actualRecordIds}")

        print('Successfully verified all record IDs')

        self.driver.verifyStageIsCleaned(self.topic)

    def clean(self):
        self.driver.cleanTableStagePipe(self.topic)
