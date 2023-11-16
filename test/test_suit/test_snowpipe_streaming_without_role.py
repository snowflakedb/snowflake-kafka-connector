import datetime

from test_suit.test_utils import RetryableError, NonRetryableError
import json
from time import sleep

class TestSnowpipeStreamingIngestWithoutRole:
    def __init__(self, driver, nameSalt):
        self.driver = driver
        self.fileName = "travis_correct_snowpipe_streaming_no_role"
        self.topic = self.fileName + nameSalt
        self.recordNum = 100

    def getConfigFileName(self):
        return self.fileName + ".json"

    def send(self):
        key = []
        value = []

        # send two less record because we are sending tombstone records. tombstone ingestion is enabled by default
        for e in range(self.recordNum - 2):
            value.append(json.dumps(
                {'numbernumbernumbernumbernumbernumbernumbernumbernumbernumbernumbernumber': str(e)}
            ).encode('utf-8'))

        # append tombstone except for 2.5.1 due to this bug: https://issues.apache.org/jira/browse/KAFKA-10477
        if self.driver.testVersion == '2.5.1':
            value.append(json.dumps(
                {'numbernumbernumbernumbernumbernumbernumbernumbernumbernumbernumbernumber': str(self.recordNum - 1)}
            ).encode('utf-8'))
            value.append(json.dumps(
                {'numbernumbernumbernumbernumbernumbernumbernumbernumbernumbernumbernumber': str(self.recordNum)}
            ).encode('utf-8'))
        else:
            value.append(None)
            value.append("") # community converters treat this as a tombstone

        self.driver.sendBytesData(self.topic, value, key)
        sleep(2)

    def verify(self, round):
        res = self.driver.snowflake_conn.cursor().execute(
            "SELECT count(*) FROM {}".format(self.topic)).fetchone()[0]
        print("Count records in table {}={}".format(self.topic, str(res)))
        if res < self.recordNum:
            print("Topic:" + self.topic + " count is less, will retry")
            raise RetryableError()
        elif res > self.recordNum:
            print("Topic:" + self.topic + " count is more, duplicates detected")
            raise NonRetryableError("Duplication occurred, number of record in table is larger than number of record sent")
        else:
            print("Table:" + self.topic + " count is exactly " + str(self.recordNum))

    def clean(self):
        # dropping of stage and pipe doesnt apply for snowpipe streaming. (It executes drop if exists)
        self.driver.cleanTableStagePipe(self.topic)
        return
