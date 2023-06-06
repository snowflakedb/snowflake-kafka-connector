import datetime

from test_suit.test_utils import RetryableError, NonRetryableError
import json
from time import sleep

class TestTailIngestion:
    def __init__(self, driver, nameSalt):
        self.driver = driver
        self.fileName = "test_tail_ingestion"
        self.topic = self.fileName + nameSalt
        self.nameSalt = nameSalt

        self.topicNum = 1
        self.initialRecordCount = 100
        self.testRecordCount = 10

        # create topic and partitions in constructor since the post REST api will automatically create topic with only one partition
        self.driver.createTopics(self.topic, partitionNum=1, replicationNum=1)

    def getConfigFileName(self):
        return self.fileName + ".json"

    def send(self):
        # set up data to send
        key = []
        records_100 = []
        records_10 = []
        for e in range(self.initialRecordCount):
            records_100.append(json.dumps(
                {'numbernumbernumbernumbernumbernumbernumbernumbernumbernumbernumbernumber': str(e)}
            ).encode('utf-8'))

        for e in range(self.testRecordCount):
            records_10.append(json.dumps(
                {'numbernumbernumbernumbernumbernumbernumbernumbernumbernumbernumbernumber': str(e)}
            ).encode('utf-8'))

        # send 100 rows which should trigger a flush
        self.driver.sendBytesData(self.topic, records_100, key, partition=0)

        # send 10 rows, which shouldnt hit any buffer thresholds
        self.driver.sendBytesData(self.topic, records_10, key, partition=0)

        # wait to ensure data is all sent
        sleep(2)

        # delete connector should force flush the above 10 rows
        self.driver.deleteConnector(self.topic)


    def verify(self, round):
        res = self.driver.snowflake_conn.cursor().execute(
            "SELECT count(*) FROM {}".format(self.topic)).fetchone()[0]
        print("Count records in table {}={}".format(self.topic, str(res)))

        goalCount = self.initialRecordCount + self.testRecordCount

        if res < (goalCount):
            print("Topic:" + self.topic + " count is less, will retry")
            raise RetryableError()
        elif res > (goalCount):
            print("Topic:" + self.topic + " count is more, duplicates detected")
            raise NonRetryableError("Duplication occurred, number of record in table is larger than number of record sent")
        else:
            print("Table:" + self.topic + " count is exactly " + str(goalCount))

    def clean(self):
        # dropping of stage and pipe doesnt apply for snowpipe streaming. (It executes drop if exists)
        self.driver.cleanTableStagePipe(self.topic)
        return
