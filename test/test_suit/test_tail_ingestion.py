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
        self.partitionNum = 1
        self.recordNum = 100

        # create topic and partitions in constructor since the post REST api will automatically create topic with only one partition
        self.driver.createTopics(self.topic, partitionNum=self.partitionNum, replicationNum=1)

    def getConfigFileName(self):
        return self.fileName + ".json"

    def send(self):
        # set up data to send
        key = []
        value = []
        for e in range(self.recordNum):
            value.append(json.dumps(
                {'numbernumbernumbernumbernumbernumbernumbernumbernumbernumbernumbernumber': str(e)}
            ).encode('utf-8'))

        # send 100 rows and immediately stop
        self.driver.sendBytesData(self.topic, value, key, partition=0)
        sleep (5)
        self.driver.deleteConnector(self.topic)

        # sleep (2)
        #
        # # ensure kafka just flushed on time
        # self.driver.deleteConnector(self.topic)
        # sleep(10)
        # self.driver.createConnector(self.getConfigFileName(), self.nameSalt)
        # self.driver.sendBytesData(self.topic, value, key, partition=0)
        #
        # sleep(2)
        #
        # # send 100 rows and immediately shut down
        # self.driver.sendBytesData(self.topic, value, key, partition=0)
        # self.driver.deleteConnector(self.topic)


    def verify(self, round):
        res = self.driver.snowflake_conn.cursor().execute(
            "SELECT count(*) FROM {}".format(self.topic)).fetchone()[0]
        print("Count records in table {}={}".format(self.topic, str(res)))

        goalCount = self.recordNum * self.partitionNum

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
