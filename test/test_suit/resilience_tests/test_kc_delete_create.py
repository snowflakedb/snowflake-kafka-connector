import datetime

from test_suit.test_utils import RetryableError, NonRetryableError, ResetAndRetry
import json
from time import sleep
from test_suit.base_e2e import BaseE2eTest

# sends data 1/2
# deletes the connector
# creates the connector
# sends data 2/2
# verifies that 2 rounds of data were ingested
class TestKcDeleteCreate(BaseE2eTest):
    def __init__(self, driver, nameSalt):
        self.driver = driver
        self.nameSalt = nameSalt
        self.fileName = "test_kc_delete_create"
        self.topic = self.fileName + nameSalt
        self.connectorName = self.fileName + nameSalt

        self.sleepTime = 10

        self.topicNum = 1
        self.partitionNum = 1
        self.recordNum = 1000

        self.expectedsends = 0

        # create topic and partitions in constructor since the post REST api will automatically create topic with only one partition
        self.driver.createTopics(self.topic, self.partitionNum, 1)

    def getConfigFileName(self):
        return self.fileName + ".json"

    def send(self):
        self.__sendbytes()

        self.driver.deleteConnector(self.connectorName)
        print("Waiting {} seconds for method to complete".format(str(self.sleepTime)))
        sleep(self.sleepTime)

        self.driver.createConnector(self.getConfigFileName(), self.nameSalt)
        print("Waiting {} seconds for method to complete".format(str(self.sleepTime)))
        sleep(self.sleepTime)

        self.__sendbytes()

    def verify(self, round):
        # verify record count
        goalCount = self.recordNum * self.expectedsends
        res = self.driver.select_number_of_records(self.topic)

        print("Count records in table {}={}. Goal record count: {}".format(self.topic, str(res), str(goalCount)))

        if res < goalCount:
            print("Less records than expected, will retry")
            raise RetryableError()
        elif res > goalCount:
            print("Topic:" + self.topic + " count is more, duplicates detected")
            raise NonRetryableError("Duplication occurred, number of record in table is larger than number of record sent")
        else:
            print("Success - expected number of records found")

    def clean(self):
        # dropping of stage and pipe doesnt apply for snowpipe streaming. (It executes drop if exists)
        self.driver.cleanTableStagePipe(self.topic)
        return

    def __sendbytes(self):
        print("Sending {} records".format(str(self.recordNum)))
        key = []
        value = []
        for e in range(self.recordNum):
            value.append(json.dumps(
                {'numbernumbernumbernumbernumbernumbernumbernumbernumbernumbernumbernumber': str(e)}
            ).encode('utf-8'))
        self.driver.sendBytesData(self.topic, value, key, 0)
        self.expectedsends = self.expectedsends + 1
        sleep(2)