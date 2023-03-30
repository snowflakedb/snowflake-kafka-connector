import datetime

from test_suit.test_utils import RetryableError, NonRetryableError
import json
from time import sleep

class TestKcPause:
    def __init__(self, driver, nameSalt):
        self.driver = driver
        self.topics = []
        self.topicNum = 10
        self.partitionNum = 3
        self.recordNum = 200
        self.fileName = "resilience_kc_pause"
        self.connectorName = self.fileName + nameSalt
        self.nameSalt = nameSalt

        for i in range(self.topicNum):
            self.topics.append(self.fileName + str(i) + nameSalt)

        for t in range(self.topicNum):
            self.driver.createTopics(self.topics[t], self.partitionNum, 1)

        sleep(5)

    def getConfigFileName(self):
        return self.fileName + ".json"

    def send(self):
        # pause connector
        self.driver.pauseConnector(self.connectorName)
        sleep(10)

        # send data to connector while paused
        self.__send_data(self)
        sleep(10)

        # start connector
        self.driver.resumeConnector(self.connectorName)

        # send more data
        self.__send_data(self)


    def verify(self, round):
        # verify two sets of data were ingested
        for t in range(self.topicNum):
            res = self.driver.snowflake_conn.cursor().execute(
                "SELECT count(*) FROM {}".format(self.topics[t])).fetchone()[0]

            if res != self.partitionNum * self.recordNum * (round + 1) * 2:
                raise RetryableError()

    def clean(self):
        for t in range(self.topicNum):
            self.driver.cleanTableStagePipe(self.connectorName, self.topics[t], self.partitionNum)

    def __send_data(self):
        for p in range(self.partitionNum):
            for t in range(self.topicNum):
                value = []
                for e in range(self.recordNum):
                    value.append(json.dumps(
                        {'numbernumbernumbernumbernumbernumbernumbernumbernumbernumbernumbernumber': str(e)}
                    ).encode('utf-8'))
                self.driver.sendBytesData(self.topics[t], value, partition=p)