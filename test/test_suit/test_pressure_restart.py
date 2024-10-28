from test_suit.test_utils import RetryableError, NonRetryableError, ResetAndRetry
import json
from time import sleep
from test_suit.base_e2e import BaseE2eTest

class TestPressureRestart(BaseE2eTest):
    def __init__(self, driver, nameSalt):
        self.driver = driver
        self.topics = []
        self.topicNum = 10
        self.partitionNum = 3
        self.recordNum = 200000
        self.curTest = 0
        self.configIncreamental = 0
        self.fileName = "travis_pressure_restart"
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
        for p in range(self.partitionNum):
            for t in range(self.topicNum):
                value = []
                for e in range(self.recordNum):
                    value.append(json.dumps(
                        {'numbernumbernumbernumbernumbernumbernumbernumbernumbernumbernumbernumber': str(e)}
                    ).encode('utf-8'))
                self.driver.sendBytesData(self.topics[t], value, partition=p)

    def verify(self, round):
        # restart connector with different config
        self.configIncreamental = self.configIncreamental + 1
        if self.configIncreamental % 7 == 2:
            configMap = {"buffer.size.bytes": str(6000000 + self.configIncreamental)}
            self.driver.updateConnectorConfig(self.fileName, self.connectorName, configMap)
        elif self.configIncreamental % 7 == 3:
            self.driver.restartConnector(self.connectorName)
        elif self.configIncreamental % 7 == 4:
            self.driver.pauseConnector(self.connectorName)
        elif self.configIncreamental % 7 == 5:
            self.driver.resumeConnector(self.connectorName)
        elif self.configIncreamental % 7 == 6:
            self.driver.deleteConnector(self.connectorName)
        elif self.configIncreamental % 7 == 0:
            self.driver.createConnector(self.getConfigFileName(), self.nameSalt)

        for t in range(self.curTest, self.topicNum):
            res = self.driver.snowflake_conn.cursor().execute(
                "SELECT count(*) FROM {}".format(self.topics[t])).fetchone()[0]

            if res != self.partitionNum * self.recordNum * (round + 1):
                raise RetryableError()

            if self.curTest <= t:
                self.curTest = t + 1
                raise ResetAndRetry()

        # after success, reset curTest for next round
        self.curTest = 0

    def clean(self):
        for t in range(self.topicNum):
            self.driver.cleanTableStagePipe(self.connectorName, self.topics[t], self.partitionNum)