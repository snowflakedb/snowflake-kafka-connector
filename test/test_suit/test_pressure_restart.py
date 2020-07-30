from test_suit.test_utils import RetryableError, NonRetryableError, ResetAndRetry
import json

class TestPressureRestart:
    def __init__(self, driver, nameSalt):
        self.driver = driver
        self.topics = []
        self.topicNum = 10
        self.partitionNum = 3
        self.recordNum = 100000
        self.curTest = 0
        self.configIncreamental = 0
        self.fileName = "travis_pressure_restart"
        self.connectorName = self.fileName + nameSalt
        for i in range(self.topicNum):
            self.topics.append(self.fileName + str(i) + nameSalt)

    def getConfigFileName(self):
        return self.fileName + ".json"

    def send(self):
        for t in range(self.topicNum):
            self.driver.createTopics(self.topics[t], self.partitionNum, 1)

        for p in range(self.partitionNum):
            for t in range(self.topicNum):
                value = []
                for e in range(self.recordNum):
                    value.append(json.dumps(
                        {'numbernumbernumbernumbernumbernumbernumbernumbernumbernumbernumbernumber': str(e)}
                    ).encode('utf-8'))
                self.driver.sendBytesData(self.topics[t], value, partition=p)

    def verify(self):
        # restart connector with different config
        self.configIncreamental = self.configIncreamental + 1
        if self.configIncreamental > 0:
            configMap = {"buffer.size.bytes": str(6000000 + self.configIncreamental)}
            self.driver.updateConnectorConfig(self.fileName, self.connectorName, configMap)

        for t in range(self.curTest, self.topicNum):
            res = self.driver.snowflake_conn.cursor().execute(
                "SELECT count(*) FROM {}".format(self.topics[t])).fetchone()[0]

            if res != self.partitionNum * self.recordNum:
                raise RetryableError()

            if self.curTest <= t:
                self.curTest = t + 1
                raise ResetAndRetry()

    def clean(self):
        for t in range(self.topicNum):
            self.driver.cleanTableStagePipe(self.connectorName, self.topics[t], self.partitionNum)