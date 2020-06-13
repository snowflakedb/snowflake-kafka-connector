from test_suit.test_utils import RetryableError, NonRetryableError, ResetAndRetry
from time import sleep
import json

class TestPressure:
    def __init__(self, driver, nameSalt):
        self.driver = driver
        self.topics = []
        self.topicNum = 200
        self.partitionNum = 12
        self.recordNum = 1000
        self.round = 10
        self.sleepTime = 10
        self.curTest = 0
        self.connectorName = "travis_pressure_string_json" + nameSalt
        for i in range(self.topicNum):
            self.topics.append("travis_pressure_string_json" + str(i) + nameSalt)

    def send(self):
        for t in range(self.topicNum):
            self.driver.createTopics(self.topics[t], self.partitionNum, 1)

        for r in range(self.round):
            for p in range(self.partitionNum):
                for t in range(self.topicNum):
                    value = []
                    for e in range(self.recordNum):
                        value.append(json.dumps(
                            {'numbernumbernumbernumbernumbernumbernumbernumbernumbernumbernumbernumber': str(e)}
                        ).encode('utf-8'))
                    self.driver.sendBytesData(self.topics[t], value, partition=p)
            sleep(self.sleepTime)

    def verify(self):
        for t in range(self.curTest, self.topicNum):
            res = self.driver.snowflake_conn.cursor().execute(
                "SELECT count(*) FROM {}".format(self.topics[t])).fetchone()[0]
            if res != self.partitionNum * self.recordNum * self.round:
                raise RetryableError()

            if self.curTest <= t:
                self.curTest = t + 1
                raise ResetAndRetry()

    def clean(self):
        for t in range(self.topicNum):
            self.driver.cleanTableStagePipe(self.connectorName, self.topics[t], self.partitionNum)