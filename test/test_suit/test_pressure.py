from test_suit.test_utils import RetryableError, NonRetryableError, ResetAndRetry
from time import sleep
import json

class TestPressure:
    def __init__(self, driver, nameSalt):
        self.driver = driver
        self.topics = []
        self.topicNum = 20
        self.partitionNum = 10
        self.recordNum = 100
        self.curTest = 0
        for i in range(self.topicNum):
            self.topics.append("travis_pressure_string_json" + str(i) + nameSalt)

    def send(self):
        for t in range(self.topicNum):
            self.driver.createTopics(self.topics[t], self.partitionNum, 1)

        for p in range(self.partitionNum):
            for t in range(self.topicNum):
                value = []
                for e in range(self.recordNum):
                    value.append(json.dumps({'number': str(e)}).encode('utf-8'))
                self.driver.sendBytesData(self.topics[t], value, partition=p)

        sleep(120)
        for p in range(self.partitionNum):
            for t in range(self.topicNum):
                value = []
                for e in range(self.recordNum):
                    value.append(json.dumps({'number': str(e)}).encode('utf-8'))
                self.driver.sendBytesData(self.topics[t], value, partition=p)

    def verify(self):
        for t in range(self.curTest, self.topicNum):
            res = self.driver.snowflake_conn.cursor().execute(
                "SELECT count(*) FROM {}".format(self.topics[t])).fetchone()[0]
            if res != self.partitionNum * self.recordNum * 2:
                raise RetryableError()

            if self.curTest <= t:
                self.curTest = t + 1
                raise ResetAndRetry()

    def clean(self):
        for t in range(self.topicNum):
            self.driver.cleanTableStagePipe(self.topics[t], self.partitionNum, True)