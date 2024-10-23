from test_suit.test_utils import RetryableError, NonRetryableError, ResetAndRetry
from time import sleep
from multiprocessing.dummy import Pool as ThreadPool
import json
from test_suit.base_e2e import BaseE2eTest

class TestPressure(BaseE2eTest):
    def __init__(self, driver, nameSalt):
        self.driver = driver
        self.topics = []
        self.topicNum = 200
        self.partitionNum = 12
        self.recordNum = 10000
        self.curTest = 0
        self.threadCount = 10
        self.fileName = "travis_pressure_string_json"
        self.connectorName = self.fileName + nameSalt
        for i in range(self.topicNum):
            self.topics.append(self.fileName + str(i) + nameSalt)

        for t in range(self.topicNum):
            self.driver.createTopics(self.topics[t], self.partitionNum, 1)

        sleep(5)

    def getConfigFileName(self):
        return self.fileName + ".json"

    def send(self):
        threadPool = ThreadPool(self.threadCount)

        args = []
        for t in range(self.topicNum):
            for p in range(self.partitionNum):
                args.append((t, p))
        threadPool.starmap(self.sendHelper, args)
        threadPool.close()
        threadPool.join()

    def sendHelper(self, t, p):
        value = []
        for e in range(self.recordNum):
            value.append(json.dumps(
                {'numbernumbernumbernumbernumbernumbernumbernumbernumbernumbernumbernumber': str(e)}
            ).encode('utf-8'))
        self.driver.sendBytesData(self.topics[t], value, [], p)
        # self.threadPool.starmap(self.driver.sendBytesData, [(self.topics[t], value, [], p)])

    def verify(self, round):
        for t in range(self.curTest, self.topicNum):
            res = self.driver.snowflake_conn.cursor().execute(
                "SELECT count(*) FROM {}".format(self.topics[t])).fetchone()[0]
            if res != self.partitionNum * self.recordNum * (round + 1):
                print("Round", round, "Result", res, "topicNum", t)
                raise RetryableError()

            if self.curTest <= t:
                self.curTest = t + 1
                raise ResetAndRetry()
            
        # after success, reset curTest for next round
        self.curTest = 0

    def clean(self):
        threadPool = ThreadPool(self.threadCount)
        args = []
        for t in range(self.topicNum):
            args.append((self.connectorName, self.topics[t], self.partitionNum))
        threadPool.starmap(self.driver.cleanTableStagePipe, args)
        threadPool.close()
        threadPool.join()