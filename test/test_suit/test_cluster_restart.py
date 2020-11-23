from test_suit.test_utils import RetryableError, NonRetryableError, ResetAndRetry
import json
import random
import string
from multiprocessing.dummy import Pool as ThreadPool

class TestClusterRestart:
    def __init__(self, driver, nameSalt):
        self.driver = driver
        self.topics = []
        self.topicNum = 10
        self.partitionNum = 3
        self.recordNum = 200000
        self.curTest = 0
        self.configIncreamental = 0
        self.fileName = "travis_cluster_restart"
        self.connectorName = self.fileName + nameSalt
        self.nameSalt = nameSalt
        self.threadCount = 10
        for i in range(self.topicNum):
            self.topics.append(self.fileName + str(i) + nameSalt)

    def getConfigFileName(self):
        return self.fileName + ".json"

    def send(self):
        self.driver.deleteK8sLogFolder()
        for t in range(self.topicNum):
            self.driver.createTopics(self.topics[t], self.partitionNum, 1)

        args = []
        threadPool = ThreadPool(self.threadCount)
        for t in range(self.topicNum):
            for p in range(self.partitionNum):
                args.append((t, p))
        threadPool.starmap(self.sendHelper, args)
        threadPool.close()
        threadPool.join()

    def sendHelper(self, t, p):
        value = []
        for e in range(self.recordNum):
            randomString = ''.join(random.choices(string.ascii_uppercase + string.digits, k=300))
            value.append(json.dumps({randomString: str(e)}).encode('utf-8'))
        self.driver.sendBytesData(self.topics[t], value, partition=p)

    def verify(self, round):
        # restart connector with different config
        self.configIncreamental = self.configIncreamental + 1
        if self.configIncreamental % 10 == 9:
            self.driver.dumpK8sLog(self.configIncreamental, round)
            self.driver.restartKafkaConnectPod()

        for t in range(self.curTest, self.topicNum):
            res = self.driver.snowflake_conn.cursor().execute(
                "SELECT count(*) FROM {}".format(self.topics[t])).fetchone()[0]

            if res < self.partitionNum * self.recordNum * (round + 1):
                raise RetryableError(" Record count in table: " + str(res) +
                                     " Expecting: " + str(self.partitionNum * self.recordNum * (round + 1)))

            if res > self.partitionNum * self.recordNum * (round + 1):
                raise NonRetryableError(" Record count in table: " + str(res) +
                                        " Expecting: " + str(self.partitionNum * self.recordNum * (round + 1)))

            if self.curTest <= t:
                self.curTest = t + 1
                raise ResetAndRetry()

        # after success, reset curTest for next round
        self.curTest = 0

    def clean(self):
        for t in range(self.topicNum):
            self.driver.cleanTableStagePipe(self.connectorName, self.topics[t], self.partitionNum)