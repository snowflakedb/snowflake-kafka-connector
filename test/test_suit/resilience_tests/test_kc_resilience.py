import datetime

from test_suit.test_utils import RetryableError, NonRetryableError, ResetAndRetry
import json
from time import sleep

class TestKcResilience:
    def __init__(self, driver, nameSalt):
        self.driver = driver
        self.nameSalt = nameSalt
        self.fileName = "test_kc_resilience"
        self.topic = self.fileName + nameSalt
        self.connectorName = self.fileName + nameSalt
        self.testIt = 0
        self.totalIt = 7
        self.sleepTime = 10

        self.topicNum = 1
        self.partitionNum = 1
        self.recordNum = 1000

        # create topic and partitions in constructor since the post REST api will automatically create topic with only one partition
        self.driver.createTopics(self.topic, self.partitionNum, 1)

    def getConfigFileName(self):
        return self.fileName + ".json"

    def send(self):
        print("Resilience tests for Kafka Connector. Note: this test uses verify() as a runner to test how different methods interact, so most of the logic is within the verify block.\n"
              "Each iteration upserts a new connector, sends data, tests out the methods, sends data again and finally verifies that the expected data arrived in the table\n"
              "iteration 1: restart connector\n"
              "iteration 2: pause then resume connector\n"
              "iteration 3: delete then create connector\n"
              "iteration 4: delete then resume connector - second round of data should be missing\n"
              "iteration 5: pause then create connector\n"
              "iteration 6: recreate connector\n")

    def verify(self, round):
        if self.testIt < self.totalIt:
            self.testIt = self.testIt + 1

            print("[RES TEST ITERATION] {}: {}/{}".format(self.fileName, str(self.testIt), str(self.totalIt)))
            self.driver.createConnector(self.getConfigFileName(), self.nameSalt)
            print("Waiting {} seconds for connector to start".format(str(self.sleepTime)))
            sleep(self.sleepTime)

            prevcount = self.driver.snowflake_conn.cursor().execute(
                "SELECT count(*) FROM {}".format(self.topic)).fetchone()[0]
            print("Current record count: {}".format(str(prevcount)))

            self.__sendbytes()
            expectedsends = 1

            # test methods
            if self.testIt % self.totalIt == 1:
                print("Test restarting connector")
                self.driver.restartConnector(self.connectorName)
            elif self.testIt % self.totalIt == 2:
                print("Test pause connector, wait " + str(self.sleepTime) + " seconds, then resume connector")
                self.driver.pauseConnector(self.connectorName);
                sleep(self.sleepTime)
                self.driver.resumeConnector(self.connectorName);
            elif self.testIt % self.totalIt == 3:
                print("Test delete connector, wait " + str(self.sleepTime) + " seconds then create connector")
                self.driver.deleteConnector(self.connectorName)
                sleep(self.sleepTime)
                self.driver.createConnector(self.getConfigFileName(), self.nameSalt)
            elif self.testIt % self.totalIt == 4:
                print("Test delete connector, wait " + str(self.sleepTime) + " seconds, then resume connector")
                self.driver.deleteConnector(self.connectorName)
                sleep(self.sleepTime)
                self.driver.resumeConnector(self.connectorName);
                expectedsends = expectedsends - 1 # resume shouldn't create the connector, so miss the sendbytes
            elif self.testIt % self.totalIt == 5:
                print("Test pause connector, wait " + str(self.sleepTime) + " seconds, then create connector")
                self.driver.pauseConnector(self.connectorName);
                sleep(self.sleepTime)
                self.driver.createConnector(self.getConfigFileName(), self.nameSalt)
            elif self.testIt % self.totalIt == 6:
                print("Test recreate connector")
                self.driver.createConnector(self.getConfigFileName(), self.nameSalt)

            self.__sendbytes();
            expectedsends = expectedsends + 1

            print("Waiting " + str(self.sleepTime) + " seconds for last method to complete")
            sleep(self.sleepTime)

            # verify record count
            goalCount = prevcount + (self.recordNum * expectedsends)
            res = self.driver.snowflake_conn.cursor().execute(
                "SELECT count(*) FROM {}".format(self.topic)).fetchone()[0]
            print("Count records in table {}={}. Goal record count: {}".format(self.topic, str(res), str(goalCount)))
            if res < goalCount:
                print("Less records than expected, will retry")
                raise RetryableError()
            elif res > goalCount:
                print("Topic:" + self.topic + " count is more, duplicates detected")
                raise NonRetryableError("Duplication occurred, number of record in table is larger than number of record sent")
            else:
                print("Success - expected number of records found, continuing to next test iteration")

            raise ResetAndRetry()

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
        sleep(2)