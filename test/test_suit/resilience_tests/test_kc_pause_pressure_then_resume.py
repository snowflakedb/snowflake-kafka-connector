from test_suit.test_utils import RetryableError, NonRetryableError
import json
from time import sleep

class TestKcPausePressureThenResume:
    def __init__(self, driver, nameSalt):
        self.driver = driver
        self.fileName = "test_kc_pause_pressure_then_resume"
        self.topic = self.fileName + nameSalt
        self.connectorName = self.fileName + nameSalt

        self.topicNum = 1
        self.partitionNum = 3
        self.recordNum = 1000

        # create topic and partitions in constructor since the post REST api will automatically create topic with only one partition
        self.driver.createTopics(self.topic, self.partitionNum, 1)

    def getConfigFileName(self):
        return self.fileName + ".json"

    def send(self):
        print("Partition count:" + str(self.partitionNum))
        print("Topic:", self.topic)
        self.driver.describeTopic(self.topic)

        self.driver.pauseConnector(self.connectorName);
        print("Wait for connector to pause - 10 secs")
        sleep(10)

        self.__sendbytes();

        self.driver.resumeConnector(self.connectorName);
        print("Wait for connector to start - 10 secs")
        sleep(10)

        self.__sendbytes();

    def verify(self, round):
        goalCount = self.recordNum * self.partitionNum * 2
        res = self.driver.snowflake_conn.cursor().execute(
            "SELECT count(*) FROM {}".format(self.topic)).fetchone()[0]
        print("Count records in table {}={}".format(self.topic, str(res)))
        if res < goalCount:
            print("Topic:" + self.topic + " count is less, will retry")
            raise RetryableError()
        elif res > goalCount:
            print("Topic:" + self.topic + " count is more, duplicates detected")
            raise NonRetryableError("Duplication occurred, number of record in table is larger than number of record sent")
        else:
            print("Table:" + self.topic + " count is exactly " + str(self.recordNum * self.partitionNum))

    def clean(self):
        # dropping of stage and pipe doesnt apply for snowpipe streaming. (It executes drop if exists)
        self.driver.cleanTableStagePipe(self.topic)
        return

    def __sendbytes(self):
        for p in range(self.partitionNum):
            print("Sending data to Partition:" + str(p))
            key = []
            value = []
            for e in range(self.recordNum):
                value.append(json.dumps(
                    {'numbernumbernumbernumbernumbernumbernumbernumbernumbernumbernumbernumber': str(e)}
                ).encode('utf-8'))
            self.driver.sendBytesData(self.topic, value, key, p)
            sleep(2)