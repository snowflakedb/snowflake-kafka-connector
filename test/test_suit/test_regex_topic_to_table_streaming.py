from test_suit.test_utils import RetryableError, NonRetryableError
from time import sleep
from confluent_kafka import avro
import json

# Runs only in confluent test suite environment
class TestRegexTopicToTableStreaming:
    def __init__(self, driver, nameSalt):
        self.driver = driver
        self.fileName = "test_regex_topic_to_table_streaming"
        self.recordNum = 1000
        self.topicNum = 3;

        # regex should be: .*_cat -> cattable, doggo.* -> dogtable
        self.catTable = "cattable"
        self.dogTable = "dogtable"

        # topic2table should be calico_cat -> cattable, orange_cat -> cattable, doggo_corgi -> dogtable
        self.catTopicStr1 = "calico_cat"
        self.catTopicStr2 = "orange_cat"
        self.dogTopicStr1 = "doggo_corgi"
        self.topics = []
        self.topics.append(nameSalt + self.catTopicStr1)
        self.topics.append(nameSalt + self.catTopicStr2)
        self.topics.append(self.dogTopicStr1 + nameSalt)

        # create topic and partitions in constructor since the post REST api will automatically create topic with only one partition
        for i in range(self.topicNum):
            self.driver.createTopics(self.topics[i], partitionNum=i, replicationNum=1)

    def getConfigFileName(self):
        return self.fileName + ".json"

    def send(self):
        self.__sendbytes(self.catTopicStr1)
        self.__sendbytes(self.catTopicStr2)
        self.__sendbytes(self.dogTopicStr1)

    def verify(self, round):
        catCount = self.driver.snowflake_conn.cursor().execute(
            "SELECT count(*) FROM {}".format(self.catTable)).fetchone()[0]
        print("Count records in table {}={}".format(self.catTable, str(catCount)))

        if catCount < self.recordNum * 2:
            print("Topic count is less, will retry")
            raise RetryableError()
        elif catCount > self.recordNum * 2:
            print("Topic count is more, duplicates detected")
            raise NonRetryableError("Duplication occurred, number of record in table is larger than number of record sent")
        else:
            print("Table count is exactly " + str(catCount))

        dogCount = self.driver.snowflake_conn.cursor().execute(
            "SELECT count(*) FROM {}".format(self.dogTable)).fetchone()[0]
        print("Count records in table {}={}".format(self.dogTable, str(dogCount)))

        if dogCount < self.recordNum:
            print("Topic count is less, will retry")
            raise RetryableError()
        elif dogCount > self.recordNum:
            print("Topic count is more, duplicates detected")
            raise NonRetryableError("Duplication occurred, number of record in table is larger than number of record sent")
        else:
            print("Table count is exactly " + str(dogCount))

    def clean(self):
        # dropping of stage and pipe doesnt apply for snowpipe streaming. (It executes drop if exists)
        self.driver.cleanTableStagePipe(self.catTable, "")
        self.driver.cleanTableStagePipe(self.dogTable, "")

        for topic in self.topics:
            self.driver.cleanTableStagePipe(topic)
        return

    def __sendbytes(self, topic):
        print("Sending {} records".format(str(self.recordNum)))
        key = []
        value = []
        for e in range(self.recordNum):
            value.append(json.dumps(
                {'numbernumbernumbernumbernumbernumbernumbernumbernumbernumbernumbernumber': str(e)}
            ).encode('utf-8'))
        self.driver.sendBytesData(topic, value, key, 0)
        sleep(2)