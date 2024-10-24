from test_suit.test_utils import RetryableError, NonRetryableError
from time import sleep
from confluent_kafka import avro
import json
from test_suit.base_e2e import BaseE2eTest

# Runs only in confluent test suite environment
class TestMultipleTopicToOneTableSnowpipe(BaseE2eTest):
    def __init__(self, driver, nameSalt):
        self.driver = driver
        self.fileName = "travis_correct_multiple_topic_to_one_table_snowpipe"
        self.topics = []
        self.topicNum = 3
        self.partitionNum = 3
        self.recordNum = 1000
        self.tableName = self.fileName + nameSalt

        for i in range(self.topicNum):
            self.topics.append(self.fileName + nameSalt + str(i))

            # create topic and partitions in constructor since the post REST api will automatically create topic with only one partition
            self.driver.createTopics(self.topics[-1], partitionNum=self.partitionNum, replicationNum=1)
        

    def getConfigFileName(self):
        return self.fileName + ".json"

    def send(self):
        # create topic with n partitions and only one replication factor
        print("Partition count:" + str(self.partitionNum))
        print("Topics:")
        for topic in self.topics:
            print(topic)

        for topic in self.topics:    
            self.driver.describeTopic(topic)

            for p in range(self.partitionNum):
                print("Sending in Partition:" + str(p))
                key = []
                value = []
                for e in range(self.recordNum):
                    value.append(json.dumps(
                        {'numbernumbernumbernumbernumbernumbernumbernumbernumbernumbernumbernumber': str(e)}
                    ).encode('utf-8'))
                self.driver.sendBytesData(topic, value, key, partition=p)
                sleep(2)

    def verify(self, round):
        res = self.driver.snowflake_conn.cursor().execute(
            "SELECT count(*) FROM {}".format(self.tableName)).fetchone()[0]
        print("Count records in table {}={}".format(self.tableName, str(res)))
        if res < (self.recordNum * self.partitionNum * self.topicNum):
            print("Topic count is less, will retry")
            raise RetryableError()
        elif res > (self.recordNum * self.partitionNum * self.topicNum):
            print("Topic count is more, duplicates detected")
            raise NonRetryableError("Duplication occurred, number of record in table is larger than number of record sent")
        else:
            print("Table count is exactly " + str(self.recordNum * self.partitionNum * self.topicNum))

        # for duplicates
        res = self.driver.snowflake_conn.cursor().execute("Select record_metadata:\"offset\"::string as OFFSET_NO,record_metadata:\"partition\"::string as PARTITION_NO from {} group by OFFSET_NO, PARTITION_NO having count(*)>{}".format(self.tableName, str(self.topicNum))).fetchone()
        print("Duplicates:{}".format(res))
        if res is not None:
            raise NonRetryableError("Over Duplication detected")

        # for uniqueness in offset numbers
        rows = self.driver.snowflake_conn.cursor().execute("Select count(distinct record_metadata:\"offset\"::number) as UNIQUE_OFFSETS,record_metadata:\"partition\"::number as PARTITION_NO from {} group by PARTITION_NO order by PARTITION_NO".format(self.tableName)).fetchall()

        if rows is None:
            raise NonRetryableError("Unique offsets for partitions not found")
        else:
            assert len(rows) == 3

            for p in range(self.partitionNum):
                # unique offset count and partition no are two columns (returns tuple)
                if rows[p][0] != self.recordNum or rows[p][1] != p:
                    raise NonRetryableError("Unique offsets for partitions count doesnt match")

        rows = self.driver.snowflake_conn.cursor().execute("Select count(distinct record_metadata:\"topic\"::string) as TOPIC_NO, record_metadata:\"partition\"::number as PARTITION_NO from {} group by PARTITION_NO order by PARTITION_NO".format(self.tableName)).fetchall()

        if rows is None:
            raise NonRetryableError("Topics for partitions not found")
        else:
            assert len(rows) == 3

            for p in range(self.partitionNum):
                # unique offset count and partition no are two columns (returns tuple)
                if rows[p][0] != self.topicNum or rows[p][1] != p:
                    raise NonRetryableError("Topics and partitions count doesnt match")

    def clean(self):
        # dropping of stage and pipe doesnt apply for snowpipe streaming. (It executes drop if exists)
        self.driver.cleanTableStagePipe(self.tableName, "", self.partitionNum)
        # for topic in self.topics:
        #     self.driver.cleanTableStagePipe(topic)
        return