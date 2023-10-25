import datetime

from test_suit.test_utils import RetryableError, NonRetryableError
import json
from time import sleep

class TestSnowpipeStreamingChannelFormatV2:
    def __init__(self, driver, nameSalt):
        self.driver = driver
        self.fileName = "test_snowpipe_streaming_channel_format_v2"
        self.topic = self.fileName + nameSalt

        self.topicNum = 1
        self.partitionNum = 3
        self.recordNum = 1000

        # create topic and partitions in constructor since the post REST api will automatically create topic with only one partition
        self.driver.createTopics(self.topic, partitionNum=self.partitionNum, replicationNum=1)

    def getConfigFileName(self):
        return self.fileName + ".json"

    def send(self):
        # create topic with n partitions and only one replication factor
        print("Partition count:" + str(self.partitionNum))
        print("Topic:", self.topic)

        self.driver.describeTopic(self.topic)

        for p in range(self.partitionNum):
            print("Sending in Partition:" + str(p))
            key = []
            value = []

            for e in range(self.recordNum):
                value.append(json.dumps(
                    {'numbernumbernumbernumbernumbernumbernumbernumbernumbernumbernumbernumber': str(e)}
                ).encode('utf-8'))

            self.driver.sendBytesData(self.topic, value, key, partition=p)
            sleep(2)

    def verify(self, round):
        res = self.driver.snowflake_conn.cursor().execute(
            "SELECT count(*) FROM {}".format(self.topic)).fetchone()[0]
        print("Count records in table {}={}".format(self.topic, str(res)))
        if res < (self.recordNum * self.partitionNum):
            print("Topic:" + self.topic + " count is less, will retry")
            raise RetryableError()
        elif res > (self.recordNum * self.partitionNum):
            print("Topic:" + self.topic + " count is more, duplicates detected")
            raise NonRetryableError("Duplication occurred, number of record in table is larger than number of record sent")
        else:
            print("Table:" + self.topic + " count is exactly " + str(self.recordNum * self.partitionNum))

        # for duplicates
        res = self.driver.snowflake_conn.cursor().execute("Select record_metadata:\"offset\"::string as OFFSET_NO,record_metadata:\"partition\"::string as PARTITION_NO from {} group by OFFSET_NO, PARTITION_NO having count(*)>1".format(self.topic)).fetchone()
        print("Duplicates:{}".format(res))
        if res is not None:
            raise NonRetryableError("Duplication detected")

        # for uniqueness in offset numbers
        rows = self.driver.snowflake_conn.cursor().execute("Select count(distinct record_metadata:\"offset\"::number) as UNIQUE_OFFSETS,record_metadata:\"partition\"::number as PARTITION_NO from {} group by PARTITION_NO order by PARTITION_NO".format(self.topic)).fetchall()

        if rows is None:
            raise NonRetryableError("Unique offsets for partitions not found")
        else:
            assert len(rows) == self.partitionNum

            for p in range(self.partitionNum):
                # unique offset count and partition no are two columns (returns tuple)
                if rows[p][0] != self.recordNum or rows[p][1] != p:
                    raise NonRetryableError("Unique offsets for partitions count doesnt match")

    def clean(self):
        # dropping of stage and pipe doesnt apply for snowpipe streaming. (It executes drop if exists)
        self.driver.cleanTableStagePipe(self.topic)
        return
