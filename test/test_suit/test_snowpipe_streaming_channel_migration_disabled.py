import datetime

from test_suit.test_utils import RetryableError, NonRetryableError
import json
from time import sleep
from test_suit.base_e2e import BaseE2eTest


"""
Only config added here is about migrating channel offsets from channel created in version 2.1.0 to any future versions.
This test verifies if the functionality can be disabled
"""
class TestSnowpipeStreamingStringJsonChannelMigrationDisabled(BaseE2eTest):
    def __init__(self, driver, nameSalt):
        self.driver = driver
        self.fileName = "test_snowpipe_streaming_channel_migration_disabled"
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

            # send two less record because we are sending tombstone records. tombstone ingestion is enabled by default
            for e in range(self.recordNum - 2):
                value.append(json.dumps(
                    {'numbernumbernumbernumbernumbernumbernumbernumbernumbernumbernumbernumber': str(e)}
                ).encode('utf-8'))

            # append tombstone except for 2.5.1 due to this bug: https://issues.apache.org/jira/browse/KAFKA-10477
            if self.driver.testVersion == '2.5.1':
                value.append(json.dumps(
                    {'numbernumbernumbernumbernumbernumbernumbernumbernumbernumbernumbernumber': str(self.recordNum - 1)}
                ).encode('utf-8'))
                value.append(json.dumps(
                    {'numbernumbernumbernumbernumbernumbernumbernumbernumbernumbernumbernumber': str(self.recordNum)}
                ).encode('utf-8'))
            else:
                value.append(None)
                value.append("") # community converters treat this as a tombstone

            self.driver.sendBytesData(self.topic, value, key, partition=p)
            sleep(2)

    def verify(self, round):
        res = self.driver.select_number_of_records(self.topic)
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
            assert len(rows) == 3

            for p in range(self.partitionNum):
                # unique offset count and partition no are two columns (returns tuple)
                if rows[p][0] != self.recordNum or rows[p][1] != p:
                    raise NonRetryableError("Unique offsets for partitions count doesnt match")

    def clean(self):
        # dropping of stage and pipe doesnt apply for snowpipe streaming. (It executes drop if exists)
        self.driver.cleanTableStagePipe(self.topic)
        return
