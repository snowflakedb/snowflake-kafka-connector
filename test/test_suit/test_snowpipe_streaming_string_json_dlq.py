import datetime

from test_suit.test_utils import RetryableError, NonRetryableError
import json
from time import sleep
from test_suit.base_e2e import BaseE2eTest

'''
Testing this doesnt require a custom DLQ api to be invoked since this is happening at connect level. 
i.e the bad message being to Kafka is not being serialized at Converter level. 
'''
class TestSnowpipeStreamingStringJsonDLQ(BaseE2eTest):
    def __init__(self, driver, nameSalt):
        self.driver = driver
        self.fileName = "snowpipe_streaming_string_json_dlq"
        self.topic = self.fileName + nameSalt

        self.topicNum = 1
        self.partitionNum = 1
        self.recordNum = 5
        self.expected_record_count_in_table = 0
        self.expected_record_count_in_dlq = 5

        # create topic and partitions in constructor since the post REST api will automatically create topic with only one partition
        self.driver.createTopics(self.topic, partitionNum=self.partitionNum, replicationNum=1)

    def getConfigFileName(self):
        return self.fileName + ".json"

    def send(self):
        # create topic with n partitions and only one replication factor
        print("Partition count:" + str(self.partitionNum))
        print("Topic:", self.topic)

        for p in range(self.partitionNum):
            print("Sending Incorrect Data in Partition:" + str(p))
            key = []
            value = []
            incorrect_data = "{invalid_string\"}"
            for e in range(self.recordNum):
                value.append(incorrect_data.encode('utf-8'))
            self.driver.sendBytesData(self.topic, value, key, partition=p)
            sleep(2)

    def verify(self, round):
        res = self.driver.select_number_of_records(self.topic)
        print("Count records in table {}={}".format(self.topic, str(res)))
        if res > 0:
            print("Topic:" + self.topic + " count is more than expected, will not retry")
            raise NonRetryableError("Data should not have been ingested in table")
        elif res == self.expected_record_count_in_table:
            print("Table:" + self.topic + " count is exactly " + str(self.expected_record_count_in_table))

        # Last offset number in dlq is one less than count of messages sent
        offsets_in_dlq = self.driver.consume_messages_dlq(self.fileName, self.partitionNum - 1, self.expected_record_count_in_dlq - 1)

        if offsets_in_dlq == self.expected_record_count_in_dlq:
            print("Offsets in DLQ:{}".format(str(offsets_in_dlq)))
        else:
            raise NonRetryableError("Offsets count found in DLQ:{}".format(offsets_in_dlq))

    def clean(self):
        # dropping of stage and pipe doesnt apply for snowpipe streaming. (It executes drop if exists)
        self.driver.cleanTableStagePipe(self.topic)
        return
