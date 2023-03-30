import datetime

from test_suit.test_utils import RetryableError, NonRetryableError
import json
from time import sleep

class TestAtLeastOnceSemantic:
    def __init__(self, driver, nameSalt):
        self.driver = driver
        self.fileName = "at_least_once_semantic"
        self.topic = self.fileName + nameSalt

    def getConfigFileName(self):
        return self.fileName + ".json"

    def send(self):
        for i in range(20):
            key = []
            value = []
            for e in range(50):
                key.append(json.dumps({'number': str(e)}).encode('utf-8'))
                value.append(json.dumps({'number': str(e)}).encode('utf-8'))
            self.driver.sendBytesData(self.topic, value, key)
            sleep(2)

    def verify(self, round):
        res = self.driver.snowflake_conn.cursor().execute(
            "SELECT count(*) FROM {}".format(self.topic)).fetchone()[0]
        if res < 1000:
            raise RetryableError()
        elif res >= 1000:
            res = self.driver.snowflake_conn.cursor().execute("Select record_metadata:\"offset\"::string as OFFSET_NO,record_metadata:\"partition\"::string as PARTITION_NO from {} group by OFFSET_NO, PARTITION_NO having count(*)>1".format(self.topic)).fetchone()
            print("Result is:")
            print(res)
            if res is None:
                raise NonRetryableError("Duplication should be detected with at least once semantic")

        self.driver.verifyStageIsCleaned(self.topic)

    def clean(self):
        self.driver.cleanTableStagePipe(self.topic)
