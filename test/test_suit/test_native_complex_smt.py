from test_suit.test_utils import RetryableError, NonRetryableError
import json
from test_suit.base_e2e import BaseE2eTest


class TestNativeComplexSmt(BaseE2eTest):
    def __init__(self, driver, nameSalt):
        self.driver = driver
        self.fileName = "travis_correct_native_complex_smt"
        self.topic = self.fileName + nameSalt
        self.table = self.topic

    def getConfigFileName(self):
        return self.fileName + ".json"

    def send(self):
        value = []
        for e in range(100):
            value.append(json.dumps({"c1": {"int": str(e)},
                                     'c2': "Suppose to be dropped."}).encode('utf-8'))
        self.driver.sendBytesData(self.topic, value)

    def verify(self, round):
        res = self.driver.snowflake_conn.cursor().execute(
            "SELECT count(*) FROM {}".format(self.table)).fetchone()[0]
        if res == 0:
            raise RetryableError()
        elif res != 100:
            raise NonRetryableError("Number of record in table is different from number of record sent")

        res = self.driver.snowflake_conn.cursor().execute(
            "Select * from {} limit 1".format(self.table)).fetchone()
        goldMeta = r'{"CreateTime":\d*,"key":{"int":"\d"},"offset":\d,"partition":\d,' \
                   r'"topic":"travis_correct_native_complex_smt_\w*"}'
        goldContent = r'{"c1":{"int":"\d"}}'
        self.driver.regexMatchOneLine(res, goldMeta, goldContent)

        self.driver.verifyStageIsCleaned(self.topic)

    def clean(self):
        self.driver.cleanTableStagePipe(self.topic, self.table)
