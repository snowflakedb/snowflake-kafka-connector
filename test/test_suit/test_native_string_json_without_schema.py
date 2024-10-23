from test_suit.test_utils import RetryableError, NonRetryableError
import json
from test_suit.base_e2e import BaseE2eTest

class TestNativeStringJsonWithoutSchema(BaseE2eTest):
    def __init__(self, driver, nameSalt):
        self.driver = driver
        self.fileName = "travis_correct_native_string_json_without_schema"
        self.topic = self.fileName + nameSalt

    def getConfigFileName(self):
        return self.fileName + ".json"

    def send(self):
        value = []
        for e in range(100):
            value.append(json.dumps({'number': str(e),
                                     'c2': "Suppose to be dropped."}).encode('utf-8'))
        self.driver.sendBytesData(self.topic, value)

    def verify(self, round):
        res = self.driver.select_number_of_records(self.topic)
        if res == 0:
            raise RetryableError()
        elif res != 100:
            raise NonRetryableError("Number of record in table is different from number of record sent")

        # validate content of line 1, this way we ensure SMT is working
        res = self.driver.snowflake_conn.cursor().execute(
            "Select * from {} limit 1".format(self.topic)).fetchone()
        goldMeta = r'{"CreateTime":\d*,"offset":0,"partition":0,' \
                   r'"topic":"travis_correct_native_string_json_without_schema_\w*"}'
        goldContent = r'{"number":"0"}'
        self.driver.regexMatchOneLine(res, goldMeta, goldContent)

        self.driver.verifyStageIsCleaned(self.topic)

    def clean(self):
        self.driver.cleanTableStagePipe(self.topic)
