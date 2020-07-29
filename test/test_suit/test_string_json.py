from test_suit.test_utils import RetryableError, NonRetryableError
import json


class TestStringJson:
    def __init__(self, driver, nameSalt):
        self.driver = driver
        self.fileName = "travis_correct_string_json"
        self.topic = self.fileName + nameSalt

    def getConfigFileName(self):
        return self.fileName + ".json"

    def send(self):
        value = []
        for e in range(100):
            value.append(json.dumps({'number': str(e)}).encode('utf-8'))
        self.driver.sendBytesData(self.topic, value)

    def verify(self):
        res = self.driver.snowflake_conn.cursor().execute(
            "SELECT count(*) FROM {}".format(self.topic)).fetchone()[0]
        if res == 0:
            raise RetryableError()
        elif res != 100:
            raise NonRetryableError("Number of record in table is different from number of record sent")

        # validate content of line 1
        res = self.driver.snowflake_conn.cursor().execute(
            "Select * from {} limit 1".format(self.topic)).fetchone()
        goldMeta = r'{"CreateTime":\d*,"offset":0,"partition":0,"topic":"travis_correct_string_json....."}'
        goldContent = r'{"number":"0"}'
        self.driver.regexMatchOneLine(res, goldMeta, goldContent)

    def clean(self):
        self.driver.cleanTableStagePipe(self.topic)