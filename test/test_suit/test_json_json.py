from test_suit.test_utils import RetryableError, NonRetryableError
import json


class TestJsonJson:
    def __init__(self, driver, nameSalt):
        self.driver = driver
        self.topic = "travis_correct_json_json" + nameSalt

    def send(self):
        key = []
        value = []
        for e in range(100):
            key.append(json.dumps({'number': str(e)}).encode('utf-8'))
            value.append(json.dumps({'number': str(e)}).encode('utf-8'))
        self.driver.sendBytesData(self.topic, value, key)

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
        goldMeta = r'{"key":[{"number":"0"}],"offset":0,"partition":0}'
        goldContent = r'{"number":"0"}'
        self.driver.regexMatchOneLine(res, goldMeta, goldContent)

    def clean(self):
        self.driver.cleanTableStagePipe(self.topic)
