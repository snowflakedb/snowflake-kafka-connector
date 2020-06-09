from test_suit.test_utils import RetryableError, NonRetryableError
import json


class TestNativeStringJsonWithoutSchema:
    def __init__(self, driver, nameSalt):
        self.driver = driver
        self.topic = "travis_correct_native_string_json_without_schema" + nameSalt

    def send(self):
        value = []
        for e in range(100):
            value.append(json.dumps({'number': str(e),
                                     'c2': "Suppose to be dropped."}).encode('utf-8'))
        self.driver.sendBytesData(self.topic, value)

    def verify(self):
        res = self.driver.snowflake_conn.cursor().execute(
            "SELECT count(*) FROM {}".format(self.topic)).fetchone()[0]
        if res == 0:
            raise RetryableError()
        elif res != 100:
            raise NonRetryableError("Number of record in table is different from number of record sent")

        # validate content of line 1, this way we ensure SMT is working
        res = self.driver.snowflake_conn.cursor().execute(
            "Select * from {} limit 1".format(self.topic)).fetchone()
        goldMeta = r'{"CreateTime":\d*,"offset":0,"partition":0,' \
                   r'"topic":"travis_correct_native_string_json_without_schema....."}'
        goldContent = r'{"number":"0"}'
        self.driver.regexMatchOneLine(res, goldMeta, goldContent)

    def clean(self):
        self.driver.cleanTableStagePipe(self.topic)
