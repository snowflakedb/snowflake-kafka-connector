from test_suit.test_utils import RetryableError, NonRetryableError
import json


class TestNativeComplexSmt:
    def __init__(self, driver, nameSalt):
        self.driver = driver
        self.topic = "travis_correct_native_complex_smt" + nameSalt
        self.table = self.topic

    def send(self):
        value = []
        for e in range(100):
            value.append(json.dumps({"c1": {"int": str(e)},
                                     'c2': "Suppose to be dropped."}).encode('utf-8'))
        self.driver.sendBytesData(self.topic, value)

    def verify(self):
        res = self.driver.snowflake_conn.cursor().execute(
            "SELECT count(*) FROM {}".format(self.table)).fetchone()[0]
        if res == 0:
            raise RetryableError()
        elif res != 100:
            raise NonRetryableError("Number of record in table is different from number of record sent")

        res = self.driver.snowflake_conn.cursor().execute(
            "Select * from {} limit 1".format(self.table)).fetchone()
        goldMeta = r'{"CreateTime":\d*,"key":{"int":"\d"},"offset":\d,"partition":\d,' \
                   r'"topic":"travis_correct_native_complex_smt....."}'
        goldContent = r'{"c1":{"int":"\d"}}'
        self.driver.regexMatchOneLine(res, goldMeta, goldContent)

    def clean(self):
        self.driver.cleanTableStagePipe(self.topic, self.table)
