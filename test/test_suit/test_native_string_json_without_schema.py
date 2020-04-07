from test_suit.test_utils import RetryableError, NonRetryableError
import json


class TestNativeStringJsonWithoutSchema:
    def __init__(self, driver, nameSalt):
        self.driver = driver
        self.topic = "travis_correct_native_string_json_without_schema" + nameSalt

    def send(self):
        print("\n=== Sending TestNativeStringJsonWithoutSchema json data ===")
        value = []
        for e in range(100):
            value.append(json.dumps({'number': str(e), 'c2': "Suppose to be dropped. If you see this in a table, something is wrong"}).encode('utf-8'))
        self.driver.sendBytesData(self.topic, value)
        print("=== Done ===")

    def verify(self):
        self.driver.verifyWithRetry(self.verifyRetry)
        print("=== TestNativeStringJsonWithoutSchema passed ===")

    def clean(self):
        self.driver.cleanTableStagePipe(self.topic)
        
    def verifyRetry(self):
        res = self.driver.snowflake_conn.cursor().execute(
            "SELECT count(*) FROM {}".format(self.topic)).fetchone()[0]
        if res == 0:
            raise RetryableError()
        elif res != 100:
            raise NonRetryableError()

        
