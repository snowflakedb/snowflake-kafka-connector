from test_suit.test_utils import RetryableError, NonRetryableError
import json


class TestJsonJson:
    def __init__(self, driver, nameSalt):
        self.driver = driver
        self.topic = "travis_correct_json_json" + nameSalt

    def send(self):
        print("\n=== Sending TestJsonJson json data ===")
        key = []
        value = []
        for e in range(100):
            key.append(json.dumps({'number': str(e)}).encode('utf-8'))
            value.append(json.dumps({'number': str(e)}).encode('utf-8'))
        self.driver.sendBytesData(self.topic, value, key)
        print("=== Done ===")

    def verify(self):
        self.driver.verifyWithRetry(self.verifyRetry)
        print("=== TestJsonJson passed ===")

    def clean(self):
        self.driver.cleanTableStagePipe(self.topic)
        
    def verifyRetry(self):
        res = self.driver.snowflake_conn.cursor().execute(
            "SELECT count(*) FROM {}".format(self.topic)).fetchone()[0]
        if res == 0:
            raise RetryableError()
        elif res != 100:
            raise NonRetryableError()
