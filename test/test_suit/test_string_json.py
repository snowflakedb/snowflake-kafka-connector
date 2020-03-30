from test_suit.test_utils import RetryableError, NonRetryableError
import json


class TestStringJson:
    def __init__(self, driver, nameSalt):
        self.driver = driver
        self.topic = "travis_correct_string_json" + nameSalt

    def send(self):
        print("\n=== Sending TestStringJson json data ===")
        value = []
        for e in range(100):
            value.append(json.dumps({'number': str(e)}).encode('utf-8'))
        self.driver.sendBytesData(self.topic, value)
        print("=== Done ===")

    def verify(self):
        self.driver.verifyWithRetry(self.verifyRetry)
        print("=== TestStringJson passed ===")

    def clean(self):
        self.driver.cleanTableStagePipe(self.topic)
        
    def verifyRetry(self):
        res = self.driver.snowflake_conn.cursor().execute(
            "SELECT count(*) FROM {}".format(self.topic)).fetchone()[0]
        if res == 0:
            raise RetryableError()
        elif res != 100:
            raise NonRetryableError()

        
