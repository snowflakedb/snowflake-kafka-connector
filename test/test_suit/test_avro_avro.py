from test_suit.test_utils import RetryableError, NonRetryableError


class TestAvroAvro:
    def __init__(self, driver, nameSalt):
        self.driver = driver
        self.topic = "travis_correct_avro_avro" + nameSalt

    def send(self):
        print("\n=== Sending TestAvroAvro data ===")
        avroBytes = open("./test_avro_data/twitter.avro", "rb").read()
        key = []
        value = []
        # only append 50 times because the file have two records
        for e in range(50):
            key.append(avroBytes)
            value.append(avroBytes)
        self.driver.sendBytesData(self.topic, value, key)
        print("=== Done ===")

    def verify(self):
        self.driver.verifyWithRetry(self.verifyRetry)
        print("=== TestAvroAvro passed ===")

    def clean(self):
        self.driver.cleanTableStagePipe(self.topic)

    def verifyRetry(self):
        res = self.driver.snowflake_conn.cursor().execute(
            "SELECT count(*) FROM {}".format(self.topic)).fetchone()[0]
        if res == 0:
            raise RetryableError()
        elif res != 100:
            raise NonRetryableError()
