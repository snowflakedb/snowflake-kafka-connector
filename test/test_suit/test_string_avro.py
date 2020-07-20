from test_suit.test_utils import RetryableError, NonRetryableError


class TestStringAvro:
    def __init__(self, driver, nameSalt):
        self.driver = driver
        self.topic = "travis_correct_string_avro" + nameSalt

    def send(self):
        avroBytes = open(self.driver.TEST_DATA_FOLDER + "twitter.avro", "rb").read()
        value = []
        # only append 50 times because the file have two records
        for e in range(50):
            value.append(avroBytes)
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
        goldMeta = r'{"CreateTime":\d*,"offset":0,"partition":0,' \
                   r'"topic":"travis_correct_string_avro....."}'
        goldContent = r'{"timestamp":\d*,"tweet":"Rock:Nerfpaper,scissorsisfine.","username":"miguno"}'
        self.driver.regexMatchOneLine(res, goldMeta, goldContent)

    def clean(self):
        self.driver.cleanTableStagePipe(self.topic)