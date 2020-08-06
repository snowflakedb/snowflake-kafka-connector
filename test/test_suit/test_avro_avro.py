from test_suit.test_utils import RetryableError, NonRetryableError


class TestAvroAvro:
    def __init__(self, driver, nameSalt):
        self.driver = driver
        self.fileName = "travis_correct_avro_avro"
        self.topic = self.fileName + nameSalt

    def getConfigFileName(self):
        return self.fileName + ".json"

    def send(self):
        avroBytes = open(self.driver.TEST_DATA_FOLDER + "twitter.avro", "rb").read()
        key = []
        value = []
        # only append 50 times because the file have two records
        for e in range(50):
            key.append(avroBytes)
            value.append(avroBytes)
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
        goldMeta = r'{"CreateTime":\d*,"key":[{"timestamp":\d*,"tweet":"Rock:Nerfpaper,scissorsisfine.",' \
                   r'"username":"miguno"},{"timestamp":\d*,"tweet":"Worksasintended.TerranisIMBA.",' \
                   r'"username":"BlizzardCS"}],"offset":0,"partition":0,"topic":"travis_correct_avro_avro....."}'
        goldContent = r'{"timestamp":\d*,"tweet":"Rock:Nerfpaper,scissorsisfine.","username":"miguno"}'
        self.driver.regexMatchOneLine(res, goldMeta, goldContent)

    def clean(self):
        self.driver.cleanTableStagePipe(self.topic)