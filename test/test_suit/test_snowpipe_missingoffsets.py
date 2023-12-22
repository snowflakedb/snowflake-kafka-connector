from test_suit.test_utils import RetryableError, NonRetryableError
import json
import random

class TestSnowpipeMissingOffsets:
    def __init__(self, driver, nameSalt):
        self.driver = driver
        self.fileName = "test_snowpipe_missingoffsets"
        self.topic = self.fileName + nameSalt
        self.recordCount = 100 # 100 total records sent
        self.lastRecordOffset = 120 # 20 records missing

    def getConfigFileName(self):
        return self.fileName + ".json"

    def send(self):
        value = []

        # first 50 are consecutive (0-49)
        for e in range(50):
            value.append(json.dumps({'number': str(e)}).encode('utf-8'))

        # add two tombstone records (50, 51)
        # append tombstone except for 2.5.1 due to this bug: https://issues.apache.org/jira/browse/KAFKA-10477
        if self.driver.testVersion == '2.5.1':
            value.append(json.dumps(
                {'numbernumbernumbernumbernumbernumbernumbernumbernumbernumbernumbernumber': str(50)}
            ).encode('utf-8'))
        else:
            value.append(None)
        value.append("") # custom sf converters treat this as a normal record

        # generate random missing offsets
        missingOffsets = random.sample(range(52, self.lastRecordOffset), 20)

        # next 48 are not consecutive (52-99)
        for e in range(52, self.lastRecordOffset):
            if e not in missingOffsets:
                value.append(json.dumps({'number': str(e)}).encode('utf-8'))

        header = [('header1', 'value1'), ('header2', '{}')]
        self.driver.sendBytesData(self.topic, value, [], 0, header)

    def verify(self, round):
        res = self.driver.snowflake_conn.cursor().execute(
            "SELECT count(*) FROM {}".format(self.topic)).fetchone()[0]

        if res == 0:
            raise RetryableError()
        elif res != self.recordCount:
            raise NonRetryableError("Number of record in table is different from number of record sent")

        # validate content of line 1
        oldVersions = ["5.4.0", "5.3.0", "5.2.0", "2.4.0", "2.3.0", "2.2.0"]
        if self.driver.testVersion in oldVersions:
            goldMeta = r'{"CreateTime":\d*,"headers":{"header1":"value1","header2":{}},"offset":0,"partition":0,"topic":"test_snowpipe_missingoffsets....."}'
        else:
            goldMeta = r'{"CreateTime":\d*,"headers":{"header1":"value1","header2":[]},"offset":0,"partition":0,"topic":"test_snowpipe_missingoffsets....."}'

        res = self.driver.snowflake_conn.cursor().execute(
            "Select * from {} limit 1".format(self.topic)).fetchone()
        goldContent = r'{"number":"0"}'
        self.driver.regexMatchOneLine(res, goldMeta, goldContent)

        self.driver.verifyStageIsCleaned(self.topic)

    def clean(self):
        self.driver.cleanTableStagePipe(self.topic)