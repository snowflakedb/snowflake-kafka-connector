from test_suit.test_utils import RetryableError, NonRetryableError
import json
from test_suit.base_e2e import BaseE2eTest


class TestStringJson(BaseE2eTest):
    def __init__(self, driver, nameSalt):
        self.driver = driver
        self.fileName = "travis_correct_string_json"
        self.topic = self.fileName + nameSalt
        self.recordCount = 100

    def getConfigFileName(self):
        return self.fileName + ".json"

    def send(self):
        value = []

        # send two less record because we are sending tombstone records. tombstone ingestion is enabled by default
        for e in range(self.recordCount - 2):
            value.append(json.dumps({'number': str(e)}).encode('utf-8'))

        # append tombstone except for 2.5.1 due to this bug: https://issues.apache.org/jira/browse/KAFKA-10477
        if self.driver.testVersion == '2.5.1':
            value.append(json.dumps(
                {'numbernumbernumbernumbernumbernumbernumbernumbernumbernumbernumbernumber': str(self.recordCount)}
            ).encode('utf-8'))
        else:
            value.append(None)
        value.append("") # custom sf converters treat this as a normal record

        header = [('header1', 'value1'), ('header2', '{}')]
        self.driver.sendBytesData(self.topic, value, [], 0, header)

    def verify(self, round):
        res = self.driver.select_number_of_records(self.topic)

        if res == 0:
            raise RetryableError()
        elif res != self.recordCount:
            raise NonRetryableError("Number of record in table is different from number of record sent")

        # validate content of line 1
        oldVersions = ["5.4.0", "5.3.0", "5.2.0", "2.4.0", "2.3.0", "2.2.0"]
        if self.driver.testVersion in oldVersions:
            goldMeta = r'{"CreateTime":\d*,"headers":{"header1":"value1","header2":{}},"offset":0,"partition":0,"topic":"travis_correct_string_json_\w*"}'
        else:
            goldMeta = r'{"CreateTime":\d*,"headers":{"header1":"value1","header2":[]},"offset":0,"partition":0,"topic":"travis_correct_string_json_\w*"}'

        res = self.driver.snowflake_conn.cursor().execute(
            "Select * from {} limit 1".format(self.topic)).fetchone()
        goldContent = r'{"number":"0"}'
        self.driver.regexMatchOneLine(res, goldMeta, goldContent)

        self.driver.verifyStageIsCleaned(self.topic)

    def clean(self):
        self.driver.cleanTableStagePipe(self.topic)