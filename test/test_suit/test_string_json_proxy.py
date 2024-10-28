from test_suit.test_utils import RetryableError, NonRetryableError
import json, os
from test_suit.base_e2e import BaseE2eTest


class TestStringJsonProxy(BaseE2eTest):
    def __init__(self, driver, nameSalt):
        self.driver = driver
        self.fileName = "travis_correct_string_proxy"
        self.topic = self.fileName + nameSalt

    def getConfigFileName(self):
        return self.fileName + ".json"

    def send(self):
        value = []
        for e in range(100):
            value.append(json.dumps({'number': str(e)}).encode('utf-8'))
        header = [('header1', 'value1'), ('header2', '{}')]
        self.driver.sendBytesData(self.topic, value, [], 0, header)

    def verify(self, round):
        res = self.driver.select_number_of_records(self.topic)
        if res == 0:
            raise RetryableError()
        elif res != 100:
            raise NonRetryableError("Number of record in table is different from number of record sent")

        # validate content of line 1
        oldVersions = ["5.4.0", "5.3.0", "5.2.0", "2.4.0", "2.3.0", "2.2.0"]
        if self.driver.testVersion in oldVersions:
            goldMeta = r'{"CreateTime":\d*,"headers":{"header1":"value1","header2":{}},"offset":0,"partition":0,"topic":"travis_correct_string_proxy_\w*"}'
        else:
            goldMeta = r'{"CreateTime":\d*,"headers":{"header1":"value1","header2":[]},"offset":0,"partition":0,"topic":"travis_correct_string_proxy_\w*"}'

        res = self.driver.snowflake_conn.cursor().execute(
            "Select * from {} limit 1".format(self.topic)).fetchone()
        goldContent = r'{"number":"0"}'
        self.driver.regexMatchOneLine(res, goldMeta, goldContent)

        self.driver.verifyStageIsCleaned(self.topic)

    def clean(self):
        self.driver.cleanTableStagePipe(self.topic)
        # unset the JVM parameters
        print("Unset JVM Parameters in Clean phase of testing")

        current_dir = os.getcwd()
        print(f"Current Directory:{current_dir}")

        path_parent = os.path.dirname(current_dir)

        print(f"Changing working directory to:{path_parent}")
        os.chdir(path_parent)

        mvnExecMain = "mvn exec:java -Dexec.mainClass=\"com.snowflake.kafka.connector.internal.ResetProxyConfigExec\""
        print(self.run_cmd(mvnExecMain))

        print(f"Changing working directory back to:{current_dir}")
        os.chdir(current_dir)

    def run_cmd(self, command):
        import subprocess
        return subprocess.Popen(command, shell=True, stdout=subprocess.PIPE).stdout.read()