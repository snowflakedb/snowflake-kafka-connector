from test_suit.test_utils import RetryableError, NonRetryableError
import json, os


class TestStringJsonProxy:
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
        res = self.driver.snowflake_conn.cursor().execute(
            "SELECT count(*) FROM {}".format(self.topic)).fetchone()[0]
        if res == 0:
            raise RetryableError()
        elif res != 100:
            raise NonRetryableError("Number of record in table is different from number of record sent")

        # validate content of line 1
        oldVersions = ["5.4.0", "5.3.0", "5.2.0", "2.4.0", "2.3.0", "2.2.0"]
        if self.driver.testVersion in oldVersions:
            goldMeta = r'{"CreateTime":\d*,"headers":{"header1":"value1","header2":{}},"offset":0,"partition":0,"topic":"travis_correct_string_proxy....."}'
        else:
            goldMeta = r'{"CreateTime":\d*,"headers":{"header1":"value1","header2":[]},"offset":0,"partition":0,"topic":"travis_correct_string_proxy....."}'

        res = self.driver.snowflake_conn.cursor().execute(
            "Select * from {} limit 1".format(self.topic)).fetchone()
        goldContent = r'{"number":"0"}'
        self.driver.regexMatchOneLine(res, goldMeta, goldContent)

        self.driver.verifyStageIsCleaned(self.topic)

    def clean(self):
        self.driver.cleanTableStagePipe(self.topic)
        # unset the JVM parameters
        print("Unset JVM Parameters in Clean phase of testing")
        path_parent = os.path.dirname(os.getcwd())
        print("Current Directory:{0}".format(path_parent))
        os.chdir(path_parent)
        print("One directory Up:{0}".format(os.getcwd()))
        mvnExecMain = "mvn exec:java -Dexec.mainClass=\"com.snowflake.kafka.connector.internal.ResetProxyConfigExec\""
        print(self.run_cmd(mvnExecMain))

    def run_cmd(self, command):
        import subprocess
        return subprocess.Popen(command, shell=True, stdout=subprocess.PIPE).stdout.read()