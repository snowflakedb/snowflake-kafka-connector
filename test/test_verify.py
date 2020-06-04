from confluent_kafka import Producer
from confluent_kafka.avro import AvroProducer
from confluent_kafka.admin import AdminClient, NewTopic
from time import sleep
from test_suit.test_utils import parsePrivateKey
import json
import os
import re
import sys
import snowflake.connector
import test_suit


def errorExit(message):
    print(message)
    exit(1)


class KafkaTest:
    def __init__(self, kafkaAddress, schemaRegistryAddress,
                 testHost, testUser, testDatabase, testSchema, testWarehouse, pk, pk_passphrase):

        self.SEND_INTERVAL = 0.01  # send a record every 10 ms
        self.VERIFY_INTERVAL = 60  # verify every 60 secs
        self.MAX_RETRY = 20  # max wait time 20 mins

        self.adminClient = AdminClient({"bootstrap.servers": kafkaAddress})
        self.producer = Producer({'bootstrap.servers': kafkaAddress})
        self.avroProducer = AvroProducer({'bootstrap.servers': kafkaAddress,
                                          'schema.registry.url': schemaRegistryAddress})

        reg = "[^\/]*snowflakecomputing"  # find the account name
        account = re.findall(reg, testHost)
        if len(account) != 1 or len(account[0]) < 20:
            print(
                "Format error in 'host' field at profile.json, expecting account.snowflakecomputing.com:443")

        pkb = parsePrivateKey(pk, pk_passphrase)
        self.snowflake_conn = snowflake.connector.connect(
            user=testUser,
            private_key=pkb,
            account=account[0][:-19],
            warehouse=testWarehouse,
            database=testDatabase,
            schema=testSchema
        )

    def msgSendInterval(self):
        # sleep self.SEND_INTERVAL before send the second message
        sleep(self.SEND_INTERVAL)

    def verifyWaitTime(self):
        # sleep two minutes before verify result in SF DB
        print("\n=== Sleep {} secs before verify result in Snowflake DB ===".format(
            self.VERIFY_INTERVAL), flush=True)
        sleep(self.VERIFY_INTERVAL)

    def verifyWithRetry(self, func):
        retryNum = 0
        while retryNum < self.MAX_RETRY:
            try:
                func()
                break
            except test_suit.test_utils.ResetAndRetry:
                retryNum = 0
                print("=== Reset retry count and retry ===", flush=True)
            except test_suit.test_utils.RetryableError:
                retryNum += 1
                print("=== Failed, retryable ===", flush=True)
                self.verifyWaitTime()
            except test_suit.test_utils.NonRetryableError as e:
                print("\n=== Non retryable error raised ===\n{}".format(e.msg), flush=True)
                raise test_suit.test_utils.NonRetryableError()
            except snowflake.connector.errors.ProgrammingError as e:
                if e.errno == 2003:
                    retryNum += 1
                    print("=== Failed, table not created ===", flush=True)
                    self.verifyWaitTime()
                else:
                    raise
        if retryNum == self.MAX_RETRY:
            print("\n=== Max retry exceeded ===", flush=True)
            raise test_suit.test_utils.NonRetryableError()

    def createTopics(self, topicName, partitionNum=1, replicationNum=1):
        self.adminClient.create_topics([NewTopic(topicName, partitionNum, replicationNum)])

    def sendBytesData(self, topic, value, key=[], partition=0):
        if len(key) == 0:
            for v in value:
                self.producer.produce(topic, value=v, partition=partition)
                # self.msgSendInterval()
        else:
            for k, v in zip(key, value):
                self.producer.produce(topic, value=v, key=k, partition=partition)
                # self.msgSendInterval()
        self.producer.flush()

    def sendAvroSRData(self, topic, value, value_schema, key=[], key_schema="", partition=0):
        if len(key) == 0:
            for v in value:
                self.avroProducer.produce(
                    topic=topic, value=v, value_schema=value_schema, partition=partition)
                # self.msgSendInterval()
        else:
            for k, v in zip(key, value):
                self.avroProducer.produce(
                    topic=topic, value=v, value_schema=value_schema, key=k, key_schema=key_schema, partition=partition)
                # self.msgSendInterval()
        self.avroProducer.flush()

    def cleanTableStagePipe(self, topicName, partitionNumber=1, stripConnectorName=False):
        topicName = topicName.upper()
        if stripConnectorName:
            topicNameList = topicName.split("_")
            topicNameList[-2] = ''.join(i for i in topicNameList[-2] if not i.isdigit())
            stripedTopicName = '_'.join(topicNameList)
            connectorName = stripedTopicName
        else:
            connectorName = topicName
        tableName = topicName
        stageName = "SNOWFLAKE_KAFKA_CONNECTOR_{}_STAGE_{}".format(connectorName, topicName)

        print("\n=== Drop table {} ===".format(tableName))
        self.snowflake_conn.cursor().execute("DROP table IF EXISTS {}".format(tableName))

        print("=== Drop stage {} ===".format(stageName))
        self.snowflake_conn.cursor().execute("DROP stage IF EXISTS {}".format(stageName))

        for p in range(partitionNumber):
            pipeName = "SNOWFLAKE_KAFKA_CONNECTOR_{}_PIPE_{}_{}".format(connectorName, topicName, p)
            print("=== Drop pipe {} ===".format(pipeName))
            self.snowflake_conn.cursor().execute("DROP pipe IF EXISTS {}".format(pipeName))

        print("=== Done ===", flush=True)

    # validate content match gold regex
    def regexMatchOneLine(self, res, goldMetaRegex, goldContentRegex):
        meta = res[0].replace(" ", "").replace("\n", "")
        content = res[1].replace(" ", "").replace("\n", "")
        goldMetaRegex = "^" + goldMetaRegex.replace("\"", "\\\"").replace("{", "\\{").replace("}", "\\}") \
            .replace("[", "\\[").replace("]", "\\]") + "$"
        goldContentRegex = "^" + goldContentRegex.replace("\"", "\\\"").replace("{", "\\{").replace("}", "\\}") \
            .replace("[", "\\[").replace("]", "\\]") + "$"
        if re.search(goldMetaRegex, meta) is None:
            raise test_suit.test_utils.NonRetryableError("Record meta data:\n{}\ndoes not match gold regex "
                                                         "label:\n{}".format(meta, goldMetaRegex))
        if re.search(goldContentRegex, content) is None:
            raise test_suit.test_utils.NonRetryableError("Record content:\n{}\ndoes not match gold regex "
                                                         "label:\n{}".format(content, goldContentRegex))


def runTestSet(driver, testSet, nameSalt, pressure):
    from test_suit.test_string_json import TestStringJson
    from test_suit.test_json_json import TestJsonJson
    from test_suit.test_string_avro import TestStringAvro
    from test_suit.test_avro_avro import TestAvroAvro
    from test_suit.test_string_avrosr import TestStringAvrosr
    from test_suit.test_avrosr_avrosr import TestAvrosrAvrosr

    from test_suit.test_native_string_avrosr import TestNativeStringAvrosr
    from test_suit.test_native_string_json_without_schema import TestNativeStringJsonWithoutSchema
    from test_suit.test_pressure import TestPressure

    testStringJson = TestStringJson(driver, nameSalt)
    testJsonJson = TestJsonJson(driver, nameSalt)
    testStringAvro = TestStringAvro(driver, nameSalt)
    testAvroAvro = TestAvroAvro(driver, nameSalt)
    testStringAvrosr = TestStringAvrosr(driver, nameSalt)
    testAvrosrAvrosr = TestAvrosrAvrosr(driver, nameSalt)

    testNativeStringAvrosr = TestNativeStringAvrosr(driver, nameSalt)
    testNativeStringJsonWithoutSchema = TestNativeStringJsonWithoutSchema(driver, nameSalt)
    testPressure = TestPressure(driver, nameSalt)

    testSuitList = [testStringJson, testJsonJson, testStringAvro, testAvroAvro, testStringAvrosr,
                    testAvrosrAvrosr, testNativeStringAvrosr, testNativeStringJsonWithoutSchema, testPressure]
    if testSet == "confluent":
        testSuitEnableList = [True, True, True, True, True, True, True, True, pressure]
    elif testSet == "apache":
        testSuitEnableList = [True, True, True, True, False, False, False, True, pressure]
    elif testSet == "clean":
        testSuitEnableList = [False, False, False, False, False, False, False, False, False]
    else:
        errorExit(
            "Unknown testSet option {}, please input confluent, apache or clean".format(testSet))

    testCleanEnableList = [True, True, True, True, True, True, True, True, pressure]

    failedFlag = False
    try:
        for i, test in enumerate(testSuitList):
            if testSuitEnableList[i]:
                print("\n=== Sending " + test.__class__.__name__ + " data ===")
                test.send()
                print("=== Done ===", flush=True)

        if testSet != "clean":
            driver.verifyWaitTime()

        for i, test in enumerate(testSuitList):
            if testSuitEnableList[i]:
                print("\n=== Verify " + test.__class__.__name__ + " ===")
                driver.verifyWithRetry(test.verify)
                print("=== Passed ===", flush=True)
    except Exception as e:
        print(e)
        print("Error: ", sys.exc_info()[0])
        failedFlag = True
    finally:
        for i, test in enumerate(testSuitList):
            if testCleanEnableList[i]:
                test.clean()

    if failedFlag:
        exit(1)

    if testSet == "clean":
        print("\n=== All clean done ===")
    else:
        print("\n=== All test passed ===")


if __name__ == "__main__":
    if len(sys.argv) != 6:
        errorExit(
            """\n=== Usage: ./ingest.py <kafka address> <schema registry address> <test set> <name salt> 
            <pressure>===""")

    kafkaAddress = sys.argv[1]
    schemaRegistryAddress = sys.argv[2]
    testSet = sys.argv[3]
    nameSalt = sys.argv[4]
    pressure = (sys.argv[5] == 'true')

    if "SNOWFLAKE_CREDENTIAL_FILE" not in os.environ:
        errorExit(
            "\n=== Require environment variable SNOWFLAKE_CREDENTIAL_FILE but it's not set.  Aborting. ===")

    credentialPath = os.environ['SNOWFLAKE_CREDENTIAL_FILE']

    if not os.path.isfile(credentialPath):
        errorExit("\n=== Provided SNOWFLAKE_CREDENTIAL_FILE {} does not exist.  Aborting. ===".format(
            credentialPath))

    with open(credentialPath) as f:
        credentialJson = json.load(f)

        testHost = credentialJson["host"]
        testUser = credentialJson["user"]
        testDatabase = credentialJson["database"]
        testSchema = credentialJson["schema"]
        testWarehouse = credentialJson["warehouse"]
        pk = credentialJson["encrypted_private_key"]
        pk_passphrase = credentialJson["private_key_passphrase"]

    kafkaTest = KafkaTest(kafkaAddress, schemaRegistryAddress,
                          testHost, testUser, testDatabase, testSchema, testWarehouse, pk, pk_passphrase)

    runTestSet(kafkaTest, testSet, nameSalt, pressure)
