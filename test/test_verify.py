from avro.schema import Parse
from confluent_kafka import Producer
from confluent_kafka.avro import AvroProducer
from time import sleep
from test_suit.test_utils import parsePrivateKey
import json
import io
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
        self.MAX_RETRY = 10        # max wait time 10 mins

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
            except test_suit.test_utils.RetryableError:
                retryNum += 1
                print("=== Failed, retryable ===", flush=True)
                self.verifyWaitTime()
            except test_suit.test_utils.NonRetryableError:
                print("\n=== Non retryable error raised ===")
                raise test_suit.test_utils.NonRetryableError()
            except snowflake.connector.errors.ProgrammingError as e:
                if e.errno == 2003:
                    retryNum += 1
                    print("=== Failed, table not created ===", flush=True)
                    self.verifyWaitTime()
                else:
                    raise
        if retryNum == self.MAX_RETRY:
            print("\n=== Max retry exceeded ===")
            raise test_suit.test_utils.NonRetryableError()

    def sendBytesData(self, topic, value, key=[]):
        if len(key) == 0:
            for v in value:
                self.producer.produce(topic, value=v)
                self.msgSendInterval()
        else:
            for k, v in zip(key, value):
                self.producer.produce(topic, value=v, key=k)
                self.msgSendInterval()
        self.producer.flush()

    def sendAvroSRData(self, topic, value, value_schema, key=[], key_schema=""):
        if len(key) == 0:
            for v in value:
                self.avroProducer.produce(
                    topic=topic, value=v, value_schema=value_schema)
                self.msgSendInterval()
        else:
            for k, v in zip(key, value):
                self.avroProducer.produce(
                    topic=topic, value=v, value_schema=value_schema, key=k, key_schema=key_schema)
                self.msgSendInterval()
        self.avroProducer.flush()

    def cleanTableStagePipe(self, topicName):
        topicName = topicName.upper()
        tableName = topicName
        stageName = "SNOWFLAKE_KAFKA_CONNECTOR_{}_STAGE_{}".format(topicName, topicName)
        pipeName = "SNOWFLAKE_KAFKA_CONNECTOR_{}_PIPE_{}_0".format(topicName, topicName)

        print("\n=== Drop table {} ===".format(tableName))
        self.snowflake_conn.cursor().execute("DROP table IF EXISTS {}".format(tableName))

        print("=== Drop stage {} ===".format(stageName))
        self.snowflake_conn.cursor().execute("DROP stage IF EXISTS {}".format(stageName))

        print("=== Drop pipe {} ===".format(pipeName))
        self.snowflake_conn.cursor().execute("DROP pipe IF EXISTS {}".format(pipeName))

        print("=== Done ===")


def runTestSet(driver, testSet, nameSalt):
    from test_suit.test_string_json import TestStringJson
    from test_suit.test_json_json import TestJsonJson
    from test_suit.test_string_avro import TestStringAvro
    from test_suit.test_avro_avro import TestAvroAvro
    from test_suit.test_string_avrosr import TestStringAvrosr
    from test_suit.test_avrosr_avrosr import TestAvrosrAvrosr

    from test_suit.test_native_string_avrosr import TestNativeStringAvrosr
    from test_suit.test_native_string_json_without_schema import TestNativeStringJsonWithoutSchema

    testStringJson = TestStringJson(driver, nameSalt)
    testJsonJson = TestJsonJson(driver, nameSalt)
    testStringAvro = TestStringAvro(driver, nameSalt)
    testAvroAvro = TestAvroAvro(driver, nameSalt)
    testStringAvrosr = TestStringAvrosr(driver, nameSalt)
    testAvrosrAvrosr = TestAvrosrAvrosr(driver, nameSalt)

    testNativeStringAvrosr = TestNativeStringAvrosr(driver, nameSalt)
    testNativeStringJsonWithoutSchema = TestNativeStringJsonWithoutSchema(driver, nameSalt)

    testSuitList = [testStringJson, testJsonJson, testStringAvro, testAvroAvro, testStringAvrosr, 
                    testAvrosrAvrosr, testNativeStringAvrosr, testNativeStringJsonWithoutSchema]
    if testSet == "confluent":
        testSuitEnableList = [True, True, True, True, True, True, True, True]
    elif testSet == "apache":
        testSuitEnableList = [True, True, True, True, False, False, False, True]
    elif testSet == "clean":
        testSuitEnableList = [False, False, False, False, False, False, False, False]
    else:
        errorExit(
            "Unknown testSet option {}, please input confluent, apache or clean".format(testSet))

    failedFlag = False
    try:
        for i, test in enumerate(testSuitList):
            if testSuitEnableList[i]:
                test.send()
            
        if testSet != "clean":
            driver.verifyWaitTime()

        for i, test in enumerate(testSuitList):
            if testSuitEnableList[i]:
                test.verify()
    except Exception as e:
        print(e)
        print("Error: ", sys.exc_info()[0])
        failedFlag = True
    finally:
        for i, test in enumerate(testSuitList):
            test.clean()

    if failedFlag:
        exit(1)

    if testSet == "clean":
        print("\n=== All clean done ===")
    else:
        print("\n=== All test passed ===")


if __name__ == "__main__":
    if len(sys.argv) != 5:
        errorExit(
            """\n=== Usage: ./ingest.py <kafka address> <schema registry address> <test set> <name salt> ===""")

    kafkaAddress = sys.argv[1]
    schemaRegistryAddress = sys.argv[2]
    testSet = sys.argv[3]
    nameSalt = sys.argv[4]

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

    runTestSet(kafkaTest, testSet, nameSalt)
