from confluent_kafka import Producer
from confluent_kafka.avro import AvroProducer
from confluent_kafka.admin import AdminClient, NewTopic
from time import sleep
from datetime import datetime
from test_suit.test_utils import parsePrivateKey, RetryableError
from multiprocessing.dummy import Pool as ThreadPool

import json
import os
import re
import sys
import snowflake.connector
import test_suit
import requests
import traceback


def errorExit(message):
    print(datetime.now().strftime("%H:%M:%S "), message)
    exit(1)


class KafkaTest:
    def __init__(self, kafkaAddress, schemaRegistryAddress, kafkaConnectAddress, credentialPath, testVersion, enableSSL):
        self.testVersion = testVersion
        self.credentialPath = credentialPath
        with open(self.credentialPath) as f:
            credentialJson = json.load(f)
            testHost = credentialJson["host"]
            testUser = credentialJson["user"]
            testDatabase = credentialJson["database"]
            testSchema = credentialJson["schema"]
            testWarehouse = credentialJson["warehouse"]
            pk = credentialJson["encrypted_private_key"]
            pk_passphrase = credentialJson["private_key_passphrase"]

        self.TEST_DATA_FOLDER = "./test_data/"
        self.httpHeader = {'Content-type': 'application/json', 'Accept': 'application/json'}

        self.SEND_INTERVAL = 0.01  # send a record every 10 ms
        self.VERIFY_INTERVAL = 60  # verify every 60 secs
        self.MAX_RETRY = 120  # max wait time 120 mins
        self.MAX_FLUSH_BUFFER_SIZE = 5000  # flush buffer when 10000 data was in the queue

        self.kafkaConnectAddress = kafkaConnectAddress
        self.schemaRegistryAddress = schemaRegistryAddress
        self.kafkaAddress = kafkaAddress

        if enableSSL:
            print(datetime.now().strftime("\n%H:%M:%S "), "=== Enable SSL ===")
            self.client_config = {
                "bootstrap.servers": kafkaAddress,
                "security.protocol": "SASL_SSL",
                "ssl.ca.location": "./crts/ca-cert",
                "sasl.mechanism": "PLAIN",
                "sasl.username": "client",
                "sasl.password": "client-secret"
            }
        else:
            self.client_config = {
                "bootstrap.servers": kafkaAddress
            }

        self.adminClient = AdminClient(self.client_config)
        self.producer = Producer(self.client_config)
        sc_config = self.client_config
        sc_config['schema.registry.url'] = schemaRegistryAddress
        self.avroProducer = AvroProducer(sc_config)

        reg = "[^\/]*snowflakecomputing"  # find the account name
        account = re.findall(reg, testHost)
        if len(account) != 1 or len(account[0]) < 20:
            print(datetime.now().strftime("%H:%M:%S "),
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

    def startConnectorWaitTime(self):
        sleep(10)

    def verifyWaitTime(self):
        # sleep two minutes before verify result in SF DB
        print(datetime.now().strftime("\n%H:%M:%S "), "=== Sleep {} secs before verify result in Snowflake DB ===".format(
            self.VERIFY_INTERVAL), flush=True)
        sleep(self.VERIFY_INTERVAL)

    def verifyWithRetry(self, func, round):
        retryNum = 0
        while retryNum < self.MAX_RETRY:
            try:
                func(round)
                break
            except test_suit.test_utils.ResetAndRetry:
                retryNum = 0
                print(datetime.now().strftime("%H:%M:%S "), "=== Reset retry count and retry ===", flush=True)
            except test_suit.test_utils.RetryableError as e:
                retryNum += 1
                print(datetime.now().strftime("%H:%M:%S "), "=== Failed, retryable. {}===".format(e.msg), flush=True)
                self.verifyWaitTime()
            except test_suit.test_utils.NonRetryableError as e:
                print(datetime.now().strftime("\n%H:%M:%S "), "=== Non retryable error raised ===\n{}".format(e.msg), flush=True)
                raise test_suit.test_utils.NonRetryableError()
            except snowflake.connector.errors.ProgrammingError as e:
                if e.errno == 2003:
                    retryNum += 1
                    print(datetime.now().strftime("%H:%M:%S "), "=== Failed, table not created ===", flush=True)
                    self.verifyWaitTime()
                else:
                    raise
        if retryNum == self.MAX_RETRY:
            print(datetime.now().strftime("\n%H:%M:%S "), "=== Max retry exceeded ===", flush=True)
            raise test_suit.test_utils.NonRetryableError()

    def createTopics(self, topicName, partitionNum=1, replicationNum=1):
        self.adminClient.create_topics([NewTopic(topicName, partitionNum, replicationNum)])

    def sendBytesData(self, topic, value, key=[], partition=0, headers=[]):
        if len(key) == 0:
            for i, v in enumerate(value):
                self.producer.produce(topic, value=v, partition=partition, headers=headers)
                if (i + 1) % self.MAX_FLUSH_BUFFER_SIZE == 0:
                    self.producer.flush()
        else:
            for i, (k, v) in enumerate(zip(key, value)):
                self.producer.produce(topic, value=v, key=k, partition=partition, headers=headers)
                if (i + 1) % self.MAX_FLUSH_BUFFER_SIZE == 0:
                    self.producer.flush()
        self.producer.flush()

    def sendAvroSRData(self, topic, value, value_schema, key=[], key_schema="", partition=0):
        if len(key) == 0:
            for i, v in enumerate(value):
                self.avroProducer.produce(
                    topic=topic, value=v, value_schema=value_schema, partition=partition)
                if (i + 1) % self.MAX_FLUSH_BUFFER_SIZE == 0:
                    self.producer.flush()
        else:
            for i, (k, v) in enumerate(zip(key, value)):
                self.avroProducer.produce(
                    topic=topic, value=v, value_schema=value_schema, key=k, key_schema=key_schema, partition=partition)
                if (i + 1) % self.MAX_FLUSH_BUFFER_SIZE == 0:
                    self.producer.flush()
        self.avroProducer.flush()

    def cleanTableStagePipe(self, connectorName, topicName="", partitionNumber=1):
        if topicName == "":
            topicName = connectorName
        tableName = topicName
        stageName = "SNOWFLAKE_KAFKA_CONNECTOR_{}_STAGE_{}".format(connectorName, topicName)

        print(datetime.now().strftime("\n%H:%M:%S "), "=== Drop table {} ===".format(tableName))
        self.snowflake_conn.cursor().execute("DROP table IF EXISTS {}".format(tableName))

        print(datetime.now().strftime("%H:%M:%S "), "=== Drop stage {} ===".format(stageName))
        self.snowflake_conn.cursor().execute("DROP stage IF EXISTS {}".format(stageName))

        for p in range(partitionNumber):
            pipeName = "SNOWFLAKE_KAFKA_CONNECTOR_{}_PIPE_{}_{}".format(connectorName, topicName, p)
            print(datetime.now().strftime("%H:%M:%S "), "=== Drop pipe {} ===".format(pipeName))
            self.snowflake_conn.cursor().execute("DROP pipe IF EXISTS {}".format(pipeName))

        print(datetime.now().strftime("%H:%M:%S "), "=== Done ===", flush=True)

    def verifyStageIsCleaned(self, connectorName, topicName=""):
        if topicName == "":
            topicName = connectorName
        stageName = "SNOWFLAKE_KAFKA_CONNECTOR_{}_STAGE_{}".format(connectorName, topicName)

        res = self.snowflake_conn.cursor().execute("list @{}".format(stageName)).fetchone()
        if res is not None:
            raise RetryableError("stage not cleaned up ")

    # validate content match gold regex
    def regexMatchOneLine(self, res, goldMetaRegex, goldContentRegex):
        meta = res[0].replace(" ", "").replace("\n", "")
        content = res[1].replace(" ", "").replace("\n", "")
        goldMetaRegex = "^" + goldMetaRegex.replace("\"", "\\\"").replace("{", "\\{").replace("}", "\\}") \
            .replace("[", "\\[").replace("]", "\\]").replace("+", "\\+") + "$"
        goldContentRegex = "^" + goldContentRegex.replace("\"", "\\\"").replace("{", "\\{").replace("}", "\\}") \
            .replace("[", "\\[").replace("]", "\\]").replace("+", "\\+") + "$"
        if re.search(goldMetaRegex, meta) is None:
            raise test_suit.test_utils.NonRetryableError("Record meta data:\n{}\ndoes not match gold regex "
                                                         "label:\n{}".format(meta, goldMetaRegex))
        if re.search(goldContentRegex, content) is None:
            raise test_suit.test_utils.NonRetryableError("Record content:\n{}\ndoes not match gold regex "
                                                         "label:\n{}".format(content, goldContentRegex))

    def updateConnectorConfig(self, fileName, connectorName, configMap):
        with open('./rest_request_generated/' + fileName + '.json') as f:
            c = json.load(f)
            config = c['config']
            for k in configMap:
                config[k] = configMap[k]
        requestURL = "http://{}/connectors/{}/config".format(self.kafkaConnectAddress, connectorName)
        r = requests.put(requestURL, json=config, headers=self.httpHeader)
        print(datetime.now().strftime("%H:%M:%S "), r, " updated connector config")

    def restartConnector(self, connectorName):
        requestURL = "http://{}/connectors/{}/restart".format(self.kafkaConnectAddress, connectorName)
        r = requests.post(requestURL, headers=self.httpHeader)
        print(datetime.now().strftime("%H:%M:%S "), r, " restart connector")

    def pauseConnector(self, connectorName):
        requestURL = "http://{}/connectors/{}/pause".format(self.kafkaConnectAddress, connectorName)
        r = requests.put(requestURL, headers=self.httpHeader)
        print(datetime.now().strftime("%H:%M:%S "), r, " pause connector")

    def resumeConnector(self, connectorName):
        requestURL = "http://{}/connectors/{}/resume".format(self.kafkaConnectAddress, connectorName)
        r = requests.put(requestURL, headers=self.httpHeader)
        print(datetime.now().strftime("%H:%M:%S "), r, " resume connector")

    def deleteConnector(self, connectorName):
        requestURL = "http://{}/connectors/{}".format(self.kafkaConnectAddress, connectorName)
        r = requests.delete(requestURL, headers=self.httpHeader)
        print(datetime.now().strftime("%H:%M:%S "), r, " delete connector")

    def closeConnector(self, fileName, nameSalt):
        snowflake_connector_name = fileName.split(".")[0] + nameSalt
        delete_url = "http://{}/connectors/{}".format(self.kafkaConnectAddress, snowflake_connector_name)
        print(datetime.now().strftime("\n%H:%M:%S "), "=== Delete connector {} ===".format(snowflake_connector_name))
        code = requests.delete(delete_url, timeout=10).status_code
        print(datetime.now().strftime("%H:%M:%S "), code)

    def createConnector(self, fileName, nameSalt):
        rest_template_path = "./rest_request_template"
        rest_generate_path = "./rest_request_generated"

        with open(self.credentialPath) as f:
            credentialJson = json.load(f)
            testHost = credentialJson["host"]
            testUser = credentialJson["user"]
            testDatabase = credentialJson["database"]
            testSchema = credentialJson["schema"]
            pk = credentialJson["private_key"]

        print(datetime.now().strftime("\n%H:%M:%S "), "=== generate sink connector rest reqeuest from {} ===".format(rest_template_path))
        if not os.path.exists(rest_generate_path):
            os.makedirs(rest_generate_path)
        snowflake_connector_name = fileName.split(".")[0] + nameSalt

        print(datetime.now().strftime("\n%H:%M:%S "), "=== Connector Config JSON: {}, Connector Name: {} ===".format(fileName, snowflake_connector_name))
        with open("{}/{}".format(rest_template_path, fileName), 'r') as f:
            config = f.read() \
                .replace("SNOWFLAKE_PRIVATE_KEY", pk) \
                .replace("SNOWFLAKE_HOST", testHost) \
                .replace("SNOWFLAKE_USER", testUser) \
                .replace("SNOWFLAKE_DATABASE", testDatabase) \
                .replace("SNOWFLAKE_SCHEMA", testSchema) \
                .replace("CONFLUENT_SCHEMA_REGISTRY", self.schemaRegistryAddress) \
                .replace("SNOWFLAKE_TEST_TOPIC", snowflake_connector_name) \
                .replace("SNOWFLAKE_CONNECTOR_NAME", snowflake_connector_name)
            with open("{}/{}".format(rest_generate_path, fileName), 'w') as fw:
                fw.write(config)

        MAX_RETRY = 20
        retry = 0
        delete_url = "http://{}/connectors/{}".format(self.kafkaConnectAddress, snowflake_connector_name)
        post_url = "http://{}/connectors".format(self.kafkaConnectAddress)
        while retry < MAX_RETRY:
            try:
                code = requests.delete(delete_url, timeout=10).status_code
                if code == 404 or code == 200 or code == 201:
                    break
            except:
                pass
            print(datetime.now().strftime("\n%H:%M:%S "), "=== sleep for 30 secs to wait for kafka connect to accept connection ===")
            sleep(30)
            retry += 1
        if retry == MAX_RETRY:
            errorExit("\n=== max retry exceeded, kafka connect not ready in 10 mins ===")

        r = requests.post(post_url, json=json.loads(config), headers=self.httpHeader)
        print(datetime.now().strftime("%H:%M:%S "), json.loads(r.content.decode("utf-8"))["name"], r.status_code)


def runTestSet(driver, testSet, nameSalt, pressure):
    from test_suit.test_string_json import TestStringJson
    from test_suit.test_json_json import TestJsonJson
    from test_suit.test_string_avro import TestStringAvro
    from test_suit.test_avro_avro import TestAvroAvro
    from test_suit.test_string_avrosr import TestStringAvrosr
    from test_suit.test_avrosr_avrosr import TestAvrosrAvrosr

    from test_suit.test_native_string_avrosr import TestNativeStringAvrosr
    from test_suit.test_native_string_json_without_schema import TestNativeStringJsonWithoutSchema
    from test_suit.test_native_complex_smt import TestNativeComplexSmt
    from test_suit.test_pressure import TestPressure
    from test_suit.test_pressure_restart import TestPressureRestart

    from test_suit.test_native_string_protobuf import TestNativeStringProtobuf
    from test_suit.test_confluent_protobuf_protobuf import TestConfluentProtobufProtobuf

    testStringJson = TestStringJson(driver, nameSalt)
    testJsonJson = TestJsonJson(driver, nameSalt)
    testStringAvro = TestStringAvro(driver, nameSalt)
    testAvroAvro = TestAvroAvro(driver, nameSalt)
    testStringAvrosr = TestStringAvrosr(driver, nameSalt)
    testAvrosrAvrosr = TestAvrosrAvrosr(driver, nameSalt)

    testNativeStringAvrosr = TestNativeStringAvrosr(driver, nameSalt)
    testNativeStringJsonWithoutSchema = TestNativeStringJsonWithoutSchema(driver, nameSalt)
    testNativeComplexSmt = TestNativeComplexSmt(driver, nameSalt)
    testPressure = TestPressure(driver, nameSalt)
    testPressureRestart = TestPressureRestart(driver, nameSalt)

    testNativeStringProtobuf = TestNativeStringProtobuf(driver, nameSalt)
    testConfluentProtobufProtobuf = TestConfluentProtobufProtobuf(driver, nameSalt)

    ############################ round 1 ############################
    print(datetime.now().strftime("\n%H:%M:%S "), "=== Round 1 ===")
    testSuitList1 = [testStringJson, testJsonJson, testStringAvro, testAvroAvro, testStringAvrosr,
                     testAvrosrAvrosr, testNativeStringAvrosr, testNativeStringJsonWithoutSchema,
                     testNativeComplexSmt, testNativeStringProtobuf, testConfluentProtobufProtobuf]

    testCleanEnableList1 = [True, True, True, True, True, True, True, True, True, True, True]
    testSuitEnableList1 = []
    if testSet == "confluent":
        testSuitEnableList1 = [True, True, True, True, True, True, True, True, True, True, False]
    elif testSet == "apache":
        testSuitEnableList1 = [True, True, True, True, False, False, False, True, True, True, False]
    elif testSet != "clean":
        errorExit("Unknown testSet option {}, please input confluent, apache or clean".format(testSet))

    execution(testSet, testSuitList1, testCleanEnableList1, testSuitEnableList1, driver, nameSalt)
    ############################ round 1 ############################

    ############################ round 2 ############################
    print(datetime.now().strftime("\n%H:%M:%S "), "=== Round 2 ===")
    testSuitList2 = [testPressureRestart]

    testCleanEnableList2 = [True]
    testSuitEnableList2 = []
    if testSet == "confluent":
        testSuitEnableList2 = [True]
    elif testSet == "apache":
        testSuitEnableList2 = [True]
    elif testSet != "clean":
        errorExit("Unknown testSet option {}, please input confluent, apache or clean".format(testSet))

    execution(testSet, testSuitList2, testCleanEnableList2, testSuitEnableList2, driver, nameSalt, 2)
    ############################ round 2 ############################

    ############################ round 3 ############################
    print(datetime.now().strftime("\n%H:%M:%S "), "=== Round 3 ===")
    testSuitList3 = [testPressure]

    testCleanEnableList3 = [pressure]
    testSuitEnableList3 = []
    if testSet == "confluent":
        testSuitEnableList3 = [pressure]
    elif testSet == "apache":
        testSuitEnableList3 = [pressure]
    elif testSet != "clean":
        errorExit("Unknown testSet option {}, please input confluent, apache or clean".format(testSet))

    execution(testSet, testSuitList3, testCleanEnableList3, testSuitEnableList3, driver, nameSalt, 4)
    ############################ round 3 ############################


def execution(testSet, testSuitList, testCleanEnableList, testSuitEnableList, driver, nameSalt, round = 1):
    if testSet == "clean":
        for i, test in enumerate(testSuitList):
            if testCleanEnableList[i]:
                test.clean()
        print(datetime.now().strftime("\n%H:%M:%S "), "=== All clean done ===")
    else:
        try:
            for i, test in enumerate(testSuitList):
                if testSuitEnableList[i]:
                    driver.createConnector(test.getConfigFileName(), nameSalt)

            driver.startConnectorWaitTime()

            for r in range(round):
                print(datetime.now().strftime("\n%H:%M:%S "), "=== round {} ===".format(r))
                for i, test in enumerate(testSuitList):
                    if testSuitEnableList[i]:
                        print(datetime.now().strftime("\n%H:%M:%S "), "=== Sending " + test.__class__.__name__ + " data ===")
                        test.send()
                        print(datetime.now().strftime("%H:%M:%S "), "=== Done " + test.__class__.__name__ + " ===", flush=True)

                driver.verifyWaitTime()

                for i, test in enumerate(testSuitList):
                    if testSuitEnableList[i]:
                        print(datetime.now().strftime("\n%H:%M:%S "), "=== Verify " + test.__class__.__name__ + " ===")
                        driver.verifyWithRetry(test.verify, r)
                        print(datetime.now().strftime("%H:%M:%S "), "=== Passed " + test.__class__.__name__ + " ===", flush=True)

            print(datetime.now().strftime("\n%H:%M:%S "), "=== All test passed ===")
        except Exception as e:
            print(datetime.now().strftime("%H:%M:%S "), e)
            traceback.print_tb(e.__traceback__)
            print(datetime.now().strftime("%H:%M:%S "), "Error: ", sys.exc_info()[0])
            exit(1)


if __name__ == "__main__":
    if len(sys.argv) != 9:
        errorExit(
            """\n=== Usage: ./ingest.py <kafka address> <schema registry address> <kafka connect address>
             <test set> <test version> <name salt> <pressure> <enableSSL>===""")

    kafkaAddress = sys.argv[1]
    schemaRegistryAddress = sys.argv[2]
    kafkaConnectAddress = sys.argv[3]
    testSet = sys.argv[4]
    testVersion = sys.argv[5]
    nameSalt = sys.argv[6]
    pressure = (sys.argv[7] == 'true')
    enableSSL = (sys.argv[8] == 'true')

    if "SNOWFLAKE_CREDENTIAL_FILE" not in os.environ:
        errorExit(
            "\n=== Require environment variable SNOWFLAKE_CREDENTIAL_FILE but it's not set.  Aborting. ===")

    credentialPath = os.environ['SNOWFLAKE_CREDENTIAL_FILE']

    if not os.path.isfile(credentialPath):
        errorExit("\n=== Provided SNOWFLAKE_CREDENTIAL_FILE {} does not exist.  Aborting. ===".format(
            credentialPath))

    kafkaTest = KafkaTest(kafkaAddress, schemaRegistryAddress, kafkaConnectAddress, credentialPath, testVersion, enableSSL)

    runTestSet(kafkaTest, testSet, nameSalt, pressure)
