import json
import os
import re
import sys
import traceback
from datetime import datetime
from time import sleep

import requests, uuid
import snowflake.connector
from confluent_kafka import Producer, Consumer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic, ConfigResource, NewPartitions
from confluent_kafka.avro import AvroProducer
from test_suites import create_end_to_end_test_suites
import time

import test_suit
from test_suit.test_utils import parsePrivateKey, RetryableError


def errorExit(message):
    print(datetime.now().strftime("%H:%M:%S "), message)
    exit(1)


class KafkaTest:
    def __init__(self, kafkaAddress, schemaRegistryAddress, kafkaConnectAddress, credentialPath, testVersion, enableSSL,
                 snowflakeCloudPlatform, enableDeliveryGuaranteeTests=False):
        self.testVersion = testVersion
        self.credentialPath = credentialPath
        # can be None or one of AWS, AZURE, GCS
        self.snowflakeCloudPlatform = snowflakeCloudPlatform
        # default is false or set to true as env variable
        self.enableDeliveryGuaranteeTests = enableDeliveryGuaranteeTests
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
        self.MAX_RETRY = 30  # max wait time 30 mins
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
        consumer_config = self.client_config.copy()
        consumer_config['group.id'] = 'my-group-' + str(uuid.uuid4())
        consumer_config['auto.offset.reset'] = 'earliest'
        self.consumer = Consumer(consumer_config)
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
        print(datetime.now().strftime("\n%H:%M:%S "),
              "=== Sleep {} secs before verify result in Snowflake DB ===".format(
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
                print(datetime.now().strftime("\n%H:%M:%S "), "=== Non retryable error raised ===\n{}".format(e.msg),
                      flush=True)
                raise test_suit.test_utils.NonRetryableError()
            except snowflake.connector.errors.ProgrammingError as e:
                print("Error in VerifyWithRetry" + str(e))
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

    def deleteTopic(self, topicName):
        deleted_topics = self.adminClient.delete_topics([topicName])
        for topic, f in deleted_topics.items():
            try:
                f.result()  # The result itself is None
                print("Topic deletion successful:{}".format(topic))
            except Exception as e:
                print("Failed to delete topic {}: {}".format(topicName, e))

    def describeTopic(self, topicName):
        configs = self.adminClient.describe_configs(
            resources=[ConfigResource(restype=ConfigResource.Type.TOPIC, name=topicName)])
        for config_resource, f in configs.items():
            try:
                configs = f.result()
                print("Topic {} config is as follows:".format(topicName))
                for key, value in configs.items():
                    print(key, ':', value)
            except Exception as e:
                print("Failed to describe topic {}: {}".format(topicName, e))

    def createPartitions(self, topicName, new_total_partitions):
        kafka_partitions = self.adminClient.create_partitions(
            new_partitions=[NewPartitions(topicName, new_total_partitions)])
        for topic, f in kafka_partitions.items():
            try:
                f.result()  # The result itself is None
                print("Topic {} partitions created".format(topic))
            except Exception as e:
                print("Failed to create topic partitions {}: {}".format(topic, e))

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

    def consume_messages_dlq(self, fileName, partition_no, target_dlq_offset_number):
        '''

        :param fileName: File name to find out DLQ topic name from json config
        :param partition_no: partition no to search for target offset
        :param target_dlq_offset_number: Target offset number to find which stops finding any more offsets in DLQ
        :return: count of offsets
        '''
        with open('./rest_request_generated/' + fileName + '.json') as f:
            c = json.load(f)
            config = c['config']

        dlq_topic_name = config['errors.deadletterqueue.topic.name']
        return self.consume_messages(dlq_topic_name, partition_no, target_dlq_offset_number)

    def consume_messages(self, topic_name, partition_no, target_offset):
        '''
        Consumes messages from a topic and returns how many consumed.
        This function stops when target_offset number is reached
        :param topic_name: name of topic
        :param target_offset: Stops function when this offset is reached for partition 0
        :return: Count of messages consumed
        '''

        self.consumer.subscribe([topic_name])

        messages_consumed_count = 0
        start_time = time.time()
        try:
            while True:
                if time.time() - start_time >= 60:
                    print("Couldn't find target_offset:{0} in topic:{1} in 60 Seconds".format(target_offset, topic_name))
                    break
                msg = self.consumer.poll(10.0)  # Time out in seconds
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        print('Reached end of partition')
                    else:
                        print('Error while consuming message: {}'.format(msg.error()))
                else:
                    messages_consumed_count += 1
                    print('Received message: key={}, value={}, partition={}, offset={}'
                          .format(msg.key(), msg.value(), msg.partition(), msg.offset()))
                    if msg.partition() == partition_no and msg.offset() >= target_offset:
                        print('Reached target offset of {} for Topic:{}'.format(target_offset, topic_name))
                        break
        except KafkaError as e:
            print('Kafka error: {}'.format(e))

        return messages_consumed_count

    # returns kafka or confluent version
    def get_kafka_version(self):
        return self.testVersion


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

    def restartConnectorAndTasks(self, connectorName):
        requestURL = "http://{}/connectors/{}/restart?includeTasks=true&onlyFailed=false".format(self.kafkaConnectAddress, connectorName)
        r = requests.post(requestURL, headers=self.httpHeader)
        print(datetime.now().strftime("%H:%M:%S "), r, " restart connector and all tasks")

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
            # required for Snowpipe Streaming
            testRole = credentialJson["role"]
            testDatabase = credentialJson["database"]
            testSchema = credentialJson["schema"]
            pk = credentialJson["private_key"]
            # Use Encrypted key if passphrase is non empty
            pkEncrypted = credentialJson["encrypted_private_key"]

        print(datetime.now().strftime("\n%H:%M:%S "),
              "=== generate sink connector rest reqeuest from {} ===".format(rest_template_path))
        if not os.path.exists(rest_generate_path):
            os.makedirs(rest_generate_path)
        snowflake_connector_name = fileName.split(".")[0] + nameSalt
        snowflake_topic_name = snowflake_connector_name

        print(datetime.now().strftime("\n%H:%M:%S "),
              "=== Connector Config JSON: {}, Connector Name: {} ===".format(fileName, snowflake_connector_name))
        with open("{}/{}".format(rest_template_path, fileName), 'r') as f:
            fileContent = f.read()
            # Template has passphrase, use the encrypted version of P8 Key
            if fileContent.find("snowflake.private.key.passphrase") != -1:
                pk = pkEncrypted

            fileContent = fileContent \
                .replace("SNOWFLAKE_PRIVATE_KEY", pk) \
                .replace("SNOWFLAKE_HOST", testHost) \
                .replace("SNOWFLAKE_USER", testUser) \
                .replace("SNOWFLAKE_DATABASE", testDatabase) \
                .replace("SNOWFLAKE_SCHEMA", testSchema) \
                .replace("CONFLUENT_SCHEMA_REGISTRY", self.schemaRegistryAddress) \
                .replace("SNOWFLAKE_TEST_TOPIC", snowflake_topic_name) \
                .replace("SNOWFLAKE_CONNECTOR_NAME", snowflake_connector_name) \
                .replace("SNOWFLAKE_ROLE", testRole)
            with open("{}/{}".format(rest_generate_path, fileName), 'w') as fw:
                fw.write(fileContent)

        MAX_RETRY = 3
        retry = 0
        delete_url = "http://{}/connectors/{}".format(self.kafkaConnectAddress, snowflake_connector_name)
        post_url = "http://{}/connectors".format(self.kafkaConnectAddress)
        while retry < MAX_RETRY:
            try:
                print("Delete request:{0}".format(delete_url))
                code = requests.delete(delete_url, timeout=10).status_code
                print("Delete request returned:{0}".format(code))
                if code == 404 or code == 200 or code == 201:
                    break
            except BaseException as e:
                print('An exception occurred: {}'.format(e))
                pass
            print(datetime.now().strftime("\n%H:%M:%S "),
                  "=== sleep for 30 secs to wait for kafka connect to accept connection ===")
            sleep(30)
            retry += 1
        if retry == MAX_RETRY:
            print("Kafka Delete request not successful:{0}".format(delete_url))

        print("Post HTTP request to Create Connector:{0}".format(post_url))
        r = requests.post(post_url, json=json.loads(fileContent), headers=self.httpHeader)
        print("Connector Name:{0} POST Response:{1}".format(snowflake_connector_name, r.status_code), datetime.now().strftime("%H:%M:%S "))
        if not r.ok:
            print("Failed creating connector:{0} due to:{1} and HTTP response_code:{2}".format(snowflake_connector_name, r.reason, r.status_code))
            sleep(30)
            print("Retrying POST request for connector:{0}".format(snowflake_connector_name))
            r = requests.post(post_url, json=json.loads(fileContent), headers=self.httpHeader)
            print("Connector Name:{0} POST Response:{1}".format(snowflake_connector_name, r.status_code), datetime.now().strftime("%H:%M:%S "))
            if not r.ok:
                raise Exception("Failed to create connector:{0}".format(snowflake_connector_name))
        getConnectorResponse = requests.get(post_url)
        print("Get Connectors status:{0}, response:{1}".format(getConnectorResponse.status_code,
                                                               getConnectorResponse.content))

# These tests run from StressTest.yml file and not ran while running End-To-End Tests
def runStressTests(driver, testSet, nameSalt):
    from test_suit.test_pressure import TestPressure
    from test_suit.test_pressure_restart import TestPressureRestart

    testPressure = TestPressure(driver, nameSalt)

    # This test is more of a chaos test where we pause, delete, restart connectors to verify behavior.
    testPressureRestart = TestPressureRestart(driver, nameSalt)

    ############################ Stress Tests Round 1 ############################
    # TestPressure and TestPressureRestart will only run when Running StressTests
    print(datetime.now().strftime("\n%H:%M:%S "), "=== Stress Tests Round 1 ===")
    testSuitList = [testPressureRestart]

    testCleanEnableList = [True]
    testSuitEnableList = []
    if testSet == "confluent":
        testSuitEnableList = [True]
    elif testSet == "apache":
        testSuitEnableList = [True]
    elif testSet != "clean":
        errorExit("Unknown testSet option {}, please input confluent, apache or clean".format(testSet))

    execution(testSet, testSuitList, testCleanEnableList, testSuitEnableList, driver, nameSalt, round=1)
    ############################ Stress Tests Round 1 ############################

    ############################ Stress Tests Round 2 ############################
    print(datetime.now().strftime("\n%H:%M:%S "), "=== Stress Tests Round 2 ===")
    testSuitList = [testPressure]

    testCleanEnableList = [True]
    testSuitEnableList = []
    if testSet == "confluent":
        testSuitEnableList = [True]
    elif testSet == "apache":
        testSuitEnableList = [True]
    elif testSet != "clean":
        errorExit("Unknown testSet option {}, please input confluent, apache or clean".format(testSet))

    execution(testSet, testSuitList, testCleanEnableList, testSuitEnableList, driver, nameSalt, round=1)
    ############################ Stress Tests Round 2 ############################


def runTestSet(driver, testSet, nameSalt, enable_stress_test):
    if enable_stress_test:
        runStressTests(driver, testSet, nameSalt)
    else:
        test_suites = create_end_to_end_test_suites(driver, nameSalt, schemaRegistryAddress, testSet)

        ############################ round 1 ############################
        print(datetime.now().strftime("\n%H:%M:%S "), "=== Round 1 ===")

        end_to_end_tests_suite = [single_end_to_end_test.test_instance for single_end_to_end_test in test_suites.values()]

        end_to_end_tests_suite_cleaner = [single_end_to_end_test.clean for single_end_to_end_test in test_suites.values()]

        end_to_end_tests_suite_runner = []

        if testSet == "confluent":
            end_to_end_tests_suite_runner = [single_end_to_end_test.run_in_confluent for single_end_to_end_test in test_suites.values()]
        elif testSet == "apache":
            end_to_end_tests_suite_runner = [single_end_to_end_test.run_in_apache for single_end_to_end_test in test_suites.values()]
        elif testSet != "clean":
            errorExit("Unknown testSet option {}, please input confluent, apache or clean".format(testSet))

        execution(testSet, end_to_end_tests_suite, end_to_end_tests_suite_cleaner, end_to_end_tests_suite_runner, driver, nameSalt)

        ############################ Always run Proxy tests in the end ############################

        ############################ Proxy End To End Test ############################

        from test_suit.test_string_json_proxy import TestStringJsonProxy
        from test_suites import EndToEndTestSuite

        print(datetime.now().strftime("\n%H:%M:%S "), "=== Last Round: Proxy E2E Test ===")
        print("Proxy Test should be the last test, since it modifies the JVM values")

        proxy_tests_suite = [EndToEndTestSuite(
            test_instance=TestStringJsonProxy(driver, nameSalt), clean=True, run_in_confluent=True, run_in_apache=True
        )]

        end_to_end_proxy_tests_suite = [single_end_to_end_test.test_instance for single_end_to_end_test in proxy_tests_suite]

        proxy_suite_clean_enable_list = [single_end_to_end_test.clean for single_end_to_end_test in proxy_tests_suite]

        proxy_suite_runner = []

        if testSet == "confluent":
            proxy_suite_runner = [single_end_to_end_test.run_in_confluent for single_end_to_end_test in proxy_tests_suite]
        elif testSet == "apache":
            proxy_suite_runner = [single_end_to_end_test.run_in_apache for single_end_to_end_test in proxy_tests_suite]
        elif testSet != "clean":
            errorExit("Unknown testSet option {}, please input confluent, apache or clean".format(testSet))

        execution(testSet, end_to_end_proxy_tests_suite, proxy_suite_clean_enable_list, proxy_suite_runner, driver, nameSalt)
        ############################ Proxy End To End Test End ############################


def execution(testSet, testSuitList, testCleanEnableList, testSuitEnableList, driver, nameSalt, round=1):
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
                        print(datetime.now().strftime("\n%H:%M:%S "),
                              "=== Sending " + test.__class__.__name__ + " data ===")
                        test.send()
                        print(datetime.now().strftime("%H:%M:%S "), "=== Done " + test.__class__.__name__ + " ===",
                              flush=True)

                driver.verifyWaitTime()

                for i, test in enumerate(testSuitList):
                    if testSuitEnableList[i]:
                        print(datetime.now().strftime("\n%H:%M:%S "), "=== Verify " + test.__class__.__name__ + " ===")
                        driver.verifyWithRetry(test.verify, r)
                        print(datetime.now().strftime("%H:%M:%S "), "=== Passed " + test.__class__.__name__ + " ===",
                              flush=True)

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
    global schemaRegistryAddress
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

    # This will either be AWS, AZURE or GCS
    snowflakeCloudPlatform = None

    # If it is not set, we will not run delivery guarantee tests
    enableDeliveryGuaranteeTests = False
    if "SF_CLOUD_PLATFORM" in os.environ:
        snowflakeCloudPlatform = os.environ['SF_CLOUD_PLATFORM']

    if "ENABLE_DELIVERY_GUARANTEE_TESTS" in os.environ:
        enableDeliveryGuaranteeTests = (os.environ['ENABLE_DELIVERY_GUARANTEE_TESTS'] == 'True')

    kafkaTest = KafkaTest(kafkaAddress,
                          schemaRegistryAddress,
                          kafkaConnectAddress,
                          credentialPath,
                          testVersion,
                          enableSSL,
                          snowflakeCloudPlatform,
                          False)

    runTestSet(kafkaTest, testSet, nameSalt, pressure)
