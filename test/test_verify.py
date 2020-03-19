from avro.schema import Parse
from confluent_kafka import Producer
from confluent_kafka import avro as confluent_avro
from confluent_kafka.avro import AvroProducer
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric import dsa
from cryptography.hazmat.primitives import serialization
from time import sleep
import avro
import json
import io
import os
import re
import sys
import snowflake.connector

ValueSchemaStr = """
{
    "type":"record",
    "name":"value_schema",
    "fields":[
        {"name":"id","type":"int"},
        {"name":"firstName","type":"string"},
        {"name":"time","type":
            "int"
        }
    ]
}
"""
# {"type":"int","connect.version":1,"connect.name":"org.apache.kafka.connect.data.Time","logicalType":"time-millis"}

KeySchemaStr = """
{
    "type":"record",
    "name":"key_schema",
    "fields":[
        {"name":"id","type":"int"}
    ]
} 
"""

def errorExit(message):
    print(message)
    exit(1)

# define Python user-defined exceptions
class Error(Exception):
   """Base class for other exceptions"""
   pass

class RetryableError(Error):
   """Raised when we can retry"""
   pass

class NonRetryableError(Error):
   """Raised when we cannot retry"""
   pass

class KafkaTest:
    def __init__(self, kafkaAddress, schemaRegistryAddress, testJsonTopic, testAvroSRTopic, testAvroTopic, testSet,
                testHost, testUser, testDatabase, testSchema, testWarehouse, pk, pk_passphrase):
        
        self.SEND_INTERVAL = 0.01 # send a record every 10 ms
        self.VERIFY_INTERVAL = 60 # verify every 60 secs
        self.MAX_RETRY = 10 # max wait time 10 mins

        keySchema = confluent_avro.loads(KeySchemaStr)
        valueSchema = confluent_avro.loads(ValueSchemaStr)
        self.testJsonTopic = testJsonTopic
        self.testAvroSRTopic = testAvroSRTopic
        self.testAvroTopic = testAvroTopic

        self.producer = Producer({'bootstrap.servers': kafkaAddress})
        self.avroProducer = AvroProducer({'bootstrap.servers': kafkaAddress, 
                                          'schema.registry.url': schemaRegistryAddress
                                         }, default_value_schema=valueSchema, default_key_schema=keySchema)
        
        reg = "[^\/]*snowflakecomputing" # find the account name
        account = re.findall(reg, testHost)
        if len(account) != 1 or len(account[0]) < 20:
            print("Format error in 'host' field at profile.json, expecting account.snowflakecomputing.com:443")

        pkb = self.parsePrivateKey(pk, pk_passphrase)
        
        self.snowflake_conn = snowflake.connector.connect(
                user = testUser,
                private_key = pkb,
                account = account[0][:-19],
                warehouse = testWarehouse,
                database = testDatabase,
                schema = testSchema
                )

        self.setupTestSet(testSet)

    def setupTestSet(self, testSet):
        if testSet == "confluent":
            self.enable1 = True
            self.enable2 = True
            self.enable3 = True
        elif testSet == "apache":
            self.enable1 = True
            self.enable2 = False
            self.enable3 = True
        else:
            errorExit("Unknown testSet option {}, please input either confluent or apache".format(testSet))
    
    def parsePrivateKey(self, pk, pk_passphrase):
        pkpass = None
        if len(pk_passphrase) != 0:
            pkpass = pk_passphrase.encode()

        # remove header, footer, and line breaks
        pk = re.sub("-+[A-Za-z ]+-+", "", pk)
        pk = re.sub("\\s", "", pk)

        pkBuilder = ""
        pkBuilder += "-----BEGIN ENCRYPTED PRIVATE KEY-----"
        for i, c in enumerate(pk):
            if i % 64 == 0:
                pkBuilder += "\n"
            pkBuilder += c
        pkBuilder += "\n-----END ENCRYPTED PRIVATE KEY-----"

        p_key = serialization.load_pem_private_key(
            pkBuilder.encode(),
            password = pkpass,
            backend = default_backend()
        )

        pkb = p_key.private_bytes(
            encoding = serialization.Encoding.DER,
            format = serialization.PrivateFormat.PKCS8,
            encryption_algorithm = serialization.NoEncryption())

        return pkb

    def msgSendInterval(self):
        # sleep self.SEND_INTERVAL before send the second message
        sleep(self.SEND_INTERVAL)

    def verifyWaitTime(self):
        # sleep two minutes before verify result in SF DB
        print("\n=== Sleep {} secs before verify result in Snowflake DB ===".format(self.VERIFY_INTERVAL), flush=True)
        sleep(self.VERIFY_INTERVAL)

    def verifyWithRetry(self, func, cleanUp):
        retryNum = 0
        while retryNum < self.MAX_RETRY:
            try:
                func()
                break
            except RetryableError:
                retryNum += 1
                print("\n=== Failed ===", flush=True)
                self.verifyWaitTime()
            except NonRetryableError:
                print("\n=== Non retryable error raised ===")
                cleanUp()
                exit(1)
        if retryNum == self.MAX_RETRY:
            print("\n=== Max retry exceeded ===")
            cleanUp()
            exit(1)


    def sendJsonData(self, topic, value):
        for v in value:
            self.producer.produce(topic, json.dumps(v).encode('utf-8'))
            self.msgSendInterval()
        self.producer.flush()

    def sendAvroSRData(self, topic, key, value):
        for k, v in zip(key, value):
            self.avroProducer.produce(topic=topic, key=k, value=v)
            self.msgSendInterval()
        self.avroProducer.flush()

    def sendAvroData(self, topic, value):
        for v in value:
            self.producer.produce(topic, v)
        self.producer.flush()

    def runParallel(self):
        self.runTest()
        self.verifyWaitTime()
        self.runVerify()
        print("\n=== All test passed ===")

    def cleanUpTable(self, tableName):
        print("\n=== Clean up table {} ===".format(tableName))
        try:
            self.snowflake_conn.cursor().execute("DELETE FROM {}".format(tableName)).fetchone()[0]
        except:
            print("\n=== Error cleaning up table {} ===".format(tableName))

    ##############################################################################
    # add your test data and verify script here                                  #
    ##############################################################################
    def runTest(self):
        if self.enable1:
            self.test1()
        if self.enable2:
            self.test2()
        if self.enable3:
            self.test3()

    def runVerify(self):
        if self.enable1:
            self.verify1()
        if self.enable2:
            self.verify2()
        if self.enable3:
            self.verify3()
    
    ##############################################################################
    # define your test data and verify script here                               #
    # remember to clean up test data after verification                          #
    ##############################################################################
    def test1(self):
        self.verifyCleanUp1()
        print("\n=== Sending test1 json data ===")
        data = []
        for e in range(100):
            data.append({'number' : str(e)})
        self.sendJsonData(self.testJsonTopic, data)
        print("=== Done ===")

    def verify1(self):
        self.verifyWithRetry(self.verifyRetry1, self.verifyCleanUp1)
        print("=== test1 passed ===")

    def verifyRetry1(self):
        res = self.snowflake_conn.cursor().execute("SELECT count(*) FROM {}".format(self.testJsonTopic)).fetchone()[0]
        if res == 0:
            raise RetryableError() 
        elif res != 100:
            raise NonRetryableError()

    def verifyCleanUp1(self):
        self.cleanUpTable(self.testJsonTopic)


    def test2(self):
        self.verifyCleanUp2()
        print("\n=== Sending test2 avro with schema registry data ===")
        value = []
        key = []
        for e in range(100):
            # avro data must follow the schema defined in ValueSchemaStr
            key.append({"id":0})
            value.append({"id":0,"firstName":"abc0","time":18325})
        self.sendAvroSRData(self.testAvroSRTopic, key, value)
        print("=== Done ===")

    def verify2(self):
        self.verifyWithRetry(self.verifyRetry2, self.verifyCleanUp2)
        print("=== test2 passed ===")

    def verifyRetry2(self):
        res = self.snowflake_conn.cursor().execute("SELECT count(*) FROM {}".format(self.testAvroSRTopic)).fetchone()[0]
        if res == 0:
            raise RetryableError() 
        elif res != 100:
            raise NonRetryableError()

    def verifyCleanUp2(self):
        self.cleanUpTable(self.testAvroSRTopic)


    def test3(self):
        self.verifyCleanUp3()
        print("\n=== Sending test3 avro data ===")
        value = []
        for e in range(50):
            value.append(open("./test_avro_data/twitter.avro", "rb").read())
        self.sendAvroData(self.testAvroTopic, value)
        print("=== Done ===")

    def verify3(self):
        self.verifyWithRetry(self.verifyRetry3, self.verifyCleanUp3)
        print("=== test3 passed ===")

    def verifyRetry3(self):
        res = self.snowflake_conn.cursor().execute("SELECT count(*) FROM {}".format(self.testAvroTopic)).fetchone()[0]
        if res == 0:
            raise RetryableError() 
        elif res != 100:
            raise NonRetryableError()

    def verifyCleanUp3(self):
        self.cleanUpTable(self.testAvroTopic)

    ##############################################################################
    #                                    end                                     #
    ##############################################################################
        

if __name__ == "__main__":
    if len(sys.argv) != 7:
        errorExit("""\n=== Usage: ./ingest.py <kafka address> <schema registry address> <test JSON topic> \ 
                            <test AVRO schema registry topic> <test AVRO topic> <test set> ===""")
    
    kafkaAddress = sys.argv[1]
    schemaRegistryAddress = sys.argv[2]
    testJsonTopic = sys.argv[3]
    testAvroSRTopic = sys.argv[4]
    testAvroTopic = sys.argv[5]
    testSet = sys.argv[6]

    if "SNOWFLAKE_CREDENTIAL_FILE" not in os.environ:
        errorExit("\n=== Require environment variable SNOWFLAKE_CREDENTIAL_FILE but it's not set.  Aborting. ===")

    credentialPath = os.environ['SNOWFLAKE_CREDENTIAL_FILE']

    if not os.path.isfile(credentialPath):
        errorExit("\n=== Provided SNOWFLAKE_CREDENTIAL_FILE {} does not exist.  Aborting. ===".format(credentialPath))

    with open(credentialPath) as f:
        credentialJson = json.load(f)

        testHost = credentialJson["host"]
        testUser = credentialJson["user"]
        testDatabase = credentialJson["database"]
        testSchema = credentialJson["schema"]
        testWarehouse = credentialJson["warehouse"]
        pk = credentialJson["encrypted_private_key"]
        pk_passphrase = credentialJson["private_key_passphrase"]

    kafkaTest = KafkaTest(kafkaAddress, schemaRegistryAddress, testJsonTopic, testAvroSRTopic, testAvroTopic, testSet,
                        testHost, testUser, testDatabase, testSchema, testWarehouse, pk, pk_passphrase)

    kafkaTest.runParallel()
