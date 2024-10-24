from test_suit.test_utils import RetryableError, NonRetryableError
from time import sleep
from confluent_kafka import avro
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry import Schema
from test_suit.base_e2e import BaseE2eTest

# SR -> Schema Registry
# Runs only in confluent test suite environment
class TestAutoTableCreationTopic2Table(BaseE2eTest):
    def __init__(self, driver, nameSalt, schemaRegistryAddress, testSet):
        self.driver = driver
        self.fileName = "travis_correct_auto_table_creation_topic2table"
        self.table = self.fileName + nameSalt
        self.topics = []

        self.topicNum = 2
        self.recordNum = 100
        self.partitionNum = 1

        for i in range(self.topicNum):
            self.topics.append(self.table + str(i))

        # the schema registry should only be started in confluent test suite environment
        if testSet != "confluent":
            return

        self.schemaRegistryAddress = schemaRegistryAddress
        conf = {"url": self.schemaRegistryAddress}
        self.srClient = SchemaRegistryClient(conf)

        ValueSchemaStr = []

        ValueSchemaStr.append("""
        {
            "type":"record",
            "name":"value_schema_0",
            "fields":[
                {"name":"id","type":"int"},
                {"name":"approval","type":"boolean"},
                {"name":"info_map","type":{"type":"map","values":"string"}}
            ]
        }
        """)

        ValueSchemaStr.append("""
        {
            "type":"record",
            "name":"value_schema_1",
            "fields":[
                {"name":"id","type":"int"},
                {"name":"first_name","type":"string"},
                {"name":"rating","type":"float"}
            ]
        }
        """)

        self.goldSchema = {
            'ID': 'NUMBER',
            'FIRST_NAME': 'VARCHAR',
            'RATING': 'FLOAT',
            'APPROVAL': 'BOOLEAN',
            'INFO_MAP': 'VARIANT',
            'RECORD_METADATA': 'VARIANT'
        }

        self.records = []

        self.records.append({
            'id': 100,
            'approval': 'true',
            'info_map': {
                'TREE_1': 'APPLE',
                'TREE_2': 'PINEAPPLE'
            }
        })

        self.records.append({
            'id': 100,
            'first_name': 'Zekai',
            'rating': 0.99
        })

        self.valueSchema = []

        for i, topic in enumerate(self.topics):
            self.valueSchema.append(avro.loads(ValueSchemaStr[i]))
            avroSchema = Schema(ValueSchemaStr[i], "AVRO")
            self.srClient.register_schema(topic + '-value', avroSchema)

            # create topic and partitions in constructor since the post REST api will automatically create topic with only one partition
            self.driver.createTopics(topic, partitionNum=self.partitionNum, replicationNum=1)

    def getConfigFileName(self):
        return self.fileName + ".json"

    def send(self):
        # create topic with n partitions and only one replication factor
        print("Partition count:" + str(self.partitionNum))
        print("Topics:", self.topics)

        for i, topic in enumerate(self.topics):
            self.driver.describeTopic(topic)
            for p in range(self.partitionNum):
                print("Sending in Partition:" + str(p))
                key = []
                value = []
                for _ in range(self.recordNum):
                    value.append(self.records[i])
                self.driver.sendAvroSRData(topic, value, self.valueSchema[i], key=[], key_schema="", partition=p)
                sleep(2)

    def verify(self, round):
        res_col_info = self.driver.snowflake_conn.cursor().execute(
            "desc table {}".format(self.table)).fetchall()

        col_set = []
        for col in res_col_info:
            col_set.append(col[0])
            typeInSF = col[1]
            if '(' in typeInSF:
                typeInSF = typeInSF[:typeInSF.find('(')]
            if self.goldSchema[col[0]] != typeInSF:
                raise NonRetryableError("Column {} type mismatch. Desired: {}, but got: {}".format(col[0], self.goldSchema[col[0]], typeInSF))

        print('column present:' + str(col_set))
        for key in self.goldSchema:
            if key not in col_set:
                raise NonRetryableError("Missing column {}".format(key))


        res = self.driver.snowflake_conn.cursor().execute(
            "SELECT count(*) FROM {}".format(self.table)).fetchone()[0]
        if res == 0:
            raise RetryableError()
        elif res != self.recordNum * self.topicNum:
            raise NonRetryableError("Number of record in table is different from number of record sent")

    def clean(self):
        # dropping of stage and pipe doesnt apply for snowpipe streaming. (It executes drop if exists)
        self.driver.cleanTableStagePipe(self.table)
        return

