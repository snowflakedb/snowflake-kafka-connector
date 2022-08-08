from test_suit.test_utils import RetryableError, NonRetryableError
from time import sleep
from confluent_kafka import avro

# SR -> Schema Registry
# Runs only in confluent test suite environment
class TestAutoTableCreation:
    def __init__(self, driver, nameSalt):
        self.driver = driver
        self.fileName = "travis_correct_auto_table_creation"
        self.topic = self.fileName + nameSalt

        self.topicNum = 1
        self.recordNum = 100
        self.partitionNum = 1

        ValueSchemaStr = """
        {
            "type":"record",
            "name":"value_schema",
            "fields":[
                {"name":"id","type":"int"},
                {"name":"first_name","type":"string"},
                {"name":"rating","type":"float"},
                {"name":"approval","type":"boolean"},
                {"name":"info_array","type":{"type":"array","items":"string"}},
                {"name":"info_map","type":{"type":"map","values":"string"}}
            ]
        }
        """

        self.snowflakeSchema = {
            'ID': 'NUMBER',
            'FIRST_NAME': 'TEXT',
            'RATING': 'FLOAT',
            'APPROVAL': 'BOOLEAN',
            'INFO_ARRAY': 'ARRAY',
            'INFO_MAP': 'VARIANT',
            'RECORD_METADATA': 'VARIANT'
        }

        self.record = {
            'id': 100,
            'first_name': 'Zekai',
            'rating': 0.99,
            'approval': 'true',
            'info_array': ['HELLO', 'WORLD'],
            'info_map': {
                'TREE_1': 'APPLE',
                'TREE_2': 'PINEAPPLE'
            }
        }

        self.valueSchema = avro.loads(ValueSchemaStr)

        # create topic and partitions in constructor since the post REST api will automatically create topic with only one partition
        self.driver.createTopics(self.topic, partitionNum=self.partitionNum, replicationNum=1)

    def getConfigFileName(self):
        return self.fileName + ".json"

    def send(self):
        # create topic with n partitions and only one replication factor
        print("Partition count:" + str(self.partitionNum))
        print("Topic:", self.topic)

        self.driver.describeTopic(self.topic)

        for p in range(self.partitionNum):
            print("Sending in Partition:" + str(p))
            key = []
            value = []
            for _ in range(self.recordNum):
                value.append(self.record)
            self.driver.sendAvroSRData(self.topic, value, self.valueSchema, key=[], key_schema="", partition=p)
            sleep(2)

    def verify(self, round):
        res_col_info = self.driver.snowflake_conn.cursor().execute(
            "Select * from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME = '{}'".format(self.topic)).fetchall();

        col_set = []
        for col in res_col_info:
            col_set.append(col)
            if self.snowflakeSchema[col[3]] != col[7]:
                raise NonRetryableError("Column {} type mismatch. Desired: {}, but got: {}".format(col[3], self.snowflakeSchema[col[3]], col[7]))

        for key in self.snowflakeSchema:
            if key not in col_set:
                raise NonRetryableError("Missing column {}".format(key))

        res = self.driver.snowflake_conn.cursor().execute(
            "SELECT count(*) FROM {}".format(self.topic)).fetchone()[0]
        if res == 0:
            raise RetryableError()
        elif res != 100:
            raise NonRetryableError("Number of record in table is different from number of record sent")




def clean(self):
        # dropping of stage and pipe doesnt apply for snowpipe streaming. (It executes drop if exists)
        self.driver.cleanTableStagePipe(self.topic)
        return

