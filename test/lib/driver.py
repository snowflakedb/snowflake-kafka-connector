import json
import logging
import re
import time
import uuid
from pathlib import Path

import requests
import snowflake.connector
from confluent_kafka import Consumer, KafkaError, Producer
from confluent_kafka.admin import AdminClient, ConfigResource, NewPartitions, NewTopic
from confluent_kafka.avro import AvroProducer

from lib.config import Profile, SnowflakeConnectorConfig
from test_suit.test_utils import NonRetryableError, ResetAndRetry, RetryableError

logger = logging.getLogger(__name__)


class KafkaDriver:
    def __init__(
        self,
        kafkaAddress: str,
        schemaRegistryAddress: str,
        kafkaConnectAddress: str,
        credentials: Profile,
        testVersion: str,
        enableSSL: bool,
    ):
        self.testVersion = testVersion
        self.credentials = credentials

        self.TEST_DATA_FOLDER = Path("test_data")
        self.httpHeader = {"Content-type": "application/json", "Accept": "application/json"}

        self.SEND_INTERVAL = 0.01  # send a record every 10 ms
        self.VERIFY_INTERVAL = 10  # verify every 10 secs
        self.MAX_RETRY = 60  # max wait time 1 min
        self.MAX_FLUSH_BUFFER_SIZE = 5000  # flush buffer when 5000 data was in the queue

        self.kafkaConnectAddress = kafkaConnectAddress
        self.schemaRegistryAddress = schemaRegistryAddress
        self.kafkaAddress = kafkaAddress

        if enableSSL:
            logger.info("=== Enable SSL ===")
            self.client_config = {
                "bootstrap.servers": kafkaAddress,
                "security.protocol": "SASL_SSL",
                "ssl.ca.location": "./crts/ca-cert",
                "sasl.mechanism": "PLAIN",
                "sasl.username": "client",
                "sasl.password": "client-secret",
            }
        else:
            self.client_config = {
                "bootstrap.servers": kafkaAddress,
                "broker.address.family": "v4",
            }

        self.adminClient = AdminClient(self.client_config)

        producer_config = self.client_config.copy()
        # Setting max request size to 30 MiB to support large blob tests.
        producer_config["message.max.bytes"] = 31457280  # 30 MiB
        self.producer = Producer(producer_config)

        consumer_config = self.client_config.copy()
        consumer_config["group.id"] = f"my-group-{uuid.uuid4()}"
        consumer_config["auto.offset.reset"] = "earliest"
        self.consumer = Consumer(consumer_config)

        avro_producer_config = producer_config.copy()
        avro_producer_config["schema.registry.url"] = schemaRegistryAddress
        self.avroProducer = AvroProducer(avro_producer_config)

        snowflake_connector_config = SnowflakeConnectorConfig.from_profile(credentials)
        self.snowflake_conn = snowflake.connector.connect(**snowflake_connector_config.to_dict())

    def msgSendInterval(self):
        # sleep self.SEND_INTERVAL before send the second message
        time.sleep(self.SEND_INTERVAL)

    def startConnectorWaitTime(self):
        time.sleep(10)

    def verifyWaitTime(self):
        # sleep before verifying result in SF DB
        logger.info(f"=== Sleep {self.VERIFY_INTERVAL} secs before verify result in Snowflake DB ===")
        time.sleep(self.VERIFY_INTERVAL)

    def verifyWithRetry(self, func, retry_round, configFileName):
        retryNum = 0
        while retryNum < self.MAX_RETRY:
            try:
                func(retry_round)
                break
            except ResetAndRetry:
                retryNum = 0
                logger.info(f"=== Reset retry count and retry {configFileName} ===")
            except RetryableError as e:
                retryNum += 1
                logger.warning(f"=== Failed {configFileName}, retryable. {e.msg} ===")
                self.verifyWaitTime()
            except NonRetryableError as e:
                logger.error(f"=== Non retryable error for {configFileName} raised ===\n{e.msg}")
                raise e
            except snowflake.connector.errors.ProgrammingError as e:
                logger.error(f"Error in VerifyWithRetry for {configFileName}: {e}")
                if e.errno == 2003:
                    retryNum += 1
                    logger.warning(f"=== Failed, table not created for {configFileName} ===")
                    self.verifyWaitTime()
                else:
                    raise
        if retryNum == self.MAX_RETRY:
            logger.error(f"=== Max retry exceeded for {configFileName} ===")
            raise NonRetryableError()

    def createTopics(self, topicName, partitionNum=1, replicationNum=1):
        self.adminClient.create_topics([NewTopic(topicName, partitionNum, replicationNum)])

    def deleteTopic(self, topicName):
        deleted_topics = self.adminClient.delete_topics([topicName])
        for topic, f in deleted_topics.items():
            try:
                f.result()  # The result itself is None
                logger.info(f"Topic deletion successful: {topic}")
            except Exception as e:
                logger.error(f"Failed to delete topic {topicName}: {e}")

    def describeTopic(self, topicName):
        configs = self.adminClient.describe_configs(
            resources=[ConfigResource(restype=ConfigResource.Type.TOPIC, name=topicName)]
        )
        for _, f in configs.items():
            try:
                configs = f.result()
                logger.info(f"Topic {topicName} config is as follows:")
                for key, value in configs.items():
                    logger.info(f"{key}: {value}")
            except Exception as e:
                logger.error(f"Failed to describe topic {topicName}: {e}")

    def createPartitions(self, topicName, new_total_partitions):
        kafka_partitions = self.adminClient.create_partitions(
            new_partitions=[NewPartitions(topicName, new_total_partitions)]
        )
        for topic, f in kafka_partitions.items():
            try:
                f.result()  # The result itself is None
                logger.info(f"Topic {topic} partitions created")
            except Exception as e:
                logger.error(f"Failed to create topic partitions {topic}: {e}")

    def sendBytesData(self, topic, value, key=None, partition=0, headers=None):
        if not key:
            for i, v in enumerate(value):
                self.producer.produce(topic, value=v, partition=partition, headers=headers or [])
                if (i + 1) % self.MAX_FLUSH_BUFFER_SIZE == 0:
                    self.producer.flush()
        else:
            for i, (k, v) in enumerate(zip(key, value, strict=True)):
                self.producer.produce(topic, value=v, key=k, partition=partition, headers=headers or [])
                if (i + 1) % self.MAX_FLUSH_BUFFER_SIZE == 0:
                    self.producer.flush()
        self.producer.flush()

    def sendAvroSRData(self, topic, value, value_schema, key=None, key_schema="", partition=0, headers=None):
        if not key:
            for i, v in enumerate(value):
                self.avroProducer.produce(
                    topic=topic, value=v, value_schema=value_schema, partition=partition, headers=headers or []
                )
                if (i + 1) % self.MAX_FLUSH_BUFFER_SIZE == 0:
                    self.producer.flush()
        else:
            for i, (k, v) in enumerate(zip(key, value, strict=True)):
                self.avroProducer.produce(
                    topic=topic,
                    value=v,
                    value_schema=value_schema,
                    key=k,
                    key_schema=key_schema,
                    partition=partition,
                    headers=headers or [],
                )
                if (i + 1) % self.MAX_FLUSH_BUFFER_SIZE == 0:
                    self.producer.flush()
        self.avroProducer.flush()

    def consume_messages_dlq(self, fileName, partition_no, target_dlq_offset_number):
        """

        :param fileName: File name to find out DLQ topic name from json config
        :param partition_no: partition no to search for target offset
        :param target_dlq_offset_number: Target offset number to find which stops finding any more offsets in DLQ
        :return: count of offsets
        """
        with (Path("rest_request_generated") / f"{fileName}.json").open() as f:
            c = json.load(f)
            config = c["config"]

        dlq_topic_name = config["errors.deadletterqueue.topic.name"]
        return self.consume_messages(dlq_topic_name, partition_no, target_dlq_offset_number)

    def consume_messages(self, topic_name, partition_no, target_offset):
        """
        Consumes messages from a topic and returns how many consumed.
        This function stops when target_offset number is reached
        :param topic_name: name of topic
        :param target_offset: Stops function when this offset is reached for partition 0
        :return: Count of messages consumed
        """

        self.consumer.subscribe([topic_name])

        messages_consumed_count = 0
        start_time = time.time()
        try:
            while True:
                if time.time() - start_time >= 60:
                    logger.warning(f"Couldn't find target_offset:{target_offset} in topic:{topic_name} in 60 Seconds")
                    break
                msg = self.consumer.poll(10.0)  # Time out in seconds
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.info("Reached end of partition")
                    else:
                        logger.error(f"Error while consuming message: {msg.error()}")
                else:
                    messages_consumed_count += 1
                    logger.debug(
                        f"Received message: key={msg.key()}, value={msg.value()}, partition={msg.partition()}, offset={msg.offset()}"
                    )
                    if msg.partition() == partition_no and msg.offset() >= target_offset:
                        logger.info(f"Reached target offset of {target_offset} for Topic:{topic_name}")
                        break
        except KafkaError as e:
            logger.error(f"Kafka error: {e}")

        return messages_consumed_count

    # returns kafka or confluent version
    def get_kafka_version(self):
        return self.testVersion

    def cleanTableStagePipe(self, connectorName, topicName="", partitionNumber=1):
        if not topicName:
            topicName = connectorName
        tableName = topicName

        logger.info(f"=== Drop table {tableName} ===")
        self.snowflake_conn.cursor().execute(f"DROP table IF EXISTS {tableName}")

        for p in range(partitionNumber):
            pipeName = f"SNOWFLAKE_KAFKA_CONNECTOR_{connectorName}_PIPE_{topicName}_{p}"
            logger.info(f"=== Drop pipe {pipeName} ===")
            self.snowflake_conn.cursor().execute(f"DROP pipe IF EXISTS {pipeName}")

        ssv2PipeName = f"SNOWFLAKE_KAFKA_CONNECTOR_SSV2_PIPE_{tableName}"
        self.snowflake_conn.cursor().execute(f"DROP PIPE IF EXISTS {ssv2PipeName}")

        logger.info("=== Done ===")

    def enable_schema_evolution_for_iceberg(self, table: str):
        self.snowflake_conn.cursor().execute(f"alter iceberg table {table} set ENABLE_SCHEMA_EVOLUTION = true")

    def create_empty_iceberg_table(self, table_name: str, external_volume: str):
        sql = f"""
            CREATE ICEBERG TABLE IF NOT EXISTS {table_name} (
                record_metadata OBJECT()
            )
            EXTERNAL_VOLUME = '{external_volume}'
            CATALOG = 'SNOWFLAKE'
            BASE_LOCATION = '{table_name}'
            ;
        """
        self.snowflake_conn.cursor().execute(sql)

    def create_table(self, table_name: str):
        sql = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    RECORD_METADATA VARIANT
                )
            """
        logger.info(f"=== Creating table {table_name} ===")
        logger.info(f"{sql}")
        self.snowflake_conn.cursor().execute(sql)
        logger.info(f"=== Table {table_name} created ===")

    def drop_table(self, table_name: str):
        sql = f"""
                DROP TABLE IF EXISTS {table_name}
            """
        logger.info(f"=== Dropping table {table_name} ===")
        self.snowflake_conn.cursor().execute(sql)
        logger.info(f"=== Table {table_name} dropped ===")

    def create_iceberg_table_with_sample_content(self, table_name: str, external_volume: str):
        sql = f"""
            CREATE ICEBERG TABLE IF NOT EXISTS {table_name} (
                record_content OBJECT(
                    id INT,
                    body_temperature FLOAT,
                    name STRING,
                    approved_coffee_types ARRAY(STRING),
                    animals_possessed OBJECT(dogs BOOLEAN, cats BOOLEAN)
                )
            )
            EXTERNAL_VOLUME = '{external_volume}'
            CATALOG = 'SNOWFLAKE'
            BASE_LOCATION = '{table_name}'
            ;
        """
        self.snowflake_conn.cursor().execute(sql)

    def drop_iceberg_table(self, table_name: str):
        self.snowflake_conn.cursor().execute(f"DROP ICEBERG TABLE IF EXISTS {table_name}")

    def select_number_of_records(self, table_name: str) -> str:
        return self.snowflake_conn.cursor().execute(f"SELECT count(*) FROM {table_name}").fetchone()[0]

    # Escape JSON-structural characters that are also regex metacharacters,
    # while preserving intentional regex wildcards like \d* and \w*.
    _GOLD_REGEX_ESCAPE = str.maketrans(
        {
            '"': r"\"",
            "{": r"\{",
            "}": r"\}",
            "[": r"\[",
            "]": r"\]",
            "+": r"\+",
        }
    )

    @staticmethod
    def _to_gold_regex(pattern):
        """Wrap a gold pattern into a full-match regex, escaping JSON-structural characters."""
        return f"^{pattern.translate(KafkaDriver._GOLD_REGEX_ESCAPE)}$"

    def regexMatchOneLine(self, res, goldMetaRegex, goldContentRegex):
        """Validate that a result row's metadata and content match the gold regex patterns."""
        meta = res[0].replace(" ", "").replace("\n", "")
        content = res[1].replace(" ", "").replace("\n", "")
        goldMetaRegex = self._to_gold_regex(goldMetaRegex)
        goldContentRegex = self._to_gold_regex(goldContentRegex)
        if re.search(goldMetaRegex, meta) is None:
            raise NonRetryableError(f"Record meta data:\n{meta}\ndoes not match gold regex label:\n{goldMetaRegex}")
        if re.search(goldContentRegex, content) is None:
            raise NonRetryableError(f"Record content:\n{content}\ndoes not match gold regex label:\n{goldContentRegex}")

    def regexMatchMeta(self, meta, goldMetaRegex):
        """Validate that a metadata string matches the gold regex pattern."""
        meta = meta.replace(" ", "").replace("\n", "")
        goldMetaRegex = self._to_gold_regex(goldMetaRegex)
        if re.search(goldMetaRegex, meta) is None:
            raise NonRetryableError(f"Record meta data:\n{meta}\ndoes not match gold regex label:\n{goldMetaRegex}")

    def updateConnectorConfig(self, fileName, connectorName, configMap):
        with (Path("rest_request_generated") / f"{fileName}.json").open() as f:
            c = json.load(f)
            config = c["config"]
            for k in configMap:
                config[k] = configMap[k]
        requestURL = f"http://{self.kafkaConnectAddress}/connectors/{connectorName}/config"
        r = requests.put(requestURL, json=config, headers=self.httpHeader)
        logger.info(f"{r} updated connector config")

    def restartConnector(self, connectorName):
        requestURL = f"http://{self.kafkaConnectAddress}/connectors/{connectorName}/restart"
        r = requests.post(requestURL, headers=self.httpHeader)
        logger.info(f"{r} restart connector")

    def restartConnectorAndTasks(self, connectorName):
        requestURL = (
            f"http://{self.kafkaConnectAddress}/connectors/{connectorName}/restart?includeTasks=true&onlyFailed=false"
        )
        r = requests.post(requestURL, headers=self.httpHeader)
        logger.info(f"{r} restart connector and all tasks")

    def pauseConnector(self, connectorName):
        requestURL = f"http://{self.kafkaConnectAddress}/connectors/{connectorName}/pause"
        r = requests.put(requestURL, headers=self.httpHeader)
        logger.info(f"{r} pause connector")

    def resumeConnector(self, connectorName):
        requestURL = f"http://{self.kafkaConnectAddress}/connectors/{connectorName}/resume"
        r = requests.put(requestURL, headers=self.httpHeader)
        logger.info(f"{r} resume connector")

    def deleteConnector(self, connectorName):
        requestURL = f"http://{self.kafkaConnectAddress}/connectors/{connectorName}"
        r = requests.delete(requestURL, headers=self.httpHeader)
        logger.info(f"{r} delete connector")

    def closeConnector(self, fileName, nameSalt):
        snowflake_connector_name = fileName.split(".")[0] + nameSalt
        delete_url = f"http://{self.kafkaConnectAddress}/connectors/{snowflake_connector_name}"
        logger.info(f"=== Delete connector {snowflake_connector_name} ===")
        code = requests.delete(delete_url, timeout=10).status_code
        logger.info(f"Delete response code: {code}")

    def createConnector(self, fileName, nameSalt):
        rest_template_path = Path("rest_request_template")
        rest_generate_path = Path("rest_request_generated")

        logger.info(f"=== generate sink connector rest request from {rest_template_path} ===")
        rest_generate_path.mkdir(parents=True, exist_ok=True)
        snowflake_connector_name = fileName.split(".")[0] + nameSalt
        snowflake_topic_name = snowflake_connector_name

        logger.info(f"=== Connector Config JSON: {fileName}, Connector Name: {snowflake_connector_name} ===")
        with (rest_template_path / fileName).open() as f:
            config_template = json.load(f)

        def replace_values(obj, replacements):
            """Recursively traverse a parsed JSON object, applying substring replacements to string values."""
            if isinstance(obj, dict):
                return {k: replace_values(v, replacements) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [replace_values(item, replacements) for item in obj]
            elif isinstance(obj, str):
                for old, new in replacements.items():
                    obj = obj.replace(old, new)
                return obj
            else:
                return obj

        config = replace_values(
            config_template,
            {
                "SNOWFLAKE_HOST": self.credentials.make_url(),
                "SNOWFLAKE_DATABASE": self.credentials.database,
                "SNOWFLAKE_SCHEMA": self.credentials.schema,
                "SNOWFLAKE_USER": self.credentials.user,
                "SNOWFLAKE_ROLE": self.credentials.role,
                "SNOWFLAKE_PRIVATE_KEY": self.credentials.private_key,
                "CONFLUENT_SCHEMA_REGISTRY": self.schemaRegistryAddress,
                "SNOWFLAKE_TEST_TOPIC": snowflake_topic_name,
                "SNOWFLAKE_CONNECTOR_NAME": snowflake_connector_name,
            },
        )

        with (rest_generate_path / fileName).open("w") as fw:
            json.dump(config, fw, indent=4)

        MAX_RETRY = 9
        retry = 0
        delete_url = f"http://{self.kafkaConnectAddress}/connectors/{snowflake_connector_name}"
        post_url = f"http://{self.kafkaConnectAddress}/connectors"
        while retry < MAX_RETRY:
            try:
                logger.info(f"Delete request: {delete_url}")
                code = requests.delete(delete_url, timeout=10).status_code
                logger.info(f"Delete request returned: {code}")
                if code in (200, 201, 404):
                    break
            except Exception as e:
                logger.error(f"An exception occurred: {e}")
            logger.info("=== sleep for 10 secs to wait for kafka connect to accept connection ===")
            time.sleep(10)
            retry += 1
        if retry == MAX_RETRY:
            logger.error(f"Kafka Delete request not successful: {delete_url}")

        logger.info(f"Post HTTP request to Create Connector: {post_url}")
        r = requests.post(post_url, json=config, headers=self.httpHeader)
        logger.info(f"Connector Name:{snowflake_connector_name} POST Response:{r.status_code}")
        if not r.ok:
            logger.error(
                f"Failed creating connector:{snowflake_connector_name} due to:{r.reason} and HTTP response_code:{r.status_code}"
            )
            time.sleep(30)
            logger.info(f"Retrying POST request for connector:{snowflake_connector_name}")
            r = requests.post(post_url, json=config, headers=self.httpHeader)
            logger.info(f"Connector Name:{snowflake_connector_name} POST Response:{r.status_code}")
            if not r.ok:
                raise RuntimeError(f"Failed to create connector:{snowflake_connector_name}")
        getConnectorResponse = requests.get(post_url)
        logger.info(
            f"Get Connectors status:{getConnectorResponse.status_code}, response:{getConnectorResponse.content}"
        )
