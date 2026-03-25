import json
import logging
import time
from typing import Callable, Dict
import uuid
from pathlib import Path

import requests
import snowflake.connector
from confluent_kafka import (
    Consumer,
    ConsumerGroupTopicPartitions,
    KafkaError,
    Producer,
    TopicPartition,
)
from confluent_kafka.admin import AdminClient, ConfigResource, NewPartitions, NewTopic
from confluent_kafka.avro import AvroProducer

from lib.config import Profile, SnowflakeConnectorConfig


class Error(Exception):
    """Base class for test exceptions"""

    pass


class ResetAndRetry(Error):
    """Raised when we want to reset the retry count"""

    def __init__(self, msg=""):
        self.msg = msg


class RetryableError(Error):
    """Raised when we can retry"""

    def __init__(self, msg=""):
        self.msg = msg


class NonRetryableError(Error):
    """Raised when we cannot retry"""

    def __init__(self, msg=""):
        self.msg = msg


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
        self.httpHeader = {
            "Content-type": "application/json",
            "Accept": "application/json",
        }

        self.SEND_INTERVAL = 0.01  # send a record every 10 ms
        self.VERIFY_INTERVAL = 10  # verify every 10 secs
        self.MAX_RETRY = 60  # max wait time 1 min
        self.MAX_FLUSH_BUFFER_SIZE = (
            5000  # flush buffer when 5000 data was in the queue
        )

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

        self._avro_producer_config = producer_config.copy()
        self._avro_producer_config["schema.registry.url"] = schemaRegistryAddress
        # Lazy-init: Apache platform has no schema registry, so we can't
        # create the AvroProducer eagerly.
        self._avroProducer = None

        snowflake_connector_config = SnowflakeConnectorConfig.from_profile(credentials)
        self.snowflake_conn = snowflake.connector.connect(
            **snowflake_connector_config.to_dict()
        )

    @property
    def avroProducer(self):
        if self._avroProducer is None:
            self._avroProducer = AvroProducer(self._avro_producer_config)
        return self._avroProducer

    def msgSendInterval(self):
        # sleep self.SEND_INTERVAL before send the second message
        time.sleep(self.SEND_INTERVAL)

    def startConnectorWaitTime(self):
        time.sleep(10)

    def verifyWaitTime(self):
        # sleep before verifying result in SF DB
        logger.info(
            f"=== Sleep {self.VERIFY_INTERVAL} secs before verify result in Snowflake DB ==="
        )
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
                logger.error(
                    f"=== Non retryable error for {configFileName} raised ===\n{e.msg}"
                )
                raise e
            except snowflake.connector.errors.ProgrammingError as e:
                logger.error(f"Error in VerifyWithRetry for {configFileName}: {e}")
                if e.errno == 2003:
                    retryNum += 1
                    logger.warning(
                        f"=== Failed, table not created for {configFileName} ==="
                    )
                    self.verifyWaitTime()
                else:
                    raise
        if retryNum == self.MAX_RETRY:
            logger.error(f"=== Max retry exceeded for {configFileName} ===")
            raise NonRetryableError()

    def createTopics(self, topicName, partitionNum=1, replicationNum=1):
        self.adminClient.create_topics(
            [NewTopic(topicName, partitionNum, replicationNum)]
        )

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
            resources=[
                ConfigResource(restype=ConfigResource.Type.TOPIC, name=topicName)
            ]
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
                self.producer.produce(
                    topic, value=v, partition=partition, headers=headers or []
                )
                if (i + 1) % self.MAX_FLUSH_BUFFER_SIZE == 0:
                    self.producer.flush()
        else:
            for i, (k, v) in enumerate(zip(key, value, strict=True)):
                self.producer.produce(
                    topic, value=v, key=k, partition=partition, headers=headers or []
                )
                if (i + 1) % self.MAX_FLUSH_BUFFER_SIZE == 0:
                    self.producer.flush()
        self.producer.flush()

    def sendAvroSRData(
        self,
        topic,
        value,
        value_schema,
        key=None,
        key_schema="",
        partition=0,
        headers=None,
    ):
        if not key:
            for i, v in enumerate(value):
                self.avroProducer.produce(
                    topic=topic,
                    value=v,
                    value_schema=value_schema,
                    partition=partition,
                    headers=headers or [],
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

    def consume_messages_dlq(self, config, partition_no, target_dlq_offset_number):
        """

        :param config: Connector config
        :param partition_no: partition no to search for target offset
        :param target_dlq_offset_number: Target offset number to find which stops finding any more offsets in DLQ
        :return: count of offsets
        """
        dlq_topic_name = config["config"]["errors.deadletterqueue.topic.name"]
        return self.consume_messages(
            dlq_topic_name, partition_no, target_dlq_offset_number
        )

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
                    logger.warning(
                        f"Couldn't find target_offset:{target_offset} in topic:{topic_name} in 60 Seconds"
                    )
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
                    if (
                        msg.partition() == partition_no
                        and msg.offset() >= target_offset
                    ):
                        logger.info(
                            f"Reached target offset of {target_offset} for Topic:{topic_name}"
                        )
                        break
        except KafkaError as e:
            logger.error(f"Kafka error: {e}")

        return messages_consumed_count

    # returns kafka or confluent version
    def get_kafka_version(self):
        return self.testVersion

    def cleanTableStagePipe(self, topic: str):
        logger.info(f"=== Drop table {topic} ===")
        self.snowflake_conn.cursor().execute(
            "DROP TABLE IF EXISTS identifier(%s)", (topic,)
        )

        # Drop SSv2 streaming pipe (current naming convention: tableName-STREAMING)
        ssv2PipeName = f"{topic}-STREAMING"
        logger.info(f"=== Drop SSv2 pipe {ssv2PipeName} ===")
        self.snowflake_conn.cursor().execute(
            "DROP PIPE IF EXISTS identifier(%s)", (ssv2PipeName,)
        )

        logger.info("=== Done ===")

    def create_table(self, table_name: str):
        logger.info(f"=== Creating table {table_name} ===")
        self.snowflake_conn.cursor().execute(
            "CREATE TABLE IF NOT EXISTS identifier(%s) (RECORD_METADATA VARIANT)",
            (table_name,),
        )

    def drop_table(self, table_name: str):
        logger.info(f"=== Dropping table {table_name} ===")
        self.snowflake_conn.cursor().execute(
            "DROP TABLE IF EXISTS identifier(%s)", (table_name,)
        )

    def select_number_of_records(self, table_name: str) -> str | None:
        try:
            return (
                self.snowflake_conn.cursor()
                .execute("SELECT count(*) FROM identifier(%s)", (table_name,))
                .fetchone()[0]
            )
        except snowflake.connector.errors.ProgrammingError as e:
            if "does not exist or not authorized" in e.msg:
                return None
            raise

    def get_connector_status(self, connector_name: str) -> dict | None:
        """Query Kafka Connect REST API for connector and task states.

        Returns the parsed JSON from GET /connectors/{name}/status, e.g.:
        {
          "name": "...",
          "connector": {"state": "RUNNING", ...},
          "tasks": [{"id": 0, "state": "RUNNING", ...}, ...]
        }
        Returns None if the connector is not found or the request fails.
        """
        url = f"http://{self.kafkaConnectAddress}/connectors/{connector_name}/status"
        try:
            r = requests.get(url, timeout=10)
            if r.ok:
                return r.json()
            logger.debug(f"GET {url} returned {r.status_code}: {r.text[:200]}")
        except Exception as e:
            logger.debug(f"Failed to query connector status: {e}")
        return None

    def wait_for_connector_running(
        self, connector_name: str, timeout: int = 60, interval: int = 3
    ):
        """Poll until the connector and all its tasks report RUNNING state.

        Raises TimeoutError if the connector does not reach RUNNING within
        *timeout* seconds.
        """
        deadline = time.monotonic() + timeout
        while True:
            status = self.get_connector_status(connector_name)
            if status is not None:
                connector_state = status.get("connector", {}).get("state")
                tasks = status.get("tasks", [])
                if (
                    connector_state == "RUNNING"
                    and tasks
                    and all(t.get("state") == "RUNNING" for t in tasks)
                ):
                    logger.info(
                        f"Connector {connector_name} is RUNNING with "
                        f"{len(tasks)} task(s)"
                    )
                    return
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"Connector {connector_name} did not reach RUNNING state "
                    f"within {timeout}s (last status: {status})"
                )
            time.sleep(interval)

    def get_failed_tasks(self, connector_name: str) -> list:
        """Return list of FAILED tasks with their traces, or empty list."""
        status = self.get_connector_status(connector_name)
        if status is None:
            return []
        return [t for t in status.get("tasks", []) if t.get("state") == "FAILED"]

    def get_consumer_group_offset(
        self, connector_name: str, topic: str, partition: int = 0
    ) -> int | None:
        """Query the committed consumer group offset for a connector's sink task.

        Returns the committed offset, or None if no offset has been committed yet.
        """
        group_id = f"connect-{connector_name}"
        request = ConsumerGroupTopicPartitions(
            group_id, [TopicPartition(topic, partition)]
        )
        futures = self.adminClient.list_consumer_group_offsets([request])
        response = futures[group_id].result()
        for topic_partition in response.topic_partitions:
            if topic_partition.error:
                logger.error(
                    f"Error querying offset for {group_id}/{topic}[{partition}]: "
                    f"{topic_partition.error}"
                )
                return None
            return topic_partition.offset
        return None

    def restartConnector(self, connectorName):
        requestURL = (
            f"http://{self.kafkaConnectAddress}/connectors/{connectorName}/restart"
        )
        r = requests.post(requestURL, headers=self.httpHeader)
        logger.info(f"{r} restart connector")

    def restartConnectorAndTasks(self, connectorName):
        requestURL = f"http://{self.kafkaConnectAddress}/connectors/{connectorName}/restart?includeTasks=true&onlyFailed=false"
        r = requests.post(requestURL, headers=self.httpHeader)
        logger.info(f"{r} restart connector and all tasks")

    def pauseConnector(self, connectorName):
        requestURL = (
            f"http://{self.kafkaConnectAddress}/connectors/{connectorName}/pause"
        )
        r = requests.put(requestURL, headers=self.httpHeader)
        logger.info(f"{r} pause connector")

    def resumeConnector(self, connectorName):
        requestURL = (
            f"http://{self.kafkaConnectAddress}/connectors/{connectorName}/resume"
        )
        r = requests.put(requestURL, headers=self.httpHeader)
        logger.info(f"{r} resume connector")

    def deleteConnector(self, connectorName):
        requestURL = f"http://{self.kafkaConnectAddress}/connectors/{connectorName}"
        r = requests.delete(requestURL, headers=self.httpHeader)
        logger.info(f"{r} delete connector")

    def closeConnector(self, connector_name: str, *, wait_timeout: int = None):
        """Delete a connector.
        If `wait_timeout` is provided, also wait for it to fully disappear.

        The Kafka Connect DELETE endpoint returns immediately, but the worker
        shuts down the task's consumer asynchronously.  We poll until a GET
        returns 404 so the caller can safely assume no consumer is running.
        """
        base_url = f"http://{self.kafkaConnectAddress}/connectors/{connector_name}"
        logger.info(f"=== Delete connector {connector_name} ===")
        response = requests.delete(base_url, timeout=10)
        match response.ok:
            case True:
                logger.info(f"Delete response code: {response.status_code}")
            case False:
                logger.error(
                    f"Failed to delete connector {connector_name}: {response.text}"
                )

        if wait_timeout is None:
            return response.ok

        deadline = time.monotonic() + wait_timeout
        while time.monotonic() < deadline:
            status_code = requests.get(base_url, timeout=5).status_code
            if status_code == 404:
                logger.info(f"Connector {connector_name} fully removed")
                return True
            logger.debug(
                f"Connector {connector_name} still present (status {status_code}), "
                f"waiting..."
            )
            time.sleep(1)
        logging.error(
            f"Connector {connector_name} did not disappear within {wait_timeout}s"
        )
        return False

    Config = Dict[str, str]

    def createConnector(
        self,
        name_salt: str,
        *,
        # Either pass those:
        unsalted_name: str = None,
        config_template: Config = None,
        # Or those (deprecated):
        rest_request_template_filename: str = None,
        config_transform: Callable[[Config], Config] = None,
    ):
        """Creates the connector either with:
        - an unsalted name and a config template
        - a REST request template filename and an optional transform

        Returns the generated config."""

        match rest_request_template_filename:
            case None:
                assert unsalted_name is not None
                assert config_template is not None
                assert config_transform is None
                rest_request_template = {
                    "name": "SNOWFLAKE_CONNECTOR_NAME",
                    "config": config_template,
                }
            case _:
                assert unsalted_name is None
                assert config_template is None
                rest_request_template_path = (
                    Path("rest_request_template") / rest_request_template_filename
                )
                logger.info(
                    f"=== Generating connector REST request from {rest_request_template_path} ==="
                )
                unsalted_name = rest_request_template_filename.split(".")[0]
                with rest_request_template_path.open() as f:
                    rest_request_template = json.load(f)

        snowflake_connector_name = unsalted_name + name_salt
        logger.info(f"=== Creating connector: {snowflake_connector_name} ===")
        logger.info(
            f"Config template: {json.dumps(rest_request_template['config'], indent=4)}"
        )

        snowflake_topic_name = snowflake_connector_name

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

        rest_request = replace_values(
            rest_request_template,
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
                "_NAME_SALT": name_salt,
            },
        )

        if config_transform is not None:
            rest_request["config"] = config_transform(rest_request["config"])

        MAX_RETRY = 9
        retry = 0
        delete_url = (
            f"http://{self.kafkaConnectAddress}/connectors/{snowflake_connector_name}"
        )
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
            logger.info(
                "=== sleep for 3 secs to wait for kafka connect to accept connection ==="
            )
            time.sleep(3)
            retry += 1
        if retry == MAX_RETRY:
            logger.error(f"Kafka Delete request not successful: {delete_url}")

        logger.info(f"Post HTTP request to Create Connector: {post_url}")
        r = requests.post(post_url, json=rest_request, headers=self.httpHeader)
        logger.info(
            f"Connector Name:{snowflake_connector_name} POST Response:{r.status_code}"
        )
        if not r.ok:
            logger.error(
                f"Failed creating connector {snowflake_connector_name}: "
                f"{r.status_code} {r.reason}, {r.text}"
            )
            time.sleep(10)
            logger.info(
                f"Retrying POST request for connector:{snowflake_connector_name}"
            )
            r = requests.post(post_url, json=rest_request, headers=self.httpHeader)
            logger.info(
                f"Connector Name:{snowflake_connector_name} POST Response:{r.status_code}"
            )
            if not r.ok:
                raise RuntimeError(
                    f"Failed to create connector:{snowflake_connector_name}"
                )
        getConnectorResponse = requests.get(post_url)
        logger.info(
            f"Get Connectors status:{getConnectorResponse.status_code}, response:{getConnectorResponse.content}"
        )

        return rest_request
