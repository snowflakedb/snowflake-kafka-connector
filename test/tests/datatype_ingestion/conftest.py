"""Shared fixtures for data-type ingestion tests.

Provides two infrastructure patterns:
  1. Batch connector (``ingest`` fixture) — module-scoped, creates one connector
     per ingestion mode subscribing to all topics from TABLE_SPECS. Used by
     test_type_compatibility.py for fast, uniform type testing.
  2. Per-test connector (``typed_table`` + ``create_typed_connector``) — function-
     scoped, one connector per test. Used by test_unsupported_types.py for types
     that crash streaming channels.
"""

import json
import logging
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from uuid import uuid4

import pytest
import requests
from confluent_kafka import Consumer as KafkaConsumer, KafkaError

from lib.config_migration import v4_config_to_v3

logger = logging.getLogger(__name__)

TEMPLATE_DIR = Path("rest_request_template")
BASE_TEMPLATE = "datatype_ingestion.json"


# ---------------------------------------------------------------------------
# Table specs for batch connector — every test_type_compatibility test must
# have its (test_id, col_ddl) entry here.
# ---------------------------------------------------------------------------

TABLE_SPECS = [
    # Numeric
    ("dt_number", "NUMBER"),
    ("dt_numscale", "NUMBER(10,2)"),
    ("dt_float", "FLOAT"),
    ("dt_fltspec", "FLOAT"),
    # String & binary
    ("dt_varchar", "VARCHAR"),
    ("dt_varchar10", "VARCHAR(10)"),
    ("dt_binary", "BINARY"),
    # Logical
    ("dt_boolean", "BOOLEAN"),
    ("dt_boolcoerce", "BOOLEAN"),
    # Date & time
    ("dt_date", "DATE"),
    ("dt_time", "TIME"),
    ("dt_ts_ntz", "TIMESTAMP_NTZ"),
    ("dt_ts_ntzep", "TIMESTAMP_NTZ"),
    ("dt_ts_ltz", "TIMESTAMP_LTZ"),
    ("dt_ts_tz", "TIMESTAMP_TZ"),
    # Semi-structured
    ("dt_variant", "VARIANT"),
    ("dt_object", "OBJECT"),
    ("dt_array", "ARRAY"),
    # NULL — one per supported type
    ("dt_null_number", "NUMBER"),
    ("dt_null_float", "FLOAT"),
    ("dt_null_varchar", "VARCHAR"),
    ("dt_null_boolean", "BOOLEAN"),
    ("dt_null_date", "DATE"),
    ("dt_null_time", "TIME"),
    ("dt_null_ts_ntz", "TIMESTAMP_NTZ"),
    ("dt_null_ts_ltz", "TIMESTAMP_LTZ"),
    ("dt_null_ts_tz", "TIMESTAMP_TZ"),
    ("dt_null_variant", "VARIANT"),
    ("dt_null_object", "OBJECT"),
    ("dt_null_array", "ARRAY"),
    # Cross-type mismatch
    ("dt_xtype_str_num", "NUMBER"),
    ("dt_xtype_num_bool", "BOOLEAN"),
    ("dt_xtype_obj_str", "VARCHAR"),
    ("dt_xtype_arr_num", "NUMBER"),
]


# ---------------------------------------------------------------------------
# Result & DLQ reader
# ---------------------------------------------------------------------------


@dataclass
class IngestResult:
    """What happened when values were sent through the connector pipeline."""

    values: list
    """Column values that landed in the Snowflake table (offset order)."""

    dlq_count: int = 0
    """Number of records routed to the Dead Letter Queue."""

    dlq_errors: list = field(default_factory=list)
    """Error messages extracted from DLQ record headers."""

    connector_error: str | None = None
    """Connector task failure trace, if any."""


class DlqReader:
    """Reads from a shared DLQ topic and indexes messages by source topic."""

    def __init__(self, bootstrap_servers, dlq_topic):
        self._consumer = KafkaConsumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": f"dlq-reader-{uuid4().hex[:8]}",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": "false",
            }
        )
        self._consumer.subscribe([dlq_topic])
        self._by_topic: dict[str, list] = {}

    def drain(self, timeout=2.0):
        """Read all currently available messages from the DLQ topic."""
        while True:
            msg = self._consumer.poll(timeout)
            if msg is None:
                break
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.warning("DLQ consumer error: %s", msg.error())
                continue
            source = self._header(msg, "__connect.errors.topic") or "unknown"
            self._by_topic.setdefault(source, []).append(msg)

    def count(self, topic):
        return len(self._by_topic.get(topic, []))

    def errors(self, topic):
        return [
            self._header(m, "__connect.errors.exception.message") or ""
            for m in self._by_topic.get(topic, [])
        ]

    @staticmethod
    def _header(msg, key):
        for k, v in msg.headers() or []:
            if k == key:
                return v.decode("utf-8") if isinstance(v, bytes) else v
        return None

    def close(self):
        self._consumer.close()


# ---------------------------------------------------------------------------
# Connector helpers
# ---------------------------------------------------------------------------


def _replace_values(obj, replacements):
    if isinstance(obj, dict):
        return {k: _replace_values(v, replacements) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_replace_values(item, replacements) for item in obj]
    if isinstance(obj, str):
        for old, new in replacements.items():
            obj = obj.replace(old, new)
    return obj


def _build_config(
    driver, connector_name, topics_csv, mode_salt, ingestion_mode, extra_config=None
):
    base = json.loads((TEMPLATE_DIR / BASE_TEMPLATE).read_text())
    config = _replace_values(
        base,
        {
            "SNOWFLAKE_HOST": driver.credentials.make_url(),
            "SNOWFLAKE_DATABASE": driver.credentials.database,
            "SNOWFLAKE_SCHEMA": driver.credentials.schema,
            "SNOWFLAKE_USER": driver.credentials.user,
            "SNOWFLAKE_ROLE": driver.credentials.role,
            "SNOWFLAKE_PRIVATE_KEY": driver.credentials.private_key,
            "CONFLUENT_SCHEMA_REGISTRY": driver.schemaRegistryAddress,
            "SNOWFLAKE_TEST_TOPIC": topics_csv,
            "SNOWFLAKE_CONNECTOR_NAME": connector_name,
            "_NAME_SALT": mode_salt,
        },
    )
    match ingestion_mode:
        case "v3":
            config["config"] = v4_config_to_v3(config["config"])
        case "v4-compat":
            config["config"]["snowflake.client.validation.enabled"] = "true"
    if extra_config:
        config["config"].update(extra_config)
    return config


def _register(driver, config):
    name = config["name"]
    headers = {"Content-type": "application/json", "Accept": "application/json"}
    url = f"http://{driver.kafkaConnectAddress}/connectors"
    try:
        requests.delete(f"{url}/{name}", timeout=10)
    except Exception:
        pass
    logger.info("POST connector: %s", name)
    r = requests.post(url, json=config, headers=headers)
    logger.info("POST response: %d", r.status_code)
    if not r.ok:
        time.sleep(10)
        r = requests.post(url, json=config, headers=headers)
        if not r.ok:
            raise RuntimeError(f"Failed to create {name}: {r.status_code} {r.text}")
    return name


def _delete(driver, name):
    url = f"http://{driver.kafkaConnectAddress}/connectors/{name}"
    try:
        requests.delete(url, timeout=10)
    except Exception:
        logger.warning("Failed to delete connector %s", name, exc_info=True)


# ---------------------------------------------------------------------------
# Fixtures — shared
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module", params=["v3", "v4-compat"])
def ingestion_mode(request):
    return request.param


@pytest.fixture(scope="module")
def mode_salt(session_name_salt, ingestion_mode):
    suffix = {"v3": "_v3", "v4-compat": ""}[ingestion_mode]
    return f"{session_name_salt}{suffix}"


# ---------------------------------------------------------------------------
# Fixture — batch connector (test_type_compatibility.py)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def ingest(driver, mode_salt, ingestion_mode):
    """Batch connector fixture for type compatibility tests.

    Creates all tables/topics from TABLE_SPECS, registers one connector
    (errors.tolerance=all, DLQ enabled), and yields a callable::

        result = ingest("dt_number", [42, 0, "bad"])
        # result.values  — what landed in Snowflake
        # result.dlq_count — what was rejected
        # result.dlq_errors — error messages from DLQ headers
    """
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    connector_name = f"batch_{ingestion_mode.replace('-', '')}{mode_salt}"
    dlq_topic = f"dlq_batch{mode_salt}"

    # Ensure consistent timezone for timestamp tests
    driver.snowflake_conn.cursor().execute("ALTER SESSION SET TIMEZONE = 'UTC'")

    # Create all tables + topics
    topics = []
    for test_id, col_ddl in TABLE_SPECS:
        topic = f"{test_id}{mode_salt}"
        driver.snowflake_conn.cursor().execute(
            f"CREATE OR REPLACE TABLE {topic} "
            f"(VALUE_COL {col_ddl}, RECORD_METADATA VARIANT)"
        )
        driver.createTopics(topic, partitionNum=1, replicationNum=1)
        topics.append(topic)
    driver.createTopics(dlq_topic, partitionNum=1, replicationNum=1)

    # Register batch connector
    config = _build_config(
        driver,
        connector_name,
        ",".join(topics),
        mode_salt,
        ingestion_mode,
        extra_config={
            "errors.tolerance": "all",
            "errors.deadletterqueue.topic.name": dlq_topic,
            "errors.deadletterqueue.topic.replication.factor": "1",
        },
    )
    _register(driver, config)
    driver.startConnectorWaitTime()

    dlq = DlqReader(bootstrap, dlq_topic)

    def _ingest(test_id, values, *, timeout=60):
        topic = f"{test_id}{mode_salt}"
        total = len(values)

        records = [json.dumps({"VALUE_COL": v}).encode() for v in values]
        keys = [json.dumps({"number": str(i)}).encode() for i in range(total)]
        driver.sendBytesData(topic, records, keys)

        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            tbl = driver.select_number_of_records(topic)
            dlq.drain(timeout=1)
            if tbl + dlq.count(topic) >= total:
                break
            if driver.get_failed_tasks(connector_name):
                break
            time.sleep(2)

        error = None
        if failed := driver.get_failed_tasks(connector_name):
            error = failed[0].get("trace", "no trace")

        rows = (
            driver.snowflake_conn.cursor()
            .execute(
                f'SELECT VALUE_COL FROM {topic} ORDER BY RECORD_METADATA:"offset"::int'
            )
            .fetchall()
        )
        return IngestResult(
            values=[r[0] for r in rows],
            dlq_count=dlq.count(topic),
            dlq_errors=dlq.errors(topic),
            connector_error=error,
        )

    try:
        yield _ingest
    finally:
        _delete(driver, connector_name)
        dlq.close()
        for test_id, _ in TABLE_SPECS:
            try:
                driver.deleteTopic(f"{test_id}{mode_salt}")
            except Exception:
                pass
        try:
            driver.deleteTopic(dlq_topic)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Fixtures — per-test connector (test_unsupported_types.py)
# ---------------------------------------------------------------------------


@pytest.fixture
def typed_table(driver, mode_salt):
    """Factory: create a Snowflake table + Kafka topic for a single test."""
    created = []

    def _create(test_id, col_ddl):
        topic = f"{test_id}{mode_salt}"
        driver.snowflake_conn.cursor().execute(
            f"CREATE OR REPLACE TABLE {topic} "
            f"(VALUE_COL {col_ddl}, RECORD_METADATA VARIANT)"
        )
        driver.createTopics(topic, partitionNum=1, replicationNum=1)
        created.append(topic)
        return topic

    try:
        yield _create
    finally:
        for t in created:
            try:
                driver.deleteTopic(t)
            except Exception:
                pass


@pytest.fixture
def standalone_ingest(driver, mode_salt, ingestion_mode, typed_table):
    """Per-test connector that returns IngestResult. For unsupported types."""

    created_connectors = []

    def _run(test_id, col_ddl, values, *, timeout=60):
        topic = typed_table(test_id, col_ddl)
        connector_name = f"{test_id}{mode_salt}"
        dlq_topic = f"dlq_{test_id}{mode_salt}"

        config = _build_config(
            driver,
            connector_name,
            topic,
            mode_salt,
            ingestion_mode,
            extra_config={
                "errors.tolerance": "all",
                "errors.deadletterqueue.topic.name": dlq_topic,
                "errors.deadletterqueue.topic.replication.factor": "1",
            },
        )
        _register(driver, config)
        created_connectors.append(connector_name)
        driver.startConnectorWaitTime()

        records = [json.dumps({"VALUE_COL": v}).encode() for v in values]
        keys = [json.dumps({"number": str(i)}).encode() for i in range(len(values))]
        driver.sendBytesData(topic, records, keys)

        deadline = time.monotonic() + timeout
        error = None
        while time.monotonic() < deadline:
            if failed := driver.get_failed_tasks(connector_name):
                error = failed[0].get("trace", "no trace")
                break
            tbl = driver.select_number_of_records(topic)
            if tbl >= len(values):
                break
            time.sleep(2)

        rows = (
            driver.snowflake_conn.cursor()
            .execute(
                f'SELECT VALUE_COL FROM {topic} ORDER BY RECORD_METADATA:"offset"::int'
            )
            .fetchall()
        )
        dlq_count = 0
        try:
            dlq_count = driver.consume_messages(dlq_topic, 0, len(values) - 1)
        except Exception:
            pass

        return IngestResult(
            values=[r[0] for r in rows],
            dlq_count=dlq_count,
            connector_error=error,
        )

    try:
        yield _run
    finally:
        for name in reversed(created_connectors):
            _delete(driver, name)
