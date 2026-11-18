"""Shared helpers for the server-side evolve-schema e2e tests.

With snowflake.feature.evolve.schema.enabled=true and server_side validation, the connector
sends each new value schema to the SDK's evolveSchema before appending. Columns are created with
the types the schema declares, not types guessed from JSON values.
"""

import json
import time
from pathlib import Path
from typing import Callable

import pytest
import requests

from lib.driver import KafkaDriver

TEMPLATE_DIR = Path("rest_request_template")
BASE_TEMPLATE = "datatype_ingestion.json"

# Kafka Connect logical type names, as JsonConverter writes them into the schema envelope.
TIMESTAMP_NAME = "org.apache.kafka.connect.data.Timestamp"
DECIMAL_NAME = "org.apache.kafka.connect.data.Decimal"
DATE_NAME = "org.apache.kafka.connect.data.Date"
TIME_NAME = "org.apache.kafka.connect.data.Time"

BASE_MS = 1700000000000  # 2023-11-14 22:13:20 UTC

# Error codes Snowflake answers evolve-schema with; see the GS evolve-schema endpoint.
ERR_NOT_ENABLED = "ERR_EVOLVE_SCHEMA_NOT_ENABLED"

TABLE_COLUMNS = "(RECORD_METADATA VARIANT) ENABLE_SCHEMA_EVOLUTION = TRUE"


def struct(*fields, name="Value"):
    """A Kafka Connect struct schema, as JsonConverter writes it."""
    return {"type": "struct", "name": name, "optional": False, "fields": list(fields)}


def field(field_name, type_, optional=True, **extra):
    """One struct field. `extra` carries logical-type keys such as name and parameters."""
    return {"field": field_name, "type": type_, "optional": optional, **extra}


ID_FIELD = field("ID", "int64", optional=False)


def config(
    dlq_topic: str = None,
    *,
    evolve_schema: bool = True,
    errors_tolerance: str = None,
    **overrides,
) -> dict:
    """A server_side v4 config with evolve-schema on, from the base template.

    `overrides` are written into the config last, so they can replace any key.
    """
    result = dict(json.loads((TEMPLATE_DIR / BASE_TEMPLATE).read_text())["config"])
    result["value.converter.schemas.enable"] = "true"
    result["snowflake.enable.schematization"] = "true"
    result["snowflake.validation"] = "server_side"
    result["snowflake.feature.evolve.schema.enabled"] = str(evolve_schema).lower()
    result["snowflake.compatibility.enable.column.identifier.normalization"] = "true"
    result["snowflake.streaming.validate.compatibility.with.classic"] = "false"
    if dlq_topic:
        result["errors.tolerance"] = "all"
        result["errors.deadletterqueue.topic.name"] = dlq_topic
        result["errors.deadletterqueue.topic.replication.factor"] = "1"
    if errors_tolerance:
        result["errors.tolerance"] = errors_tolerance
    result.update(overrides)
    return result


# Routes the SDK's streaming traffic through mitmproxy, so a test can inject evolve-schema faults.
MITMPROXY_OVERRIDE = {
    "snowflake.streaming.client.provider.override.map": "scheme:https,host:mitmproxy,port:8080",
}


def envelope(schema, payload) -> bytes:
    return json.dumps({"schema": schema, "payload": payload}).encode("utf-8")


def column_types(table) -> dict:
    return {row[0]: row[1] for row in table.schema()}


def nullable(table) -> dict:
    """Whether each column allows NULL, from the null? column of DESC TABLE."""
    return {row[0]: row[3] == "Y" for row in table.schema()}


# Converter settings for Schema Registry-backed records; createConnector fills in the URL.
AVRO_CONVERTER = {
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "CONFLUENT_SCHEMA_REGISTRY",
}


def start(
    driver: KafkaDriver,
    create_table: Callable,
    create_custom_connector: Callable,
    name: str,
    *,
    dlq: bool = False,
    columns: str = TABLE_COLUMNS,
    partitions: int = 1,
    **config_kwargs,
):
    """Creates the table, its topic, and a connector, all named after `name`."""
    table = create_table(name, columns=columns)
    driver.createTopics(table.name, partitionNum=partitions, replicationNum=1)
    dlq_topic = None
    if dlq:
        dlq_topic = f"dlq_{table.name.lower()}"
        driver.createTopics(dlq_topic, partitionNum=1, replicationNum=1)
    connector = create_custom_connector(name, config(dlq_topic, **config_kwargs))
    driver.startConnectorWaitTime()
    return table, connector


def send(driver: KafkaDriver, topic: str, schema, payloads, partition: int = 0):
    """Sends each payload as a JsonConverter envelope with `schema`."""
    driver.sendBytesData(
        topic, [envelope(schema, p) for p in payloads], [], partition=partition
    )


def update_config(driver: KafkaDriver, connector_name: str, new_config: dict):
    """Replaces the connector's config; Kafka Connect restarts its tasks with it."""
    url = f"http://{driver.kafkaConnectAddress}/connectors/{connector_name}/config"
    response = requests.put(url, json=new_config, timeout=30)
    assert response.ok, f"Config update failed: {response.status_code} {response.text}"


def require(client, flag: str):
    """Skips the test when an optional run_tests.sh flag's client isn't available."""
    if client is None:
        pytest.skip(f"requires run_tests.sh {flag}")
    return client


def wait_for(
    predicate: Callable[[], bool], what: str, timeout: float = 300, interval=5
):
    """Polls `predicate` until it returns true; fails with `what` after `timeout` seconds."""
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() >= deadline:
            raise AssertionError(f"Timed out after {timeout}s waiting for {what}")
        time.sleep(interval)


def wait_for_task_failure(
    driver: KafkaDriver, connector_name: str, timeout: float = 300
):
    """Waits until a task of the connector is FAILED, and returns the failed tasks."""
    failed = []

    def has_failed():
        failed[:] = driver.get_failed_tasks(connector_name)
        return bool(failed)

    wait_for(has_failed, f"a FAILED task in {connector_name}", timeout)
    return failed
