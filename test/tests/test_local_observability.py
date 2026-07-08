"""E2E test for local observability (SNOW-3136119).

Verifies that, with ``snowflake.streaming.metrics.prometheus.enable=true``, the connector turns on
the Snowpipe Streaming SDK's Prometheus endpoint and that the expected SDK metric names are present
in the scraped output, without regressing ingestion.

This test is forced to run first (see ``pytest_collection_modifyitems`` in ``conftest.py``): the SDK
initializes its metrics server once per Kafka Connect worker JVM, on the first client creation, so
this connector must be the first SDK client for its ``prometheus.enable`` config to take effect.

The connector JMX metrics (``flush-duration``, ``records-appended``) are covered by the in-process
Java IT ``LocalObservabilityIT``; they are not re-checked here.
"""

import logging
import os
import time
import urllib.error
import urllib.request

import pytest

from lib.config_migration import V4_CONFIG_TEMPLATE
from lib.utils import RecordProducer

logger = logging.getLogger(__name__)

NUM_RECORDS = 100
PROM_PORT = 50000
# The connector (and thus the SDK metrics endpoint) runs in the Connect-worker container; the
# test-runner reaches it by service name (kafka for apache, kafka-connect for confluent).
PROM_HOST = os.environ.get("KAFKA_CONNECT_HOST", "kafka")
EXPECTED_SDK_METRICS = [
    "output_payload_rows",
    "sf_api_success_latency_ms",
    "output_buffer_delay_ms",
]


def _connector_config(topic: str) -> dict:
    return {
        **V4_CONFIG_TEMPLATE,
        "tasks.max": "1",
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
        "snowflake.enable.schematization": "false",
        "topics": topic,
        "snowflake.streaming.metrics.prometheus.enable": "true",
        "snowflake.streaming.metrics.prometheus.port": str(PROM_PORT),
        "snowflake.streaming.metrics.prometheus.host": "0.0.0.0",
    }


@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
def test_prometheus_metrics_exposed(
    driver, create_table, create_topics, create_connector, wait_for_rows
):
    """The SDK Prometheus endpoint exposes the expected metric names after ingestion."""
    base_name = "local_observability"
    table = create_table(
        base_name, columns="(record_metadata variant)", cleanup_topic=False
    )
    topic = create_topics([base_name], with_tables=False)[0]

    connector = create_connector(v4_config=_connector_config(topic))
    driver.wait_for_connector_running(connector.name)

    RecordProducer(driver, topic).send(NUM_RECORDS)
    wait_for_rows(table.name, NUM_RECORDS, connector_name=connector.name)
    logger.info("No-regression check passed: %d rows in Snowflake", NUM_RECORDS)

    prom_url = f"http://{PROM_HOST}:{PROM_PORT}/metrics"
    body = None
    last_exc = None
    for attempt in range(10):
        try:
            with urllib.request.urlopen(prom_url, timeout=10) as resp:
                body = resp.read().decode("utf-8")
            break
        except urllib.error.URLError as exc:
            last_exc = exc
            logger.info(
                "Prometheus scrape attempt %d/10 failed (%s); retrying in 3s",
                attempt + 1,
                exc,
            )
            time.sleep(3)

    if body is None:
        pytest.fail(
            f"Could not reach Prometheus endpoint at {prom_url} after 10 attempts: {last_exc}. "
            f"This test runs first so its connector is the first SDK client and should bootstrap "
            f"the SDK metrics server from the connector's prometheus.enable config; verify that "
            f"{PROM_HOST}:{PROM_PORT} is reachable on the compose network."
        )

    logger.info("Prometheus scrape returned %d bytes from %s", len(body), prom_url)
    missing = [m for m in EXPECTED_SDK_METRICS if m not in body]
    assert not missing, (
        f"Expected SDK metric(s) not found in Prometheus output: {missing}\n"
        f"Scraped URL: {prom_url}\n"
        f"First 2000 chars of body:\n{body[:2000]}"
    )
    logger.info(
        "All expected SDK metrics found in Prometheus output: %s", EXPECTED_SDK_METRICS
    )
