"""E2E test for local observability (SNOW-3136119).

Verifies that, with ``snowflake.streaming.metrics.prometheus.enable=true``, the connector turns on
the Snowpipe Streaming SDK's Prometheus endpoint and that the expected SDK metric names are present
in the scraped output, without regressing ingestion.

This test is forced to run first (see ``pytest_collection_modifyitems`` in ``conftest.py``): the SDK
initializes its metrics server once per Kafka Connect worker JVM, on the first client creation, so
this connector must be the first SDK client for its ``prometheus.enable`` config to take effect.

The connector JMX metrics (``flush-duration``, ``records-appended``) are covered by the in-process
Java IT ``LocalObservabilityIT``; they are not re-checked here (the Jolokia scrape file is not
reachable from the test-runner container).
"""

import json
import logging
import os
import time
import urllib.error
import urllib.request
from pathlib import Path

import pytest

from lib.utils import RecordProducer

logger = logging.getLogger(__name__)

NUM_RECORDS = 100
CONFIG_FILE = "local_observability.json"
TEMPLATE_DIR = Path("rest_request_template")
PROM_PORT = 50000
# The connector (and thus the SDK metrics endpoint) runs in the Connect-worker container; the
# test-runner reaches it by service name (kafka for apache, kafka-connect for confluent).
PROM_HOST = os.environ.get("KAFKA_CONNECT_HOST", "kafka")
# SDK Prometheus metric names that must appear in the scraped output.
EXPECTED_SDK_METRICS = [
    "output_payload_rows",
    "sf_api_success_latency_ms",
    "output_buffer_delay_ms",
]


@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
def test_prometheus_metrics_exposed(
    driver, name_salt, create_table, create_custom_connector, wait_for_rows
):
    """The SDK Prometheus endpoint exposes the expected metric names after ingestion.

    1. Create a table + topic and start a connector with Prometheus enabled.
    2. Send NUM_RECORDS records and wait for them to land in Snowflake (no-regression check).
    3. Scrape http://{PROM_HOST}:{PROM_PORT}/metrics and assert every metric in
       EXPECTED_SDK_METRICS appears in the response body.

    Connector and table/topic teardown are handled by the fixtures.
    """
    unsalted_name = "local_observability"
    table = create_table(unsalted_name, columns="(record_metadata variant)")
    driver.createTopics(table.name, partitionNum=1, replicationNum=1)

    # Load the connector config template and enable Prometheus via its config (this test runs first,
    # so its connector is the first SDK client in the worker and bootstraps the SDK metrics server).
    base = json.loads((TEMPLATE_DIR / CONFIG_FILE).read_text())
    connector = create_custom_connector(unsalted_name, dict(base["config"]))
    driver.wait_for_connector_running(connector.name)

    RecordProducer(driver, table.name).send(NUM_RECORDS)
    wait_for_rows(table.name, NUM_RECORDS, connector_name=connector.name)
    logger.info("No-regression check passed: %d rows in Snowflake", NUM_RECORDS)

    # Scrape the Prometheus endpoint, retrying to allow the SDK metrics server time to bind.
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
