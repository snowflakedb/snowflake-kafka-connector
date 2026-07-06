"""
E2E test for local observability (Prometheus + JMX metrics).

Verifies that, when `snowflake.streaming.metrics.prometheus.enable=true`, the
connector exposes a Prometheus endpoint on the configured port and that the
expected SDK metric names are present in the scraped output.

A second test guards JMX metrics: if a Jolokia-scrape file is present on disk
(populated by the optional `--jmx` harness flag), it asserts the connector
metric fragments `flush-duration`, `append-row-duration`, and
`records-appended` appear in at least one scraped entry.  When no file is
present the test is skipped, so it never blocks the standard suite.

Requires: connector built with Prometheus support enabled (default CI config).
"""

import glob
import json
import logging
import os
import time
import urllib.error
import urllib.request

import pytest

logger = logging.getLogger(__name__)

NUM_PARTITIONS = 3
RECORDS_PER_PARTITION = 200
CONFIG_FILE = "local_observability.json"
PROM_PORT = 50000
PROM_HOST = os.environ.get("KAFKA_CONNECT_HOST", "kafka")
# SDK Prometheus metric names that must appear in the scraped output.
EXPECTED_SDK_METRICS = [
    "output_payload_rows",
    "sf_api_success_latency_ms",
    "output_buffer_delay_ms",
]
# JMX attribute fragments exposed by the connector's TaskMetrics bean.
EXPECTED_JMX_FRAGMENTS = [
    "flush-duration",
    "append-row-duration",
    "records-appended",
]

# Glob patterns for JMX metrics files produced by an optional Jolokia scraper.
_JMX_FILE_GLOB = "/tmp/sf-metrics-*.jsonl"
_JMX_FILE_ENV = "METRICS_FILE"


def _send_records(driver, topic: str, count_per_partition: int):
    """Send `count_per_partition` JSON records to every partition of the topic."""
    for partition in range(NUM_PARTITIONS):
        values = [
            json.dumps({"partition": partition, "seq": i}).encode()
            for i in range(count_per_partition)
        ]
        keys = [
            json.dumps({"key": f"p{partition}-{i}"}).encode()
            for i in range(count_per_partition)
        ]
        driver.sendBytesData(topic, values, keys, partition=partition)
    logger.info(
        "Sent %d records (%d per partition x %d partitions)",
        count_per_partition * NUM_PARTITIONS,
        count_per_partition,
        NUM_PARTITIONS,
    )


@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
def test_prometheus_metrics_exposed(
    driver,
    name_salt,
    create_table,
    wait_for_rows,
):
    """Prometheus endpoint exposes expected SDK metric names after ingestion.

    Steps:
    1. Create table + topic, start the connector with Prometheus enabled.
    2. Send NUM_PARTITIONS * RECORDS_PER_PARTITION records.
    3. Wait until all rows land in Snowflake (no-regression check).
    4. Scrape http://localhost:{PROM_PORT}/metrics and assert every metric in
       EXPECTED_SDK_METRICS appears in the response body.
    """
    unsalted_name = CONFIG_FILE.split(".")[0]  # "local_observability"
    connector_name = f"{unsalted_name}{name_salt}"
    topic = connector_name

    table = create_table(
        unsalted_name,
        columns="(record_metadata variant)",
    )
    driver.createTopics(topic, partitionNum=NUM_PARTITIONS, replicationNum=1)

    driver.createConnector(
        name_salt=name_salt,
        rest_request_template_filename=CONFIG_FILE,
    )
    driver.wait_for_connector_running(connector_name)

    # Send records and wait for all of them to land in Snowflake.
    _send_records(driver, topic, RECORDS_PER_PARTITION)
    expected_total = RECORDS_PER_PARTITION * NUM_PARTITIONS
    wait_for_rows(table.name, expected_total, connector_name=connector_name)
    logger.info("No-regression check passed: %d rows in Snowflake", expected_total)

    # Scrape Prometheus endpoint, retrying to allow the SDK metrics server time to bind.
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
            f"Ensure snowflake.streaming.metrics.prometheus.enable=true and "
            f"the connector container is reachable."
        )

    logger.info("Prometheus scrape returned %d bytes from %s", len(body), prom_url)

    missing = [m for m in EXPECTED_SDK_METRICS if m not in body]
    assert not missing, (
        f"Expected SDK metric(s) not found in Prometheus output: {missing}\n"
        f"Scraped URL: {prom_url}\n"
        f"First 2000 chars of body:\n{body[:2000]}"
    )
    logger.info(
        "All expected SDK metrics found in Prometheus output: %s",
        EXPECTED_SDK_METRICS,
    )


@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
def test_jmx_metrics_present(
    driver,  # noqa: ARG001 — needed to satisfy session fixture chain
    name_salt,  # noqa: ARG001 — needed to satisfy name_salt fixture
):
    """JMX Jolokia scrape file contains the connector's TaskMetrics attributes.

    This test is skipped unless a Jolokia scrape file is available on disk
    (populated by the optional `--jmx` harness flag or the METRICS_FILE env
    var).  It is intentionally a soft gate: the standard CI suite does not
    run Jolokia, so absence of the file is not a failure.
    """
    # Locate the metrics file from env var or glob.
    metrics_file = os.environ.get(_JMX_FILE_ENV)
    if not metrics_file or not os.path.isfile(metrics_file):
        candidates = sorted(glob.glob(_JMX_FILE_GLOB))
        metrics_file = candidates[-1] if candidates else None

    if not metrics_file:
        pytest.skip(
            f"No JMX metrics file found (env {_JMX_FILE_ENV} unset, "
            f"no files matching {_JMX_FILE_GLOB}). "
            f"Run with --jmx flag or set {_JMX_FILE_ENV} to enable this test."
        )

    logger.info("Reading JMX metrics from %s", metrics_file)
    with open(metrics_file, encoding="utf-8") as fh:
        lines = [ln.strip() for ln in fh if ln.strip()]

    if not lines:
        pytest.skip(f"JMX metrics file {metrics_file} is empty")

    # Collect all attribute names (or raw text) from the JSONL file.
    # Each line is expected to be a JSON object; we search the raw text of
    # every line to stay resilient to schema variations across scraper versions.
    raw_content = "\n".join(lines)
    missing = [frag for frag in EXPECTED_JMX_FRAGMENTS if frag not in raw_content]
    assert not missing, (
        f"Expected JMX metric fragment(s) not found in {metrics_file}: {missing}\n"
        f"First 2000 chars of file:\n{raw_content[:2000]}"
    )
    logger.info(
        "All expected JMX metric fragments found in %s: %s",
        metrics_file,
        EXPECTED_JMX_FRAGMENTS,
    )
