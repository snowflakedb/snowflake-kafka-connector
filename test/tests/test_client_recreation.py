"""
E2E test for client recreation on SDK pipe failover (HTTP 410).

Uses mitmproxy to inject HTTP 410 responses on Snowflake streaming API calls,
triggering SfApiPipeFailedOverError in the SDK. Verifies the connector recreates
the client, reopens channels, and delivers all records without data loss.

Requires: --with-mitmproxy flag when running run_tests.sh.
"""

import json
import logging
import os
from time import sleep

import pytest

logger = logging.getLogger(__name__)

NUM_PARTITIONS = 3
RECORDS_PER_PARTITION = 100
CONFIG_FILE = "client_recreation.json"


def _send_records(driver, topic: str, count_per_partition: int):
    """Send `count_per_partition` JSON records to each partition of the topic."""
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


@pytest.mark.fault_injection
@pytest.mark.skipif(
    not os.environ.get("MITMPROXY_CONTROL_URL"),
    reason="requires --with-mitmproxy (MITMPROXY_CONTROL_URL not set)",
)
@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
def test_client_recreation_on_pipe_failover(
    driver,
    name_salt,
    create_table,
    wait_for_rows,
    mitmproxy,
):
    """
    Verify that the connector recovers from SDK client invalidation (HTTP 410)
    and delivers all records without data loss.

    Phases:
    1. Steady state: send records, verify they land in Snowflake.
    2. Fault: enable 410 injection, send more records.
    3. Heal: disable 410 injection.
    4. Verify: all records (pre-fault + during-fault) arrive in Snowflake.
    """
    if mitmproxy is None:
        pytest.skip("mitmproxy control API not reachable")

    # The unsalted name is derived from the config filename by the driver
    unsalted_name = CONFIG_FILE.split(".")[0]  # "client_recreation"
    connector_name = f"{unsalted_name}{name_salt}"
    topic = connector_name

    # --- Setup ---
    table = create_table(
        unsalted_name,
        columns='(record_metadata variant, "partition" varchar, "seq" varchar, "key" varchar)',
    )
    driver.createTopics(topic, partitionNum=NUM_PARTITIONS, replicationNum=1)

    # The connector uses the normal SNOWFLAKE_HOST from credentials. When
    # --with-mitmproxy is active, Kafka Connect is configured with
    # HTTPS_PROXY=http://mitmproxy:8080 so both the JVM (JDBC) and the Rust
    # FFI SDK route their HTTPS traffic through the proxy transparently.
    driver.createConnector(
        name_salt=name_salt,
        rest_request_template_filename=CONFIG_FILE,
    )
    driver.wait_for_connector_running(connector_name)

    # --- Phase 1: Steady state ---
    logger.info("Phase 1: Sending records in steady state")
    _send_records(driver, topic, RECORDS_PER_PARTITION)

    expected_phase1 = RECORDS_PER_PARTITION * NUM_PARTITIONS
    wait_for_rows(table.name, expected_phase1, connector_name=connector_name)
    logger.info("Phase 1 complete: %d rows in Snowflake", expected_phase1)

    # --- Phase 2: Inject fault ---
    logger.info("Phase 2: Enabling 410 injection")
    mitmproxy.reset_counters()
    mitmproxy.enable_410()

    # Drip-feed records during the fault window. The SDK flushes async, so the
    # 410 hits during flush → client invalidated → is_locally_valid=false. But
    # this only surfaces on the NEXT appendRow call. We must keep sending records
    # so appendRow keeps being called and eventually sees the invalid client.
    for batch in range(5):
        _send_records(driver, topic, RECORDS_PER_PARTITION // 5)
        sleep(3)

    # --- Phase 3: Heal ---
    logger.info("Phase 3: Disabling 410 injection")
    mitmproxy.disable_410()

    # Verify that 410s were actually injected — if zero, the proxy isn't
    # intercepting streaming API calls and the test is not exercising the
    # client recreation path.
    proxy_status = mitmproxy.get_status()
    injected = proxy_status["injected_count"]
    logger.info("Proxy status after fault window: %s", proxy_status)
    assert injected > 0, (
        f"No 410 responses were injected by mitmproxy — the proxy is not "
        f"intercepting streaming API traffic. Proxy status: {proxy_status}"
    )
    logger.info("Confirmed: %d 410 responses injected during fault window", injected)

    # Send more records AFTER healing. The 410 during Phase 2 invalidated the
    # channel asynchronously (is_locally_valid=false). The error only surfaces
    # when the NEXT appendRow() is called — these records trigger that check,
    # which starts the client recreation + channel reopen recovery path.
    logger.info("Phase 3b: Sending post-heal records to trigger error detection")
    _send_records(driver, topic, RECORDS_PER_PARTITION)

    # --- Phase 4: Verify recovery ---
    # All records across all three phases are durably in Kafka (`producer.flush()`
    # blocks on broker ACKs). At-least-once delivery + the connector's rewind
    # semantics (async `PartitionOffsetTracker.resetAfterRecovery` after
    # `reopenChannel`) require every Kafka-committed record to eventually land in
    # Snowflake. Duplicates are allowed (hence `at_least=True`), but loss is not.
    records_sent_per_phase = RECORDS_PER_PARTITION * NUM_PARTITIONS
    expected_minimum = records_sent_per_phase * 3  # Phase 1 + Phase 2 + Phase 3b
    logger.info("Phase 4: Waiting for at least %d total rows", expected_minimum)
    # Allow more consecutive task failures — the task may restart several times
    # while the SDK propagates the 410 error and the connector recreates the client.
    wait_for_rows(
        table.name,
        expected_minimum,
        connector_name=connector_name,
        timeout=180,
        at_least=True,
        max_consecutive_failures=20,
    )

    # Verify connector is still healthy (RUNNING, no failed tasks)
    status = driver.get_connector_status(connector_name)
    assert status["connector"]["state"] == "RUNNING", (
        f"Connector not RUNNING after recovery: {status}"
    )
    failed_tasks = driver.get_failed_tasks(connector_name)
    assert len(failed_tasks) == 0, f"Tasks failed after recovery: {failed_tasks}"

    logger.info(
        "Test passed: at least %d rows recovered after 410 injection", expected_minimum
    )
