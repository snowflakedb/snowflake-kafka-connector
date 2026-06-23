"""
E2E test for client recreation on transient 404 (bulk-channel-status path).

Uses mitmproxy to inject HTTP 404 on :bulk-channel-status requests, triggering
InvalidClientError in the SDK (mirroring the customer's envoy NR scenario in
SNOW-3670537). Verifies the connector recreates the client via the offset-fetch
path (BatchOffsetFetcher) and delivers all records without data loss.

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
# Dedicated config (distinct connector/topic/table name) so this test does not collide with the
# 410 test, which derives its names from client_recreation.json and runs in the same session.
CONFIG_FILE = "client_recreation_404.json"


def _send_records(driver, topic: str, count_per_partition: int):
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
def test_client_recreation_on_bulk_channel_status_404(
    driver,
    name_salt,
    create_table,
    wait_for_rows,
    mitmproxy,
):
    """
    Verify that the connector recovers from SDK client invalidation triggered by
    a transient HTTP 404 on :bulk-channel-status (the preCommit / offset-fetch path).

    This is the SNOW-3670537 customer scenario: a brief envoy routing disruption
    returns 404 on bulk-channel-status, the Rust SDK marks the client permanently
    invalid, and without the fix the connector logs InvalidClientError forever
    with no recovery and no task failure.

    Phases:
    1. Steady state: send records, verify they land.
    2. Fault: inject 404 on :bulk-channel-status for ~15 s (several preCommit
       cycles). The SDK marks the client invalid; BatchOffsetFetcher detects
       InvalidClientError and recreates the client.
    3. Heal: disable 404 injection.
    4. Verify: all records (pre-fault + post-heal) arrive in Snowflake; connector
       is RUNNING with no failed tasks.
    """
    if mitmproxy is None:
        pytest.skip("mitmproxy control API not reachable")

    unsalted_name = CONFIG_FILE.split(".")[0]
    connector_name = f"{unsalted_name}{name_salt}"
    topic = connector_name

    # --- Setup ---
    table = create_table(
        unsalted_name,
        columns='(record_metadata variant, "partition" varchar, "seq" varchar, "key" varchar)',
    )
    driver.createTopics(topic, partitionNum=NUM_PARTITIONS, replicationNum=1)
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

    # --- Phase 2: Inject 404 on bulk-channel-status ---
    logger.info("Phase 2: Enabling 404 injection on :bulk-channel-status")
    mitmproxy.reset_counters()
    mitmproxy.enable_404_bulk_channel_status()

    # Hold the fault for ~15 s so several preCommit cycles fire while the client
    # is invalid. BatchOffsetFetcher's new recreation logic detects InvalidClientError
    # and recreates the client in the background; the connector continues running.
    sleep(15)

    # --- Phase 3: Heal ---
    logger.info("Phase 3: Disabling 404 injection")
    mitmproxy.disable_404_bulk_channel_status()

    proxy_status = mitmproxy.get_status()
    injected = proxy_status["injected_404_bcs_count"]
    logger.info("Proxy status after fault window: %s", proxy_status)
    assert injected > 0, (
        f"No 404 responses were injected on :bulk-channel-status — the proxy is "
        f"not intercepting streaming API traffic. Proxy status: {proxy_status}"
    )
    logger.info(
        "Confirmed: %d 404 responses injected on :bulk-channel-status", injected
    )

    # --- Phase 4: Post-heal ingestion + verify recovery ---
    logger.info("Phase 4: Sending post-heal records")
    _send_records(driver, topic, RECORDS_PER_PARTITION)

    # EXACT: 300 pre-fault + 300 post-heal = 600, with no duplicates. wait_for_rows defaults to
    # at_least=False, so it returns only on count == 600 and fails if the count overshoots --
    # which would expose duplicate delivery during recovery.
    expected_total = RECORDS_PER_PARTITION * NUM_PARTITIONS * 2
    logger.info("Waiting for exactly %d total rows", expected_total)
    wait_for_rows(
        table.name,
        expected_total,
        connector_name=connector_name,
        timeout=180,
        max_consecutive_failures=20,
    )

    status = driver.get_connector_status(connector_name)
    assert status["connector"]["state"] == "RUNNING", (
        f"Connector not RUNNING after recovery: {status}"
    )
    failed_tasks = driver.get_failed_tasks(connector_name)
    assert len(failed_tasks) == 0, f"Tasks failed after recovery: {failed_tasks}"

    logger.info(
        "Test passed: exactly %d rows recovered after 404-on-bulk-channel-status injection",
        expected_total,
    )
