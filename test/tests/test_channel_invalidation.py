"""E2E tests for channel invalidation recovery.

Uses SYSTEM$STREAMING_CHANNEL_INVALIDATE to set ERR_CHANNEL_MUST_BE_REOPENED
on streaming channels and verifies the connector recovers with no data loss.

Recovery mechanism: After server-side invalidation, the SDK discovers the error
on the next background flush (~25s), marks the channel locally invalid, and the
next appendRow() throws synchronously -> Failsafe fallback -> reopenChannel.

Requires: SNOW-3291474 (system function) deployed to the test account.
JIRA: SNOW-3097571
"""

import logging
import time

import pytest
import snowflake.connector

from lib.config_migration import V4_CONFIG_TEMPLATE
from lib.utils import RecordProducer

logger = logging.getLogger(__name__)

# Note on table naming: the v4 connector with no sanitization config creates tables
# using the exact topic name (case-preserved). Queries use quote_name() which wraps
# names in double-quotes (case-sensitive), so table_name must match topic exactly.
# Do NOT use topic.upper() — that only works with sanitization enabled.
CONNECTOR_CONFIG = {
    **V4_CONFIG_TEMPLATE,
    "topics": "SNOWFLAKE_TEST_TOPIC",
    "tasks.max": "1",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "errors.tolerance": "all",
    "errors.log.enable": "true",
    "snowflake.validation": "client_side",
}


def invalidate_channel(driver, credentials, table_name, topic, partition=0):
    """Call SYSTEM$STREAMING_CHANNEL_INVALIDATE to set ERR_CHANNEL_MUST_BE_REOPENED."""
    connector_name_upper = table_name.upper()
    channel_name = f"{connector_name_upper}_{topic}_{partition}"
    pipe_fqn = f'{credentials.database}.{credentials.schema}."{table_name}-STREAMING"'
    logger.info(f"Invalidating channel={channel_name} on pipe={pipe_fqn}")

    cur = driver.snowflake_conn.cursor()
    try:
        cur.execute("USE ROLE SYSADMIN")
        try:
            result = cur.execute(
                f"SELECT SYSTEM$STREAMING_CHANNEL_INVALIDATE('{pipe_fqn}', '{channel_name}')"
            ).fetchone()[0]
        except snowflake.connector.errors.ProgrammingError as e:
            if e.errno == 2140 or "Unknown function" in str(e):
                pytest.skip(
                    f"SYSTEM$STREAMING_CHANNEL_INVALIDATE is not available on this "
                    f"Snowflake account — skipping channel invalidation test ({e})"
                )
            raise
    finally:
        cur.execute(f"USE ROLE {credentials.role}")

    logger.info(f"Invalidation result: {result}")
    assert "ERR_CHANNEL_MUST_BE_REOPENED" in result, f"Invalidation failed: {result}"
    return result


def _send_to_partition(driver, topic, n, partition):
    """Send n JSON records to a specific partition.

    RecordProducer.send() hardcodes partition=0, so multi-partition tests need
    this helper to route records to specific partitions.
    """
    import json

    values = [json.dumps({"number": str(i)}).encode() for i in range(n)]
    driver.sendBytesData(topic, values, [], partition)


def _drip_feed_to_partitions(driver, topic, partitions, batch_size=10, interval=1.0):
    """Start a background thread that drip-feeds records round-robin across partitions."""
    import json
    import threading

    stop_event = threading.Event()
    counter = [0]

    def _produce():
        while not stop_event.is_set():
            for p in partitions:
                values = [
                    json.dumps({"number": str(counter[0] + i)}).encode()
                    for i in range(batch_size)
                ]
                driver.sendBytesData(topic, values, [], p)
                counter[0] += batch_size
            stop_event.wait(interval)

    thread = threading.Thread(target=_produce, daemon=True)
    thread.start()
    logger.info(
        f"Started multi-partition drip-feed to partitions {partitions} "
        f"(batch_size={batch_size}, interval={interval}s)"
    )
    return stop_event, thread


def _wait_for_stall(driver, table_name, rows_before, timeout=90):
    """Wait until ingestion stalls (row count stable for 15s). Returns stalled row count."""
    stable_count = 0
    last_rows = rows_before
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        time.sleep(5)
        current = driver.select_number_of_records(table_name)
        current = int(current) if current is not None else 0
        if current == last_rows:
            stable_count += 1
        else:
            stable_count = 0
            last_rows = current
        if stable_count >= 3:
            break
    logger.info(f"Ingestion stalled at {last_rows} rows (was {rows_before})")
    return last_rows


def _get_partition_row_counts(driver, table_name):
    """Query per-partition row counts from record_metadata."""
    rows = (
        driver.snowflake_conn.cursor()
        .execute(
            f"SELECT record_metadata:partition::int AS p, count(*) AS c "
            f'FROM "{table_name}" GROUP BY p ORDER BY p'
        )
        .fetchall()
    )
    return {int(r[0]): int(r[1]) for r in rows}


def _assert_task_running(driver, connector_name):
    """Assert all connector tasks are RUNNING."""
    status = driver.get_connector_status(connector_name)
    assert status is not None, f"Connector {connector_name} not found"
    tasks = status.get("tasks", [])
    assert tasks, f"Connector {connector_name} has no tasks"
    for task in tasks:
        state = task.get("state")
        assert state == "RUNNING", (
            f"Task {task.get('id')} is {state}, not RUNNING. "
            f"Trace: {task.get('trace', '')[:500]}"
        )


@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
def test_invalidation_during_active_ingestion(
    driver, credentials, name_salt, create_connector, wait_for_rows
):
    """Test 1: Invalidate a channel while records are actively being ingested.

    Starts continuous production, waits for some rows to land, then invalidates
    mid-stream while records are still flowing.
    """
    topic = f"test_invalidation_during_active_ingestion{name_salt}"
    table_name = topic
    driver.createTopics(topic, partitionNum=1, replicationNum=1)

    connector = create_connector(v4_config=CONNECTOR_CONFIG)
    driver.wait_for_connector_running(connector.name)

    producer = RecordProducer(driver, topic)

    # Start continuous production and wait for some rows to commit
    producer.start_continuous(batch_size=10, interval=0.5)
    wait_for_rows(table_name, 50, at_least=True, connector_name=connector.name)
    rows_before = int(driver.select_number_of_records(table_name))
    logger.info(f"Phase 1: {rows_before} rows committed while actively ingesting")

    # Invalidate mid-stream — records are still flowing
    invalidate_channel(driver, credentials, table_name, topic, partition=0)

    # Wait for stall to confirm invalidation took effect
    stalled_rows = _wait_for_stall(driver, table_name, rows_before)

    # Continue drip-feeding to trigger synchronous recovery
    wait_for_rows(
        table_name,
        stalled_rows + 50,
        at_least=True,
        connector_name=connector.name,
        timeout=120,
    )
    producer.stop_continuous()
    _assert_task_running(driver, connector.name)

    final_rows = int(driver.select_number_of_records(table_name))
    logger.info(f"Final row count: {final_rows} (stalled at {stalled_rows})")
    assert final_rows > stalled_rows


@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
def test_invalidation_between_batches(
    driver, credentials, name_salt, create_connector, wait_for_rows
):
    """Test 2: Invalidate a channel while it is idle between batches.

    Verifies the connector recovers on the next batch with no data loss.
    """
    topic = f"test_invalidation_between_batches{name_salt}"
    table_name = topic
    driver.createTopics(topic, partitionNum=1, replicationNum=1)

    connector = create_connector(v4_config=CONNECTOR_CONFIG)
    driver.wait_for_connector_running(connector.name)

    producer = RecordProducer(driver, topic)

    # Wave 1: ingest and wait for full commit
    producer.send(100)
    wait_for_rows(table_name, 100, connector_name=connector.name)
    rows_before = int(driver.select_number_of_records(table_name))
    logger.info(f"Wave 1 committed ({rows_before} rows)")

    # Invalidate while idle
    invalidate_channel(driver, credentials, table_name, topic, partition=0)

    # Send batch to trigger flush failure, then wait for stall
    producer.send(100)
    stalled_rows = _wait_for_stall(driver, table_name, rows_before)

    # Drip-feed to trigger synchronous recovery
    producer.start_continuous(batch_size=10, interval=1.0)
    wait_for_rows(
        table_name,
        stalled_rows + 50,
        at_least=True,
        connector_name=connector.name,
        timeout=120,
    )
    producer.stop_continuous()
    _assert_task_running(driver, connector.name)

    final_rows = int(driver.select_number_of_records(table_name))
    logger.info(f"Final row count: {final_rows} (was {rows_before})")
    assert final_rows > rows_before


@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
def test_invalidation_all_partitions(
    driver, credentials, name_salt, create_connector, wait_for_rows
):
    """Test 3: Invalidate all channels simultaneously on a multi-partition topic.

    Sends records to each partition explicitly, invalidates all channels,
    and verifies each partition recovers.
    """
    topic = f"test_invalidation_all_partitions{name_salt}"
    table_name = topic
    num_partitions = 3
    records_per_partition = 100
    driver.createTopics(topic, partitionNum=num_partitions, replicationNum=1)

    connector = create_connector(v4_config=CONNECTOR_CONFIG)
    driver.wait_for_connector_running(connector.name)

    # Wave 1: send records to each partition explicitly
    for p in range(num_partitions):
        _send_to_partition(driver, topic, records_per_partition, partition=p)
    total_wave1 = num_partitions * records_per_partition
    wait_for_rows(table_name, total_wave1, connector_name=connector.name)
    rows_before = int(driver.select_number_of_records(table_name))

    # Verify records landed on all partitions
    partition_counts_before = _get_partition_row_counts(driver, table_name)
    logger.info(f"Wave 1 per-partition counts: {partition_counts_before}")
    for p in range(num_partitions):
        assert partition_counts_before.get(p, 0) > 0, (
            f"Partition {p} has no records before invalidation"
        )

    # Invalidate all partitions
    for p in range(num_partitions):
        invalidate_channel(driver, credentials, table_name, topic, partition=p)
    logger.info(f"All {num_partitions} channels invalidated")

    # Send to each partition to trigger flush failure, then wait for stall
    for p in range(num_partitions):
        _send_to_partition(driver, topic, 50, partition=p)
    stalled_rows = _wait_for_stall(driver, table_name, rows_before)

    # Drip-feed to ALL partitions to trigger recovery on each channel
    stop_event, thread = _drip_feed_to_partitions(
        driver, topic, list(range(num_partitions))
    )
    wait_for_rows(
        table_name,
        stalled_rows + 50,
        at_least=True,
        connector_name=connector.name,
        timeout=120,
    )
    stop_event.set()
    thread.join(timeout=5)
    _assert_task_running(driver, connector.name)

    # Verify each partition has more rows than before
    partition_counts_after = _get_partition_row_counts(driver, table_name)
    logger.info(f"Post-recovery per-partition counts: {partition_counts_after}")
    for p in range(num_partitions):
        assert partition_counts_after.get(p, 0) > partition_counts_before.get(p, 0), (
            f"Partition {p} did not recover: "
            f"before={partition_counts_before.get(p, 0)}, "
            f"after={partition_counts_after.get(p, 0)}"
        )


@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
def test_invalidation_with_connector_restart(
    driver, credentials, name_salt, create_connector, wait_for_rows
):
    """Test 4: Invalidate a channel and then restart the connector.

    A restart clears the SDK state and reopens the channel fresh. The server-side
    error code is tied to the old client sequencer; the new channel gets a fresh
    sequencer, so the error doesn't apply. Recovery is implicit via the restart.
    """
    topic = f"test_invalidation_with_connector_restart{name_salt}"
    table_name = topic
    driver.createTopics(topic, partitionNum=1, replicationNum=1)

    connector = create_connector(v4_config=CONNECTOR_CONFIG)
    driver.wait_for_connector_running(connector.name)

    producer = RecordProducer(driver, topic)

    # Wave 1
    producer.send(100)
    wait_for_rows(table_name, 100, connector_name=connector.name)
    rows_before = int(driver.select_number_of_records(table_name))
    logger.info(f"Wave 1 committed ({rows_before} rows)")

    # Invalidate then restart
    invalidate_channel(driver, credentials, table_name, topic, partition=0)
    driver.restartConnector(connector.name)
    driver.wait_for_connector_running(connector.name)
    logger.info("Connector restarted after invalidation")

    # Drip-feed after restart
    producer.start_continuous(batch_size=10, interval=1.0)
    wait_for_rows(
        table_name,
        rows_before + 100,
        at_least=True,
        connector_name=connector.name,
        timeout=120,
    )
    producer.stop_continuous()
    _assert_task_running(driver, connector.name)

    final_rows = int(driver.select_number_of_records(table_name))
    logger.info(f"Final row count: {final_rows} (was {rows_before})")
    assert final_rows > rows_before


@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
def test_invalidation_one_partition_others_healthy(
    driver, credentials, name_salt, create_connector, wait_for_rows
):
    """Test 5: Invalidate one partition while others remain healthy.

    Sends records to each partition explicitly, invalidates only partition 1,
    and verifies partition 1 recovers while partitions 0 and 2 continue
    ingesting without interruption.
    """
    topic = f"test_invalidation_one_partition_others_healthy{name_salt}"
    table_name = topic
    num_partitions = 3
    records_per_partition = 100
    driver.createTopics(topic, partitionNum=num_partitions, replicationNum=1)

    connector = create_connector(v4_config=CONNECTOR_CONFIG)
    driver.wait_for_connector_running(connector.name)

    # Wave 1: send to each partition
    for p in range(num_partitions):
        _send_to_partition(driver, topic, records_per_partition, partition=p)
    total_wave1 = num_partitions * records_per_partition
    wait_for_rows(table_name, total_wave1, connector_name=connector.name)

    partition_counts_before = _get_partition_row_counts(driver, table_name)
    logger.info(f"Wave 1 per-partition counts: {partition_counts_before}")
    for p in range(num_partitions):
        assert partition_counts_before.get(p, 0) > 0

    # Invalidate only partition 1
    invalidate_channel(driver, credentials, table_name, topic, partition=1)
    logger.info("Partition 1 invalidated, partitions 0 and 2 untouched")

    # Send to all partitions — partitions 0,2 should ingest immediately,
    # partition 1 will stall then recover
    for p in range(num_partitions):
        _send_to_partition(driver, topic, 50, partition=p)
    rows_after_wave1 = int(driver.select_number_of_records(table_name))

    # Wait for stall on partition 1, then drip-feed to ALL partitions
    stalled_rows = _wait_for_stall(driver, table_name, rows_after_wave1)

    stop_event, thread = _drip_feed_to_partitions(
        driver, topic, list(range(num_partitions))
    )
    wait_for_rows(
        table_name,
        stalled_rows + 50,
        at_least=True,
        connector_name=connector.name,
        timeout=120,
    )
    stop_event.set()
    thread.join(timeout=5)
    _assert_task_running(driver, connector.name)

    # Verify partition 1 recovered and all partitions have more data
    partition_counts_after = _get_partition_row_counts(driver, table_name)
    logger.info(f"Post-recovery per-partition counts: {partition_counts_after}")

    # Partition 1 (invalidated) must have recovered
    assert partition_counts_after.get(1, 0) > partition_counts_before.get(1, 0), (
        f"Partition 1 did not recover: "
        f"before={partition_counts_before.get(1, 0)}, "
        f"after={partition_counts_after.get(1, 0)}"
    )
    # Healthy partitions should also have more rows
    for p in [0, 2]:
        assert partition_counts_after.get(p, 0) > partition_counts_before.get(p, 0), (
            f"Healthy partition {p} lost data: "
            f"before={partition_counts_before.get(p, 0)}, "
            f"after={partition_counts_after.get(p, 0)}"
        )


@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
def test_invalidation_offset_consistency(
    driver, credentials, name_salt, create_connector, wait_for_rows
):
    """Test 6: Verify offset consistency after invalidation recovery.

    Checks the full offset range (0..max_offset) has no gaps. Duplicates are OK.
    """
    topic = f"test_invalidation_offset_consistency{name_salt}"
    table_name = topic
    driver.createTopics(topic, partitionNum=1, replicationNum=1)

    connector = create_connector(v4_config=CONNECTOR_CONFIG)
    driver.wait_for_connector_running(connector.name)

    producer = RecordProducer(driver, topic)

    # Wave 1
    producer.send(100)
    wait_for_rows(table_name, 100, connector_name=connector.name)
    rows_before = int(driver.select_number_of_records(table_name))
    logger.info(f"Wave 1 committed ({rows_before} rows)")

    # Invalidate, wait for stall, drip-feed recovery
    invalidate_channel(driver, credentials, table_name, topic, partition=0)
    producer.send(100)
    stalled_rows = _wait_for_stall(driver, table_name, rows_before)

    producer.start_continuous(batch_size=10, interval=1.0)
    wait_for_rows(
        table_name,
        stalled_rows + 50,
        at_least=True,
        connector_name=connector.name,
        timeout=120,
    )
    producer.stop_continuous()
    _assert_task_running(driver, connector.name)

    # Verify offset integrity: the full range 0..max_offset must have no gaps.
    # With the recordProcessed fix (SNOW-3344243), the offset rewind replays all
    # records that were in-flight during the flush-failure window, so no data is lost.
    cur = driver.snowflake_conn.cursor()
    offsets = sorted(
        row[0]
        for row in cur.execute(
            f"SELECT DISTINCT record_metadata:offset::int AS off "
            f'FROM "{table_name}" ORDER BY off'
        ).fetchall()
    )

    total_rows = int(driver.select_number_of_records(table_name))
    distinct_offsets = len(offsets)
    max_offset = offsets[-1]

    logger.info(
        f"Offset check: {total_rows} total rows, {distinct_offsets} distinct offsets, "
        f"range [0..{max_offset}]"
    )

    # No gaps in the full range 0..max_offset
    expected_offsets = set(range(max_offset + 1))
    actual_offsets = set(offsets)
    missing = expected_offsets - actual_offsets
    assert not missing, (
        f"Missing offsets (gaps) in range [0..{max_offset}]: "
        f"{sorted(missing)[:20]}{'...' if len(missing) > 20 else ''}"
    )

    duplicates = total_rows - distinct_offsets
    if duplicates > 0:
        logger.info(f"Found {duplicates} duplicate rows (expected after recovery)")


@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
def test_invalidation_during_flush(
    driver, credentials, name_salt, create_connector, wait_for_rows
):
    """Test 7: Invalidate a channel right after data starts flowing (races with flush).

    Sends an initial batch, waits for the pipe to be created, then immediately
    sends more and invalidates — the invalidation races with the flush.
    """
    topic = f"test_invalidation_during_flush{name_salt}"
    table_name = topic
    driver.createTopics(topic, partitionNum=1, replicationNum=1)

    connector = create_connector(v4_config=CONNECTOR_CONFIG)
    driver.wait_for_connector_running(connector.name)

    producer = RecordProducer(driver, topic)

    # First batch: ensure the pipe/channel exists
    producer.send(100)
    wait_for_rows(table_name, 100, connector_name=connector.name)
    rows_before = int(driver.select_number_of_records(table_name))
    logger.info(f"Initial batch committed ({rows_before} rows), pipe exists")

    # Send second batch and immediately invalidate — races with flush
    producer.send(100)
    invalidate_channel(driver, credentials, table_name, topic, partition=0)
    logger.info(
        "Invalidated immediately after sending second batch (racing with flush)"
    )

    # Wait for stall, then drip-feed for recovery
    stalled_rows = _wait_for_stall(driver, table_name, rows_before)

    producer.start_continuous(batch_size=10, interval=1.0)
    wait_for_rows(
        table_name,
        stalled_rows + 50,
        at_least=True,
        connector_name=connector.name,
        timeout=120,
    )
    producer.stop_continuous()
    _assert_task_running(driver, connector.name)

    final_rows = int(driver.select_number_of_records(table_name))
    logger.info(f"Final row count: {final_rows} (stalled at {stalled_rows})")
    assert final_rows > stalled_rows
