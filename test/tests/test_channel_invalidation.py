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


def _wait_for_partitions_recovery(
    driver, table_name, counts_before, partitions, timeout=120
):
    """Wait until every partition in `partitions` exceeds its own baseline.

    Two independent races make a narrower wait insufficient:

    * Waiting on the *global* row count is racy: a drip-feed to multiple
      partitions lets the healthy partitions alone push the total past the
      threshold before the invalidated partition reopens.
    * Waiting only on the *invalidated* partition is also racy. With
      ``tasks.max=1`` a single SinkTask.put() carries records for all
      partitions, so an invalid channel stalls the whole task — the healthy
      partitions stop committing too. On recovery the partitions flush across
      separate cycles, so at the instant the invalidated partition crosses its
      baseline a healthy partition can still be one cycle behind at its
      pre-recovery count. Snapshotting then reports it as "lost data".

    Poll until all partitions under test exceed their baselines, which is
    exactly the post-condition asserted below. Returns the per-partition counts
    observed once every partition has grown.
    """
    deadline = time.monotonic() + timeout
    partition_counts = {}
    while time.monotonic() < deadline:
        partition_counts = _get_partition_row_counts(driver, table_name)
        if all(
            partition_counts.get(p, 0) > counts_before.get(p, 0) for p in partitions
        ):
            return partition_counts
        time.sleep(2)
    stuck = [
        p for p in partitions if partition_counts.get(p, 0) <= counts_before.get(p, 0)
    ]
    raise AssertionError(
        f"Partition(s) {stuck} did not grow within {timeout}s: "
        f"before={ {p: counts_before.get(p, 0) for p in partitions} }, "
        f"after={ {p: partition_counts.get(p, 0) for p in partitions} }"
    )


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
    _wait_for_stall(driver, table_name, rows_before)

    # Drip-feed to ALL partitions to trigger recovery on each channel, then wait
    # until every partition exceeds its baseline. Waiting on the global row count
    # is racy: with tasks.max=1 the channels recover across separate flush cycles,
    # so the global count can cross stalled+50 while one partition is still at its
    # pre-recovery baseline. See _wait_for_partitions_recovery.
    stop_event, thread = _drip_feed_to_partitions(
        driver, topic, list(range(num_partitions))
    )
    partition_counts_after = _wait_for_partitions_recovery(
        driver,
        table_name,
        counts_before=partition_counts_before,
        partitions=list(range(num_partitions)),
    )
    stop_event.set()
    thread.join(timeout=5)
    _assert_task_running(driver, connector.name)

    # Defense-in-depth: the wait above already guarantees these.
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
    """Test 5: Invalidate one partition and verify all partitions recover.

    Sends records to each partition explicitly, invalidates only partition 1,
    and verifies every partition ends up with more data than before. Note: with
    tasks.max=1 the invalidated channel stalls the whole task, so partitions 0
    and 2 also pause until partition 1 recovers — the guarantee under test is
    no data loss on any partition after recovery, not uninterrupted ingestion.
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

    # Send to all partitions. With tasks.max=1 the invalidated partition-1
    # channel stalls the whole SinkTask.put(), so all partitions stop committing
    # until partition 1 recovers — they do not ingest independently.
    for p in range(num_partitions):
        _send_to_partition(driver, topic, 50, partition=p)
    rows_after_wave1 = int(driver.select_number_of_records(table_name))

    # Wait for the task to stall, then drip-feed to ALL partitions
    _wait_for_stall(driver, table_name, rows_after_wave1)

    stop_event, thread = _drip_feed_to_partitions(
        driver, topic, list(range(num_partitions))
    )
    # Wait until every partition (invalidated + healthy) exceeds its wave-1
    # baseline before snapshotting. See _wait_for_partitions_recovery for why a
    # narrower wait is racy under tasks.max=1.
    partition_counts_after = _wait_for_partitions_recovery(
        driver,
        table_name,
        counts_before=partition_counts_before,
        partitions=list(range(num_partitions)),
    )
    stop_event.set()
    thread.join(timeout=5)
    _assert_task_running(driver, connector.name)

    logger.info(f"Post-recovery per-partition counts: {partition_counts_after}")

    # These per-partition assertions are defense-in-depth: the wait above
    # already guarantees them. If the wait is ever narrowed, they will NOT catch
    # a regression on their own (they read the same snapshot the wait returned) —
    # the wait is the real guard.
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

    Checks the full offset range (0..max_offset) has no gaps and no duplicates.
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

    # Wait for the connector to commit every record it consumed before
    # measuring. The connector's preCommit reports SSv2's
    # latestCommittedOffsetToken + 1 as the Kafka offset that is safe to
    # commit, so when the consumer group offset reaches records_produced,
    # the streaming pipeline has finished landing every record we sent and
    # the verification queries below see a stable table state without
    # needing to retry.
    target_kafka_offset = producer.records_produced  # one past max sent offset
    deadline = time.monotonic() + 60
    committed = None
    while time.monotonic() < deadline:
        committed = driver.get_consumer_group_offset(connector.name, topic, 0)
        if committed is not None and committed >= target_kafka_offset:
            break
        time.sleep(1)
    else:
        raise AssertionError(
            f"Connector did not commit through Kafka offset {target_kafka_offset - 1} "
            f"within 60s; last consumer group offset={committed}"
        )
    logger.info(
        f"Connector caught up: consumer group offset={committed} "
        f"(target={target_kafka_offset})"
    )

    # Verify offset integrity: the full range 0..max_offset must have no gaps
    # and no duplicates. With the recordProcessed fix (SNOW-3344243) the
    # offset rewind replays all records that were in-flight during the flush
    # window, so no data is lost; SSv2's offset tokens guarantee exactly-once
    # so no record is committed twice. Read both metrics from a single SQL
    # snapshot to avoid the inter-query race that previously made two
    # back-to-back cursor.execute() calls observe different table states.
    cur = driver.snowflake_conn.cursor()
    total_rows, distinct_offsets, max_offset = cur.execute(
        f"WITH t AS (SELECT record_metadata:offset::int AS off "
        f'FROM "{table_name}") '
        f"SELECT count(*), count(DISTINCT off), max(off) FROM t"
    ).fetchone()
    logger.info(
        f"Offset check: {total_rows} total rows, {distinct_offsets} distinct offsets, "
        f"range [0..{max_offset}]"
    )

    offsets = sorted(
        row[0]
        for row in driver.snowflake_conn.cursor()
        .execute(
            f"SELECT DISTINCT record_metadata:offset::int AS off "
            f'FROM "{table_name}" ORDER BY off'
        )
        .fetchall()
    )

    # No gaps in the full range 0..max_offset
    expected_offsets = set(range(max_offset + 1))
    actual_offsets = set(offsets)
    missing = expected_offsets - actual_offsets
    assert not missing, (
        f"Missing offsets (gaps) in range [0..{max_offset}]: "
        f"{sorted(missing)[:20]}{'...' if len(missing) > 20 else ''}"
    )

    # Snowpipe Streaming v2 provides exactly-once semantics via offset tokens, so
    # the connector should never produce duplicate offsets after channel-recovery.
    assert total_rows == distinct_offsets, (
        f"Duplicate rows detected: {total_rows} total rows, "
        f"{distinct_offsets} distinct offsets ({total_rows - distinct_offsets} duplicates)"
    )


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


# ---------------------------------------------------------------------------
# Offline unit tests for _wait_for_partitions_recovery (no live account).
# ---------------------------------------------------------------------------


class _ScriptedConn:
    """Fake snowflake_conn whose per-partition count query returns a scripted
    sequence of {partition: count} snapshots. Each execute() advances one step;
    the last entry repeats. Exercises the real _get_partition_row_counts parsing.
    """

    def __init__(self, sequence):
        self._sequence = sequence
        self.calls = 0
        self._current = {}

    def cursor(self):
        return self

    def execute(self, _sql):
        self._current = self._sequence[min(self.calls, len(self._sequence) - 1)]
        self.calls += 1
        return self

    def fetchall(self):
        return [(p, c) for p, c in self._current.items()]


class _ScriptedDriver:
    """Driver stand-in exposing only snowflake_conn, for the recovery-wait helpers."""

    def __init__(self, sequence):
        self.snowflake_conn = _ScriptedConn(sequence)

    @property
    def calls(self):
        return self.snowflake_conn.calls


def test_wait_for_partitions_recovery_polls_past_lagging_healthy_partition(monkeypatch):
    """Regression (#1512 follow-up): the snapshot must not be taken the instant the
    invalidated partition recovers.

    Replays the exact sequence observed in the failing AZURE run: at the moment
    partition 1 (invalidated) crosses its baseline, healthy partition 2 is still
    one flush cycle behind at its pre-recovery count. The old single-partition
    wait returned here and the test asserted partition 2 had "lost data". The
    multi-partition wait must keep polling until every partition exceeds baseline.
    """
    counts_before = {0: 100, 1: 100, 2: 100}
    driver = _ScriptedDriver(
        [
            {0: 210, 1: 210, 2: 100},  # partition 1 recovered; 2 lags one cycle
            {0: 210, 1: 210, 2: 210},  # partition 2's next flush lands
        ]
    )
    # Pin the clock so the deadline can't depend on real wall-time.
    monkeypatch.setattr(time, "monotonic", lambda: 0.0)
    monkeypatch.setattr(time, "sleep", lambda _seconds: None)

    result = _wait_for_partitions_recovery(
        driver, "t", counts_before=counts_before, partitions=[0, 1, 2]
    )

    # The racy first snapshot ({2: 100}) must NOT have been accepted.
    assert driver.calls >= 2, "returned on the first poll, before partition 2 grew"
    for p in (0, 1, 2):
        assert result[p] > counts_before[p]


def test_wait_for_partitions_recovery_times_out_naming_stuck_partition(monkeypatch):
    """If a partition genuinely never grows, the helper must fail with a clear
    message naming the stuck partition — not hang, and not the misleading
    'lost data' assertion the old code produced."""
    counts_before = {0: 100, 1: 100, 2: 100}
    driver = _ScriptedDriver([{0: 210, 1: 210, 2: 100}])  # partition 2 never moves

    clock = {"t": 0.0}
    monkeypatch.setattr(time, "monotonic", lambda: clock["t"])
    monkeypatch.setattr(
        time, "sleep", lambda seconds: clock.__setitem__("t", clock["t"] + seconds)
    )

    with pytest.raises(AssertionError, match=r"\[2\]"):
        _wait_for_partitions_recovery(
            driver, "t", counts_before=counts_before, partitions=[0, 1, 2], timeout=10
        )
