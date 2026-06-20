"""E2E soak test: continuous ingestion under sustained channel-invalidation
and connector-recreate chaos.

Exercises the recovery path fixed in #1476 under sustained load. A throttled
RecordProducer.start_continuous keeps data flowing into the connector's put()
while one side thread fires SYSTEM$STREAMING_CHANNEL_INVALIDATE every 10-30 s
and a second side thread (DELETE + POST) recreates the connector every 2-5
minutes, mirroring test_kc_recreate_chaos. When the soak ends we stop the
chaos first, keep producing for a short quiet period so any tail-end
invalidation can surface as an appendRow failure (and trigger the reactive
recovery path), then stop producing, drain, and assert no gaps or duplicates
across the full offset range. The drain timeout is non-fatal -- the integrity
assertions still run on the partial result so a backlog (perf issue) never
masks a data bug (correctness issue).
"""

import json
import logging
import os
import random
import threading
import time
import urllib.error
import urllib.request

import pytest

from lib.config_migration import V4_CONFIG_TEMPLATE
from lib.utils import RecordProducer
from tests.test_channel_invalidation import (
    _assert_task_running,
    invalidate_channel,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Test-run knobs -- edit these directly per worktree for concurrent A/B runs.
# ---------------------------------------------------------------------------

# Soak duration. Set to 60 for a quick sanity check; bump to thousands of
# seconds for multi-hour endurance runs.
SOAK_DURATION_SECONDS = 60 * 60 * 1

# Channel invalidation chaos: how often the side thread calls
# SYSTEM$STREAMING_CHANNEL_INVALIDATE during the soak. Spread uniformly over a
# wide range so the cadence isn't tied to a specific e2e ingestion lag -- a
# slower backend shouldn't be able to fail this test just by lagging behind.
INVALIDATION_INTERVAL_MIN = 5
INVALIDATION_INTERVAL_MAX = 120

# Recreate-style chaos: DELETE + POST the connector at random intervals so
# we also exercise the task lifecycle (close -> open -> channel reopen).
# Mirrors test_kc_recreate_chaos's createConnector(name_salt, ...) pattern.
# Set RESTART_CHAOS_ENABLED to False to skip recreates for an A/B baseline.
RESTART_CHAOS_ENABLED = True
RESTART_INTERVAL_MIN = 120
RESTART_INTERVAL_MAX = 300

# Producer pacing. The chaos-free put()->Snowflake ceiling on a typical test
# account is ~95 k rec/s, but under our chaos density (one invalidation or
# recreate every ~18 s, each stalling the channel for ~25 s of recovery) the
# effective ceiling collapses to ~24 k rec/s. Producing at ~50 k rec/s grew
# an unbounded backlog over multi-hour soaks. 10 k rec/s leaves a comfortable
# margin below the chaos-throttled ceiling so the backlog stays bounded while
# still keeping put() busy (~10-20 % of wall-clock per JMX put-duration, well
# above the 1 % floor below).
PRODUCER_BATCH_SIZE = 1000
PRODUCER_INTERVAL = 0.1

# Drain budget after the producer stops. Sized to cover the worst transient
# backlog we expect to see (one invalidation window's worth at the producer
# rate plus a safety margin) and still finish in well under a minute under
# normal conditions; the escape-hatch assertions below will run regardless.
DRAIN_TIMEOUT_SECONDS = 600

# Quiet period between stopping chaos and stopping the producer. The fix from
# #1476 is reactive: recovery fires when the next appendRow throws after the
# channel goes invalid. If invalidation chaos fires within ~SDK flush interval
# of the producer stop, the resulting STALE_CONTINUATION_TOKEN failure can
# arrive *after* the producer pipeline has gone idle -- at which point the
# connector observes ERR_CHANNEL_MUST_BE_REOPENED via the once-per-second
# status poll but never triggers recovery (no new appendRow to fail). We hit
# exactly this in a 60-min run where invalidation #179 fired 2 s before the
# producer stop and the drain hung for 10 minutes with 63 k records orphaned.
# Stopping chaos first and keeping the producer running for this many seconds
# guarantees the last invalidation's effect has time to surface as an
# appendRow exception while data is still flowing through the task.
POST_CHAOS_QUIET_SECONDS = 60

# Jolokia HTTP-JMX bridge is mounted into the kafka-connect JVM by
# run_tests.sh when invoked with --jmx; on apache it lives in the `kafka`
# container, on confluent in `kafka-connect`. KAFKA_CONNECT_HOST is set
# accordingly by the docker-compose overrides.
JOLOKIA_HOST = os.environ.get("KAFKA_CONNECT_HOST", "kafka-connect")
JOLOKIA_PORT = 8778
JOLOKIA_BASE_URL = f"http://{JOLOKIA_HOST}:{JOLOKIA_PORT}/jolokia"

# Lower bound for the fraction of soak wall-clock the task should spend in
# put(). The whole point of the soak is to keep #1476's recovery path hot,
# so we want non-trivial put() activity, but we deliberately set this well
# below the expected 10-20 % steady-state so a chaos-heavy window that
# starves put() (e.g. lots of back-to-back recreates) still fails loudly.
PUT_TIME_FRACTION_MIN = 0.01

CONNECTOR_CONFIG = {
    **V4_CONFIG_TEMPLATE,
    "topics": "SNOWFLAKE_TEST_TOPIC",
    "tasks.max": "1",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    # Any unhandled error must surface as a task failure -- the whole point
    # of the soak is to prove recovery never escapes to the framework.
    "errors.tolerance": "none",
    "errors.log.enable": "true",
    "snowflake.validation": "client_side",
}


def _read_jolokia(mbean: str) -> dict:
    """Fetch all attributes of a JMX MBean via Jolokia. Returns the value map.

    Raises RuntimeError if Jolokia is unreachable or returns a non-200 status.
    """
    url = f"{JOLOKIA_BASE_URL}/read/{mbean}"
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:
            data = json.loads(resp.read())
    except (urllib.error.URLError, OSError) as e:
        raise RuntimeError(
            f"Cannot reach Jolokia at {JOLOKIA_BASE_URL} ({e}). "
            f"Re-run with run_tests.sh --jmx to enable the Jolokia agent."
        )
    if data.get("status") != 200:
        raise RuntimeError(f"Jolokia error reading {mbean}: {data}")
    return data["value"]


def _get_put_duration_stats(connector_name: str) -> tuple[int, float]:
    """Return (count, mean_seconds) for the task's put-duration Codahale Timer.

    The JmxReporter is configured with convertDurationsTo(SECONDS), so Mean is
    already in seconds. count * mean approximates total time spent in put().

    Note: the connector's MetricsJmxReporter registers the MBean with the
    uppercased connector name (see SnowflakeSinkTask wiring), so we have to
    match that casing in the lookup.
    """
    mbean = (
        f"snowflake.kafka.connector:connector={connector_name.upper()},"
        f"task=task-0,category=task,name=put-duration"
    )
    value = _read_jolokia(mbean)
    return int(value["Count"]), float(value["Mean"])


def _start_restart_chaos(
    driver, name_salt, unsalted_name, config, stop_event
):
    """Background thread that recreates (DELETE + POST) the connector at
    random RESTART_INTERVAL_MIN..MAX second intervals. Mirrors the chaos
    pattern in test_kc_recreate_chaos: driver.createConnector handles the
    delete-then-post sequence with retries.

    Returns (thread, last_restart_time_holder, counter). last_restart_time
    is a 1-element list holding the monotonic timestamp of the most recent
    successful recreate (None if no restart has happened yet); we use it
    later to scope the put-duration metric, which resets on every restart.
    """
    counter = [0]
    last_restart_time = [None]

    def _loop():
        while not stop_event.is_set():
            delay = random.uniform(RESTART_INTERVAL_MIN, RESTART_INTERVAL_MAX)
            if stop_event.wait(delay):
                return
            try:
                driver.createConnector(
                    name_salt=name_salt,
                    unsalted_name=unsalted_name,
                    config_template=config,
                )
                counter[0] += 1
                last_restart_time[0] = time.monotonic()
                logger.info(
                    f"Connector recreate #{counter[0]} fired (after {delay:.1f}s)"
                )
            except Exception as e:
                logger.warning(f"Recreate attempt failed (continuing soak): {e}")

    thread = threading.Thread(target=_loop, daemon=True)
    thread.start()
    return thread, last_restart_time, counter


def _start_random_invalidator(
    driver, credentials, table_name, topic, stop_event, partition=0
):
    """Background thread that fires invalidations at random 10-30 s intervals.

    Returns (thread, counter_list) -- counter_list[0] holds the number of
    successful invalidations so the test can assert the recovery path was
    actually exercised.
    """
    counter = [0]

    def _loop():
        while not stop_event.is_set():
            delay = random.uniform(
                INVALIDATION_INTERVAL_MIN, INVALIDATION_INTERVAL_MAX
            )
            if stop_event.wait(delay):
                return
            try:
                invalidate_channel(
                    driver, credentials, table_name, topic, partition=partition
                )
                counter[0] += 1
                logger.info(f"Invalidation #{counter[0]} fired (after {delay:.1f}s)")
            except Exception as e:
                logger.warning(f"Invalidation attempt failed (continuing soak): {e}")

    thread = threading.Thread(target=_loop, daemon=True)
    thread.start()
    return thread, counter


@pytest.mark.soak
@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
def test_soak_with_chaos(
    driver, credentials, name_salt, create_connector, wait_for_rows
):
    """Soak test for #1476: high-rate ingestion with random invalidations
    and periodic connector recreates.

    Steps:
    1. Bring up the connector and send one record so the channel exists --
       this also lets invalidate_channel pytest.skip cleanly if
       SYSTEM$STREAMING_CHANNEL_INVALIDATE is not available on the account.
    2. Start RecordProducer.start_continuous at throttled throughput.
    3. Start a side thread that invalidates the channel every 10-30 s.
    4. Start a side thread that recreates (DELETE+POST) the connector every
       2-5 minutes -- mirrors test_kc_recreate_chaos.
    5. Soak for SOAK_DURATION_SECONDS, then stop the chaos threads but keep
       the producer running for POST_CHAOS_QUIET_SECONDS so any tail-end
       invalidation can surface as an appendRow failure (and trigger #1476's
       reactive recovery) while data is still flowing.
    6. Drain: wait until row count catches up to records_produced. If the
       drain times out (e.g. chaos throttled the channel for too long and we
       accumulated a backlog we can't burn down in DRAIN_TIMEOUT_SECONDS),
       we still run the integrity checks on whatever did land -- a backlog
       is a perf issue, but duplicates or gaps would mean #1476's recovery
       path corrupted data.
    7. Assert no duplicates or gaps in whatever rows did land: distinct
       offsets == count(*) and max_offset - min_offset + 1 == count(*),
       plus min_offset == 0. When the drain succeeded we additionally assert
       count(*) == records_produced and max_offset == records_produced - 1.
       Drain timeout (if any) is re-raised at the end so the test still fails.
    """
    topic = f"test_soak_with_chaos{name_salt}"
    table_name = topic
    driver.createTopics(topic, partitionNum=1, replicationNum=1)

    connector = create_connector(v4_config=CONNECTOR_CONFIG)
    driver.wait_for_connector_running(connector.name)

    producer = RecordProducer(driver, topic)

    # Pre-flight: send one record so the channel exists, then probe both
    # SYSTEM$STREAMING_CHANNEL_INVALIDATE (skips test if unavailable) and the
    # Jolokia put-duration MBean (fails test if --jmx wasn't enabled). Doing
    # both now means we fail/skip cleanly before spinning up the soak threads.
    producer.send(1)
    wait_for_rows(table_name, 1, connector_name=connector.name)
    invalidate_channel(driver, credentials, table_name, topic, partition=0)
    # The MBean only exists once SnowflakeSinkTask.start() has registered it.
    put_count_before, _ = _get_put_duration_stats(connector.name)
    logger.info(
        f"Pre-flight OK: invalidation worked, JMX put-duration MBean reachable "
        f"(count={put_count_before}); starting soak"
    )

    stop_event = threading.Event()
    invalidator_thread, invalidation_count = _start_random_invalidator(
        driver, credentials, table_name, topic, stop_event
    )
    restart_thread = None
    last_restart_time = [None]
    restart_count = [0]
    if RESTART_CHAOS_ENABLED:
        # Strip the salt to recover the original unsalted test name; the
        # create_connector fixture uses request.node.originalname under the
        # hood and we want the recreate to target the same connector identity.
        unsalted_name = connector.name[: -len(name_salt)]
        restart_thread, last_restart_time, restart_count = _start_restart_chaos(
            driver, name_salt, unsalted_name, connector.config, stop_event
        )
        logger.info(
            f"Restart chaos enabled (interval "
            f"{RESTART_INTERVAL_MIN}-{RESTART_INTERVAL_MAX}s)"
        )
    else:
        logger.info("Restart chaos disabled (RESTART_CHAOS_ENABLED=false)")

    producer.start_continuous(
        batch_size=PRODUCER_BATCH_SIZE, interval=PRODUCER_INTERVAL
    )

    soak_start = time.monotonic()
    try:
        logger.info(f"Soaking for {SOAK_DURATION_SECONDS}s...")
        time.sleep(SOAK_DURATION_SECONDS)

        # Stop chaos first but keep producing for POST_CHAOS_QUIET_SECONDS.
        # See POST_CHAOS_QUIET_SECONDS doc for why -- in short, this gives
        # any tail-end invalidation's STALE_CONTINUATION_TOKEN failure time
        # to surface as an appendRow exception (and thus trigger the #1476
        # reactive recovery) while data is still flowing, instead of leaving
        # the channel orphaned in ERR_CHANNEL_MUST_BE_REOPENED after the
        # producer goes idle.
        logger.info(
            f"Stopping chaos; producer will keep running for "
            f"{POST_CHAOS_QUIET_SECONDS}s to flush any tail-end invalidation "
            f"through the reactive recovery path"
        )
        stop_event.set()
        invalidator_thread.join(timeout=5)
        if restart_thread is not None:
            restart_thread.join(timeout=30)  # createConnector can be slow to abort
        time.sleep(POST_CHAOS_QUIET_SECONDS)
    finally:
        # Idempotent cleanup so an interrupt during the soak or the quiet
        # period still leaves no dangling threads. join() on an already
        # joined thread returns immediately.
        stop_event.set()
        invalidator_thread.join(timeout=5)
        if restart_thread is not None:
            restart_thread.join(timeout=30)
        producer.stop_continuous()

    soak_elapsed = time.monotonic() - soak_start
    records_produced = producer.records_produced
    # The put-duration timer is registered on SnowflakeSinkTask.start() and
    # gets a fresh MetricRegistry on every recreate, so its Count reflects
    # only the time since the last restart. Scope the fraction accordingly.
    put_count_after, put_mean_seconds = _get_put_duration_stats(connector.name)
    if last_restart_time[0] is not None:
        put_window = max(time.monotonic() - last_restart_time[0], 1.0)
        window_label = f"since last recreate {put_window:.1f}s ago"
    else:
        put_window = soak_elapsed
        window_label = f"whole soak {put_window:.1f}s"
    total_put_seconds = put_count_after * put_mean_seconds
    put_fraction = total_put_seconds / put_window
    logger.info(
        f"Soak complete in {soak_elapsed:.1f}s: produced {records_produced} records "
        f"({records_produced / soak_elapsed:.0f} rec/s), "
        f"{invalidation_count[0]} invalidations fired during soak "
        f"(plus 1 pre-flight), {restart_count[0]} connector recreates"
    )
    logger.info(
        f"JMX put-duration ({window_label}): {put_count_after} invocations, "
        f"mean={put_mean_seconds * 1000:.1f} ms, "
        f"total ~= {total_put_seconds:.1f}s "
        f"({put_fraction * 100:.1f}% of {put_window:.1f}s window)"
    )

    # -- Drain: wait until everything that was produced lands in Snowflake.
    # If the drain times out (e.g. chaos throttled the channel for too long
    # and we accumulated a backlog we can't burn down in DRAIN_TIMEOUT_SECONDS),
    # we still want to verify offset integrity on whatever did land -- a
    # backlog is a perf issue, but duplicates or gaps would mean #1476's
    # recovery path corrupted data. So we capture the drain failure and run
    # the integrity assertions first, then re-raise it at the end.
    logger.info(
        f"Draining: waiting for all {records_produced} records to land in {table_name}..."
    )
    drain_failure: AssertionError | None = None
    try:
        wait_for_rows(
            table_name,
            records_produced,
            connector_name=connector.name,
            timeout=DRAIN_TIMEOUT_SECONDS,
        )
    except AssertionError as e:
        drain_failure = e
        logger.warning(
            f"Drain did not complete in {DRAIN_TIMEOUT_SECONDS}s: {e}. "
            f"Running offset-integrity assertions on the partial result before failing."
        )
    _assert_task_running(driver, connector.name)

    # -- Integrity assertions: count(*), distinct offsets, min/max offset --
    cur = driver.snowflake_conn.cursor()
    total_rows, distinct_offsets, min_offset, max_offset = cur.execute(
        f"SELECT "
        f"  count(*), "
        f"  count(DISTINCT record_metadata:offset::int), "
        f"  min(record_metadata:offset::int), "
        f"  max(record_metadata:offset::int) "
        f'FROM "{table_name}"'
    ).fetchone()

    logger.info(
        f"Final stats: total_rows={total_rows}, distinct_offsets={distinct_offsets}, "
        f"min_offset={min_offset}, max_offset={max_offset}, "
        f"records_produced={records_produced}"
    )

    # Escape-hatch integrity checks: these hold regardless of whether drain
    # caught up. They prove there are no duplicates or gaps in whatever rows
    # did land, which is the property #1476 is fixing.
    assert total_rows > 0, "No rows landed in Snowflake during the soak"
    assert distinct_offsets == total_rows, (
        f"Duplicates detected: {total_rows} rows but only {distinct_offsets} "
        f"distinct offsets ({total_rows - distinct_offsets} duplicates)"
    )
    assert min_offset == 0, f"Expected min offset 0, got {min_offset}"
    assert max_offset - min_offset + 1 == total_rows, (
        f"Offset gap detected: rows={total_rows} but offset range covers "
        f"{max_offset - min_offset + 1} ({min_offset}..{max_offset}); "
        f"missing {(max_offset - min_offset + 1) - total_rows} offsets"
    )
    # Stronger assertion -- only reachable if drain succeeded. When the drain
    # times out we surface the original AssertionError instead.
    if drain_failure is None:
        assert total_rows == records_produced, (
            f"Row count mismatch: produced {records_produced} records but found "
            f"{total_rows} rows in {table_name}"
        )
        assert max_offset == records_produced - 1, (
            f"Offset range mismatch: max_offset={max_offset}, "
            f"expected {records_produced - 1} (records_produced - 1)"
        )
    assert invalidation_count[0] > 0, (
        f"No invalidations fired during the {SOAK_DURATION_SECONDS}s soak -- "
        f"recovery path was not exercised; check the invalidator thread"
    )
    assert put_count_after > 0, (
        "Task did not invoke put() during the soak -- producer/connector wiring "
        "broken or JMX timer not registered"
    )
    assert put_fraction > PUT_TIME_FRACTION_MIN, (
        f"Task only spent {put_fraction * 100:.1f}% of the soak inside put() "
        f"(threshold {PUT_TIME_FRACTION_MIN * 100:.1f}%); "
        f"raise the producer rate or batch size so #1476's recovery path "
        f"actually gets hammered"
    )

    # The chaos and integrity checks all passed -- if the drain timed out
    # earlier, the test still failed (we just held the AssertionError until
    # after the integrity checks so a drain failure can't mask a data bug).
    if drain_failure is not None:
        raise drain_failure
