"""E2E soak test: continuous ingestion under sustained channel-invalidation,
connector-recreate, and tasks.max-rebalance chaos.

Exercises the recovery path fixed in #1476 under sustained load. A throttled
multi-partition round-robin producer keeps data flowing into the connector's
put() while three side threads run in parallel:

  * channel invalidation: fires SYSTEM$STREAMING_CHANNEL_INVALIDATE against a
    random partition's channel every 5-120 s
  * connector recreate: DELETE + POST the connector every 2-5 min (mirrors
    test_kc_recreate_chaos)
  * tasks.max rebalance: PUT /connectors/{name}/config with a new tasks.max
    value every 60-180 s so Connect rebalances and channels move between
    tasks (this requires multiple partitions to be meaningful -- with one
    partition there's only ever one active task)

A background watchdog polls the connector status throughout the soak and
records any task ever seen in FAILED state; the test asserts the list is
empty at the end.

When the soak ends we stop the chaos first, keep producing for a short quiet
period so any tail-end invalidation can surface as an appendRow failure (and
trigger the reactive recovery path), then stop producing, drain, and assert
no gaps or duplicates per partition. The drain timeout is non-fatal -- the
integrity assertions still run on the partial result so a backlog (perf
issue) never masks a data bug (correctness issue).
"""

import copy
import json
import logging
import os
import random
import threading
import time
import urllib.error
import urllib.request

import pytest
import requests

from lib.config_migration import V4_CONFIG_TEMPLATE
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
SOAK_DURATION_SECONDS = 60 * 60 * 5

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

# tasks.max rebalance chaos: how often we PUT a new tasks.max via the Connect
# REST API to force a group rebalance and reassign partition-channels across
# tasks. Picks a value from TASKS_MAX_CHOICES different from the current one
# each cycle; with NUM_PARTITIONS partitions a 1 <-> 2 <-> 4 cycle covers
# fan-out (one task owns all channels) and fan-in (every task owns one).
# Set REBALANCE_CHAOS_ENABLED to False to skip rebalances for an A/B baseline.
REBALANCE_CHAOS_ENABLED = True
REBALANCE_INTERVAL_MIN = 60
REBALANCE_INTERVAL_MAX = 180
TASKS_MAX_CHOICES = (1, 2, 4)

# Number of topic partitions. Must be >= max(TASKS_MAX_CHOICES) for the
# rebalance chaos to actually move channels between tasks (Connect won't
# assign the same partition to multiple tasks, so extra tasks would just be
# idle if NUM_PARTITIONS < tasks.max).
NUM_PARTITIONS = 4

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
    # Initial tasks.max; the rebalance chaos thread will PUT new values during
    # the soak. Start at the lowest choice so the first rebalance event is a
    # scale-up (more interesting than a no-op).
    "tasks.max": str(TASKS_MAX_CHOICES[0]),
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    # Any unhandled error must surface as a task failure -- the whole point
    # of the soak is to prove recovery never escapes to the framework.
    "errors.tolerance": "none",
    "errors.log.enable": "true",
    "snowflake.validation": "client_side",
}


class _MultiPartitionProducer:
    """Continuous JSON producer that round-robins records across partitions.

    We need every partition to host data so that tasks.max rebalances
    actually move channels between tasks -- with a single partition there's
    only ever one active task regardless of tasks.max. records_produced is
    the global monotonically increasing count (matches RecordProducer's
    contract); per-partition record content is still uniquely identifiable
    by (partition, kafka_offset) in record_metadata.
    """

    def __init__(self, driver, topic: str, num_partitions: int):
        self._driver = driver
        self._topic = topic
        self._num_partitions = num_partitions
        self.records_produced = 0
        self._next_partition = 0
        self._stop_event = threading.Event()
        self._thread = None

    def _send_batch(self, batch_size: int):
        partition = self._next_partition
        self._next_partition = (self._next_partition + 1) % self._num_partitions
        records = []
        for _ in range(batch_size):
            self.records_produced += 1
            records.append(json.dumps({"number": str(self.records_produced)}).encode())
        self._driver.sendBytesData(self._topic, records, [], partition)

    def send(self, n: int):
        """Send n records to the next partition in the round-robin sequence."""
        self._send_batch(n)

    def start_continuous(self, batch_size: int, interval: float):
        self._stop_event.clear()

        def _produce():
            while not self._stop_event.is_set():
                self._send_batch(batch_size)
                self._stop_event.wait(interval)

        self._thread = threading.Thread(target=_produce, daemon=True)
        self._thread.start()
        logger.info(
            f"Started multi-partition producer (batch_size={batch_size}, "
            f"interval={interval}s, partitions={self._num_partitions})"
        )

    def stop_continuous(self, timeout: float = 5):
        if self._thread is not None:
            self._stop_event.set()
            self._thread.join(timeout=timeout)
            self._thread = None
            logger.info(
                f"Stopped multi-partition producer (total: {self.records_produced})"
            )


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
    """Return (total_count, total_seconds) for put-duration across all tasks.

    With tasks.max rebalance chaos there can be 1..N task MBeans live at any
    point; we want the aggregate cross-task put() time, not a single task's.
    A Jolokia wildcard read returns one entry per matching MBean, each with a
    Codahale Timer that's already converted to seconds. total_seconds is the
    sum of count*mean across tasks -- that's the right thing to compare
    against the wall-clock window for the put-fraction assertion (a task that
    started late only got a fraction of the wall clock to invoke put()).

    Note: MetricsJmxReporter registers MBeans with the uppercased connector
    name (see SnowflakeSinkTask wiring), so we have to match that casing.
    """
    mbean = (
        f"snowflake.kafka.connector:connector={connector_name.upper()},"
        f"task=*,category=task,name=put-duration"
    )
    value = _read_jolokia(mbean)
    # Wildcard reads return {mbean_name: attrs}; single-match reads return
    # attrs directly. Normalise to a dict-of-attrs.
    if not value:
        return 0, 0.0
    first_val = next(iter(value.values()))
    if not isinstance(first_val, dict):
        entries = {mbean: value}
    else:
        entries = value
    total_count = 0
    total_seconds = 0.0
    for attrs in entries.values():
        count = int(attrs["Count"])
        mean = float(attrs["Mean"])
        total_count += count
        total_seconds += count * mean
    return total_count, total_seconds


class _SharedConfig:
    """Mutable config + lock so rebalance and restart chaos can coordinate.

    Rebalance chaos PUTs new tasks.max values via the Connect REST API.
    Restart chaos does DELETE+POST and must re-POST with the latest config,
    otherwise it would silently undo the rebalance chaos changes.
    """

    def __init__(self, initial: dict):
        self._lock = threading.Lock()
        self._config = dict(initial)

    def snapshot(self) -> dict:
        with self._lock:
            return copy.deepcopy(self._config)

    def update(self, changes: dict) -> dict:
        """Apply *changes* to the shared config and return a fresh snapshot.

        Kwargs aren't used because Kafka Connect config keys contain dots
        (e.g. tasks.max), which aren't valid Python identifiers.
        """
        with self._lock:
            self._config.update(changes)
            return copy.deepcopy(self._config)


def _start_restart_chaos(driver, name_salt, unsalted_name, shared_config, stop_event):
    """Background thread that recreates (DELETE + POST) the connector at
    random RESTART_INTERVAL_MIN..MAX second intervals. Mirrors the chaos
    pattern in test_kc_recreate_chaos: driver.createConnector handles the
    delete-then-post sequence with retries.

    Reads the latest config snapshot from *shared_config* on every iteration
    so that any tasks.max change made by the rebalance thread survives the
    recreate.

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
                current_config = shared_config.snapshot()
                driver.createConnector(
                    name_salt=name_salt,
                    unsalted_name=unsalted_name,
                    config_template=current_config,
                )
                counter[0] += 1
                last_restart_time[0] = time.monotonic()
                logger.info(
                    f"Connector recreate #{counter[0]} fired (after {delay:.1f}s, "
                    f"tasks.max={current_config.get('tasks.max')})"
                )
            except Exception as e:
                logger.warning(f"Recreate attempt failed (continuing soak): {e}")

    thread = threading.Thread(target=_loop, daemon=True)
    thread.start()
    return thread, last_restart_time, counter


def _start_random_invalidator(
    driver, credentials, table_name, topic, num_partitions, stop_event
):
    """Background thread that invalidates a random partition's channel at
    INVALIDATION_INTERVAL_MIN..MAX-second intervals.

    Returns (thread, counter_list) -- counter_list[0] holds the number of
    successful invalidations so the test can assert the recovery path was
    actually exercised.
    """
    counter = [0]

    def _loop():
        while not stop_event.is_set():
            delay = random.uniform(INVALIDATION_INTERVAL_MIN, INVALIDATION_INTERVAL_MAX)
            if stop_event.wait(delay):
                return
            partition = random.randrange(num_partitions)
            try:
                invalidate_channel(
                    driver, credentials, table_name, topic, partition=partition
                )
                counter[0] += 1
                logger.info(
                    f"Invalidation #{counter[0]} fired on partition={partition} "
                    f"(after {delay:.1f}s)"
                )
            except Exception as e:
                logger.warning(f"Invalidation attempt failed (continuing soak): {e}")

    thread = threading.Thread(target=_loop, daemon=True)
    thread.start()
    return thread, counter


def _start_rebalance_chaos(driver, connector_name, shared_config, stop_event):
    """Background thread that periodically PUTs a new tasks.max via the
    Connect REST API to force a group rebalance.

    Each cycle picks a value from TASKS_MAX_CHOICES different from the
    current one, updates the shared config (so restart chaos picks up the
    new value too), and PUTs /connectors/{name}/config. Connect responds 200
    once the config is accepted; the rebalance + channel reassignment
    happens asynchronously after that.

    Returns (thread, counter_list).
    """
    counter = [0]
    base_url = f"http://{driver.kafkaConnectAddress}/connectors/{connector_name}/config"

    def _pick_next(current: int) -> int:
        candidates = [t for t in TASKS_MAX_CHOICES if t != current]
        return random.choice(candidates) if candidates else current

    def _loop():
        while not stop_event.is_set():
            delay = random.uniform(REBALANCE_INTERVAL_MIN, REBALANCE_INTERVAL_MAX)
            if stop_event.wait(delay):
                return
            try:
                current = int(shared_config.snapshot()["tasks.max"])
                new_tasks_max = _pick_next(current)
                new_config = shared_config.update({"tasks.max": str(new_tasks_max)})
                r = requests.put(
                    base_url,
                    headers=driver.httpHeader,
                    data=json.dumps(new_config),
                    timeout=30,
                )
                if r.ok:
                    counter[0] += 1
                    logger.info(
                        f"Rebalance #{counter[0]}: tasks.max {current} -> "
                        f"{new_tasks_max} (PUT returned {r.status_code}, "
                        f"after {delay:.1f}s)"
                    )
                else:
                    logger.warning(
                        f"PUT /config returned {r.status_code}: {r.text[:200]}"
                    )
            except Exception as e:
                logger.warning(f"Rebalance attempt failed (continuing soak): {e}")

    thread = threading.Thread(target=_loop, daemon=True)
    thread.start()
    return thread, counter


def _start_task_failure_watchdog(driver, connector_name, stop_event, interval=5):
    """Background thread that polls GET /connectors/{name}/status and records
    any task ever observed in FAILED state.

    Transient non-RUNNING states (UNASSIGNED during rebalance, missing status
    while the recreate thread is between DELETE and POST) are *not* failures
    and are ignored. Only state == "FAILED" is recorded -- that's the only
    state from which Connect can't recover on its own and is the symptom
    #1476 is meant to prevent.

    Returns (thread, failures_list). Each entry is a dict with keys
    {timestamp, task_id, state, trace} so the assertion message can point at
    exactly which task died and why.
    """
    failures = []
    seen_failures = set()  # de-dupe repeated polls of the same failure

    def _loop():
        while not stop_event.is_set():
            try:
                status = driver.get_connector_status(connector_name)
                if status is not None:
                    for task in status.get("tasks", []):
                        state = task.get("state")
                        task_id = task.get("id")
                        if state == "FAILED":
                            trace = (task.get("trace") or "")[:1000]
                            # Key the dedup on (id, first-line-of-trace) so a
                            # task that fails -> restarts -> fails again with
                            # a different cause gets recorded twice.
                            key = (task_id, trace.split("\n", 1)[0])
                            if key not in seen_failures:
                                seen_failures.add(key)
                                failures.append(
                                    {
                                        "timestamp": time.time(),
                                        "task_id": task_id,
                                        "state": state,
                                        "trace": trace,
                                    }
                                )
                                logger.error(
                                    f"Watchdog: task {task_id} entered FAILED "
                                    f"state. Trace: {trace[:300]}"
                                )
            except Exception as e:
                logger.debug(f"Watchdog poll failed (continuing): {e}")
            stop_event.wait(interval)

    thread = threading.Thread(target=_loop, daemon=True)
    thread.start()
    return thread, failures


@pytest.mark.soak
@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
def test_soak_with_chaos(
    driver, credentials, name_salt, create_connector, wait_for_rows
):
    """Soak test for #1476: high-rate ingestion with random invalidations,
    periodic connector recreates, and tasks.max rebalances.

    Steps:
    1. Bring up the connector against a NUM_PARTITIONS-partition topic and
       send one record so the partition-0 channel exists -- this also lets
       invalidate_channel pytest.skip cleanly if
       SYSTEM$STREAMING_CHANNEL_INVALIDATE is not available on the account.
    2. Start the multi-partition round-robin producer at throttled throughput.
    3. Start side threads: random-partition channel invalidation, connector
       recreate (DELETE+POST, mirrors test_kc_recreate_chaos), tasks.max
       rebalance (PUT /config), and task-failure watchdog.
    4. Soak for SOAK_DURATION_SECONDS, then stop the chaos threads but keep
       the producer running for POST_CHAOS_QUIET_SECONDS so any tail-end
       invalidation can surface as an appendRow failure (and trigger #1476's
       reactive recovery) while data is still flowing.
    5. Drain: wait until row count catches up to records_produced. If the
       drain times out (e.g. chaos throttled channels for too long and we
       accumulated a backlog we can't burn down in DRAIN_TIMEOUT_SECONDS),
       we still run the integrity checks on whatever did land -- a backlog
       is a perf issue, but duplicates or gaps would mean #1476's recovery
       path corrupted data.
    6. Assert per-partition offset integrity: no duplicates ((partition,
       offset) is unique), no gaps (max-min+1 == count per partition), and
       min offset == 0 (no records lost from the head). Drain timeout (if
       any) is re-raised at the end so the test still fails.
    7. Assert the watchdog never observed a task in FAILED state.
    """
    topic = f"test_soak_with_chaos{name_salt}"
    table_name = topic
    driver.createTopics(topic, partitionNum=NUM_PARTITIONS, replicationNum=1)

    connector = create_connector(v4_config=CONNECTOR_CONFIG)
    driver.wait_for_connector_running(connector.name)
    shared_config = _SharedConfig(connector.config)

    producer = _MultiPartitionProducer(driver, topic, NUM_PARTITIONS)

    # Pre-flight: send one record so the partition-0 channel exists, then
    # probe both SYSTEM$STREAMING_CHANNEL_INVALIDATE (skips test if
    # unavailable) and the Jolokia put-duration MBean (fails test if --jmx
    # wasn't enabled). Doing both now means we fail/skip cleanly before
    # spinning up the soak threads. Pre-flight covers only partition 0; the
    # remaining partitions will get their channels opened as the round-robin
    # producer starts up.
    producer.send(1)
    wait_for_rows(table_name, 1, connector_name=connector.name)
    invalidate_channel(driver, credentials, table_name, topic, partition=0)
    # The MBean only exists once SnowflakeSinkTask.start() has registered it.
    put_count_before, _ = _get_put_duration_stats(connector.name)
    logger.info(
        f"Pre-flight OK: invalidation worked, JMX put-duration MBean reachable "
        f"(count={put_count_before}); starting soak"
    )

    # Two stop events so chaos can be wound down independently of the
    # watchdog -- we want "no task failure" monitoring to cover the entire
    # test (quiet period + drain included), not just the chaos window.
    chaos_stop_event = threading.Event()
    watchdog_stop_event = threading.Event()
    invalidator_thread, invalidation_count = _start_random_invalidator(
        driver, credentials, table_name, topic, NUM_PARTITIONS, chaos_stop_event
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
            driver, name_salt, unsalted_name, shared_config, chaos_stop_event
        )
        logger.info(
            f"Restart chaos enabled (interval "
            f"{RESTART_INTERVAL_MIN}-{RESTART_INTERVAL_MAX}s)"
        )
    else:
        logger.info("Restart chaos disabled (RESTART_CHAOS_ENABLED=false)")

    rebalance_thread = None
    rebalance_count = [0]
    if REBALANCE_CHAOS_ENABLED:
        rebalance_thread, rebalance_count = _start_rebalance_chaos(
            driver, connector.name, shared_config, chaos_stop_event
        )
        logger.info(
            f"Rebalance chaos enabled (interval "
            f"{REBALANCE_INTERVAL_MIN}-{REBALANCE_INTERVAL_MAX}s, choices "
            f"{TASKS_MAX_CHOICES})"
        )
    else:
        logger.info("Rebalance chaos disabled (REBALANCE_CHAOS_ENABLED=false)")

    watchdog_thread, task_failures = _start_task_failure_watchdog(
        driver, connector.name, watchdog_stop_event
    )

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
        # producer goes idle. The watchdog keeps running through the quiet
        # period and the drain that follows -- "no task failures" should
        # hold for the *entire* test, not just the chaos window.
        logger.info(
            f"Stopping chaos; producer will keep running for "
            f"{POST_CHAOS_QUIET_SECONDS}s to flush any tail-end invalidation "
            f"through the reactive recovery path"
        )
        chaos_stop_event.set()
        invalidator_thread.join(timeout=5)
        if restart_thread is not None:
            restart_thread.join(timeout=30)  # createConnector can be slow to abort
        if rebalance_thread is not None:
            rebalance_thread.join(timeout=30)
        time.sleep(POST_CHAOS_QUIET_SECONDS)
    finally:
        # Idempotent cleanup so an interrupt during the soak or the quiet
        # period still leaves no dangling threads. join() on an already
        # joined thread returns immediately. The watchdog is left running
        # here; it'll be stopped explicitly after the drain so any task
        # failure during the drain phase is still captured.
        chaos_stop_event.set()
        invalidator_thread.join(timeout=5)
        if restart_thread is not None:
            restart_thread.join(timeout=30)
        if rebalance_thread is not None:
            rebalance_thread.join(timeout=30)
        producer.stop_continuous()

    soak_elapsed = time.monotonic() - soak_start
    records_produced = producer.records_produced
    # The put-duration timer is registered on SnowflakeSinkTask.start() and
    # gets a fresh MetricRegistry on every recreate, so its Count reflects
    # only the time since the last restart. Scope the fraction accordingly.
    # _get_put_duration_stats already sums Count*Mean across all task MBeans,
    # so total_put_seconds is the cross-task aggregate.
    put_count_after, total_put_seconds = _get_put_duration_stats(connector.name)
    if last_restart_time[0] is not None:
        put_window = max(time.monotonic() - last_restart_time[0], 1.0)
        window_label = f"since last recreate {put_window:.1f}s ago"
    else:
        put_window = soak_elapsed
        window_label = f"whole soak {put_window:.1f}s"
    put_fraction = total_put_seconds / put_window
    put_mean_seconds = total_put_seconds / put_count_after if put_count_after else 0.0
    logger.info(
        f"Soak complete in {soak_elapsed:.1f}s: produced {records_produced} records "
        f"({records_produced / soak_elapsed:.0f} rec/s), "
        f"{invalidation_count[0]} invalidations fired during soak "
        f"(plus 1 pre-flight), {restart_count[0]} connector recreates, "
        f"{rebalance_count[0]} tasks.max rebalances"
    )
    logger.info(
        f"JMX put-duration ({window_label}): {put_count_after} invocations across "
        f"all live task MBeans, mean={put_mean_seconds * 1000:.1f} ms, "
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

    # Drain is done -- stop the watchdog now so the task-failure list is
    # frozen before the assertion below reads it.
    watchdog_stop_event.set()
    watchdog_thread.join(timeout=10)
    _assert_task_running(driver, connector.name)

    # -- Integrity assertions: per-partition contiguity --
    # Kafka offsets are per-partition (each partition starts at 0), and with
    # round-robin production every partition gets data. We assert: (a) no
    # duplicates across the whole table -- (partition, offset) must be
    # globally unique -- and (b) no gaps within each partition -- offsets
    # 0..N must be contiguous. These hold regardless of whether the drain
    # caught up; they're the actual #1476 correctness property.
    cur = driver.snowflake_conn.cursor()
    total_rows, distinct_keys = cur.execute(
        f"SELECT count(*), count(DISTINCT "
        f"  record_metadata:partition::int || '/' || "
        f"  record_metadata:offset::int) "
        f'FROM "{table_name}"'
    ).fetchone()
    per_partition = cur.execute(
        f"SELECT record_metadata:partition::int AS p, "
        f"  count(*), min(record_metadata:offset::int), "
        f"  max(record_metadata:offset::int) "
        f'FROM "{table_name}" '
        f"GROUP BY p ORDER BY p"
    ).fetchall()

    logger.info(
        f"Final stats: total_rows={total_rows}, distinct_keys={distinct_keys}, "
        f"records_produced={records_produced}, per_partition={per_partition}"
    )

    assert total_rows > 0, "No rows landed in Snowflake during the soak"
    assert distinct_keys == total_rows, (
        f"Duplicates detected: {total_rows} rows but only {distinct_keys} "
        f"distinct (partition, offset) keys "
        f"({total_rows - distinct_keys} duplicates)"
    )
    sum_per_partition = sum(row[1] for row in per_partition)
    assert sum_per_partition == total_rows, (
        f"Per-partition row counts sum to {sum_per_partition} but "
        f"count(*)={total_rows}; partition grouping is inconsistent"
    )
    # Every partition should have received at least one record via the
    # round-robin producer; gaps in the partition set itself would mean the
    # producer or topic configuration is broken.
    observed_partitions = {row[0] for row in per_partition}
    assert observed_partitions == set(range(NUM_PARTITIONS)), (
        f"Expected data on partitions {set(range(NUM_PARTITIONS))} but saw "
        f"{observed_partitions}; the round-robin producer is misconfigured"
    )
    for partition, count, min_off, max_off in per_partition:
        assert min_off == 0, (
            f"Partition {partition}: expected min offset 0, got {min_off} "
            f"(head of the partition is missing)"
        )
        assert max_off - min_off + 1 == count, (
            f"Partition {partition} offset gap: count={count} but offset "
            f"range covers {max_off - min_off + 1} ({min_off}..{max_off}); "
            f"missing {(max_off - min_off + 1) - count} offsets"
        )
    # Stronger assertion -- only reachable if drain succeeded. When the drain
    # times out we surface the original AssertionError instead.
    if drain_failure is None:
        assert total_rows == records_produced, (
            f"Row count mismatch: produced {records_produced} records but found "
            f"{total_rows} rows in {table_name}"
        )
    # Only assert this when the soak is long enough that zero firings would
    # be statistically suspicious. For SOAK <= ~5x the mean interval, getting
    # zero firings is plausible noise (a 60 s sanity-check soak with
    # uniform[5, 120] s delays has a ~38 % chance of firing nothing), and a
    # flaky soak test is worse than a slightly weaker assertion. The
    # pre-flight already proves the invalidator code path itself works; this
    # assertion is a thread-liveness check, not a statistical bound.
    expected_invalidations = SOAK_DURATION_SECONDS / (
        (INVALIDATION_INTERVAL_MIN + INVALIDATION_INTERVAL_MAX) / 2
    )
    if expected_invalidations >= 5:
        assert invalidation_count[0] > 0, (
            f"No invalidations fired during the {SOAK_DURATION_SECONDS}s soak "
            f"(expected ~{expected_invalidations:.0f}) -- recovery path was "
            f"not exercised; the invalidator thread is likely dead"
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

    # No task may have entered FAILED state at any point during the soak --
    # all recovery from invalidations, recreates, and rebalances must stay
    # contained inside the connector/SDK and never escape to Connect.
    assert not task_failures, (
        f"{len(task_failures)} task failure(s) observed during the soak:\n"
        + "\n".join(
            f"  - t={f['timestamp']:.0f} task={f['task_id']} "
            f"state={f['state']}: {f['trace']}"
            for f in task_failures
        )
    )

    # The chaos and integrity checks all passed -- if the drain timed out
    # earlier, the test still failed (we just held the AssertionError until
    # after the integrity checks so a drain failure can't mask a data bug).
    if drain_failure is not None:
        raise drain_failure
