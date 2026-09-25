"""E2E soak test: continuous ingestion under sustained channel-invalidation,
connector-recreate, and tasks.max-rebalance chaos.

Exercises the connector's channel-recovery path under sustained load. A
throttled multi-partition round-robin producer keeps data flowing into the
connector's put() while several chaos sources run in parallel:

  * channel invalidation: fires SYSTEM$STREAMING_CHANNEL_INVALIDATE against a
    random partition's channel
  * connector recreate: hard DELETE + POST of the connector (mirrors
    test_kc_recreate_chaos)
  * tasks.max rebalance: PUT /connectors/{name}/config with a new tasks.max
    value so Connect rebalances and channels move between tasks (this requires
    multiple partitions to be meaningful -- with one partition there's only
    ever one active task)
  * pause/resume: PUT /pause, hold briefly, PUT /resume (graceful task
    suspension + offset commit, distinct from a recreate)
  * in-place restart: POST /restart?includeTasks=true (graceful task restart,
    distinct from the hard recreate)
  * add partitions: grow the topic mid-soak so the connector opens channels
    for brand-new partitions live

All intervals are configurable at the top of the file, and every source can be
toggled off independently for A/B baselines.

All the periodic activities -- including the task-failure watchdog -- share a
single ScheduledGenerator abstraction (wait a jittered interval, run one
action, count it, swallow-and-log errors, repeat). A ChaosOrchestrator starts
and stops the chaos sources as a group; the connector's config and lifecycle
are owned by a single-writer ConnectorManager so the recreate and rebalance
sources can't interleave conflicting REST mutations.

The soak lifecycle is expressed as nested context managers so the ordering
(and its teardown) is structural rather than hand-wired: the watchdog outlives
the producer, which outlives the chaos. When the soak ends we stop the chaos
first, keep producing for a short quiet period so any tail-end invalidation can
surface as an appendRow failure (and trigger the reactive recovery path), then
stop producing, drain, and assert no gaps or duplicates per partition. The
drain timeout is non-fatal -- the integrity assertions still run on the partial
result so a backlog (perf issue) never masks a data bug (correctness issue).
"""

import contextlib
import json
import logging
import os
import random
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Callable

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

# Pause/resume chaos: PUT /pause, hold the connector paused for a short dwell,
# then PUT /resume. Exercises graceful task suspension (offset commit on
# pause) and the resume path -- a code path distinct from delete/recreate.
# Data keeps buffering in Kafka while paused and is drained on resume.
PAUSE_RESUME_CHAOS_ENABLED = True
PAUSE_RESUME_INTERVAL_MIN = 90
PAUSE_RESUME_INTERVAL_MAX = 240
PAUSE_RESUME_DWELL_SECONDS = 10

# In-place restart chaos: POST /restart?includeTasks=true&onlyFailed=false.
# This is the *graceful* task restart path (Connect stops and restarts the
# tasks in place), as opposed to the hard DELETE+POST recreate above.
RESTART_IN_PLACE_CHAOS_ENABLED = True
RESTART_IN_PLACE_INTERVAL_MIN = 90
RESTART_IN_PLACE_INTERVAL_MAX = 240

# Add-partitions chaos: grow the topic's partition count mid-soak so the
# connector has to open channels for brand-new partitions live (and the next
# rebalance spreads them across tasks). Kafka only allows *increasing*
# partitions; we step up by ADD_PARTITIONS_STEP each cycle up to
# ADD_PARTITIONS_MAX so a long soak doesn't grow the partition count without
# bound. The producer round-robins over the current count, so new partitions
# start receiving data (from offset 0) as soon as they exist.
ADD_PARTITIONS_CHAOS_ENABLED = True
ADD_PARTITIONS_INTERVAL_MIN = 120
ADD_PARTITIONS_INTERVAL_MAX = 300
ADD_PARTITIONS_STEP = 2
ADD_PARTITIONS_MAX = 12

# How often the task-failure watchdog polls the connector status.
WATCHDOG_POLL_INTERVAL = 5

# Grace window, after chaos + drain have finished, for Connect to self-heal a
# task that was still FAILED at the last watchdog poll before we treat that
# failure as a genuine (stuck) invariant violation. Under heavy chaos a task
# can be momentarily restarting right at the end; a real recovery bug leaves it
# FAILED well past this.
WATCHDOG_SETTLE_SECONDS = 60

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

# Quiet period between stopping chaos and stopping the producer. Channel
# recovery is reactive: it fires when the next appendRow throws after the
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
# put(). The whole point of the soak is to keep the channel-recovery path hot,
# so we want non-trivial put() activity, but we deliberately set this well
# below the expected 10-20 % steady-state so a chaos-heavy window that
# starves put() (e.g. lots of back-to-back recreates) still fails loudly.
PUT_TIME_FRACTION_MIN = 0.01

# Per-chaos-source health thresholds, checked at the assertion phase so a
# silently broken chaos thread can't degrade the soak into a weaker test that
# still passes. Both checks are gated so short sanity runs don't get flaky.
#
# Liveness: only assert a source actually fired when the soak is long enough
# that zero firings would be a dead thread rather than plausible noise (a 60 s
# run with uniform[5, 120] s delays has a ~38 % chance of firing nothing).
CHAOS_LIVENESS_MIN_EXPECTED = 5
# Error rate: tolerate the odd transient blip (network, backend lag, a
# rebalance PUT racing an in-flight rebalance), but a source that fails most
# of its attempts is systematically broken (bad REST auth, rejected config)
# and its chaos isn't landing. Only judged once there are enough attempts to
# tell a systematic failure from a one-off.
CHAOS_MIN_ATTEMPTS_FOR_ERROR_CHECK = 3
CHAOS_MAX_ERROR_RATE = 0.5

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


# ---------------------------------------------------------------------------
# Scheduling: one timed-loop abstraction shared by every periodic activity.
# ---------------------------------------------------------------------------


class ScheduledGenerator:
    """A named background activity that runs *action* on a jittered schedule.

    Every chaos source and the watchdog share the same shape: wait a (usually
    random) interval, run one action, count it, swallow-and-log any error,
    repeat until stopped. This is that shape, expressed once. Adding a new
    chaos type is a new *action* callable plus one registration -- no bespoke
    thread, counter, or stop-event wiring.

    *action* may return a short detail string; when *log_each* is set it's
    appended to the per-tick INFO line (e.g. "invalidation #5: partition=2").
    Errors are counted and logged at *error_log_level* but never propagate --
    a single flaky invalidation or REST blip must not abort a multi-hour soak.
    """

    def __init__(
        self,
        name: str,
        action: Callable[[], str | None],
        interval: Callable[[], float],
        *,
        enabled: bool = True,
        join_timeout: float = 5,
        log_each: bool = True,
        error_log_level: int = logging.WARNING,
    ):
        self.name = name
        self.enabled = enabled
        self.count = 0
        self.error_count = 0
        self.last_error: Exception | None = None
        self._action = action
        self._interval = interval
        self._join_timeout = join_timeout
        self._log_each = log_each
        self._error_log_level = error_log_level
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self):
        if not self.enabled:
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name=self.name, daemon=True)
        self._thread.start()

    def _loop(self):
        # wait-then-act: the first action fires after one interval, and a stop
        # signalled during the wait exits promptly without running another
        # action.
        while not self._stop.wait(self._interval()):
            try:
                detail = self._action()
                self.count += 1
                if self._log_each:
                    suffix = f": {detail}" if detail else ""
                    logger.info(f"{self.name} #{self.count}{suffix}")
            except Exception as e:
                self.error_count += 1
                self.last_error = e
                logger.log(
                    self._error_log_level,
                    f"{self.name}: attempt failed (continuing soak): {e}",
                )

    def stop(self):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=self._join_timeout)
            self._thread = None

    def __enter__(self) -> "ScheduledGenerator":
        self.start()
        return self

    def __exit__(self, *exc) -> bool:
        self.stop()
        return False


class ChaosOrchestrator:
    """Starts a group of ScheduledGenerators together and -- as a context
    manager -- stops them together, so an interrupt mid-soak still tears down
    every chaos thread. Centralizing the enabled toggle here keeps `if
    ENABLED` checks out of the start/stop/report sites.
    """

    def __init__(self, generators: list[ScheduledGenerator]):
        self._generators = list(generators)

    def __enter__(self) -> "ChaosOrchestrator":
        for g in self._generators:
            g.start()
            logger.info(
                f"chaos: started {g.name}" if g.enabled else f"chaos: {g.name} disabled"
            )
        return self

    def __exit__(self, *exc) -> bool:
        for g in self._generators:
            g.stop()
        return False


# ---------------------------------------------------------------------------
# Connector ownership: a single writer serializes every config/lifecycle
# mutation so the recreate and rebalance chaos can't race at the REST layer.
# ---------------------------------------------------------------------------


class ConnectorManager:
    """Single-writer owner of the connector's config and lifecycle.

    Both rebalance chaos (rebalance_tasks_max) and recreate chaos (recreate)
    mutate the running connector. Serializing every mutation *together with
    its REST call* under one lock makes "the connector reflects the
    last-issued mutation" a structural invariant: the two chaos threads can't
    interleave a stale PUT /config with a recreate POST, or land their REST
    effects on Connect out of order. A mutation may block for the duration of
    the other's REST call (a recreate's DELETE+POST can take tens of seconds);
    that's intended -- overlapping connector mutations are exactly what we're
    serializing away.
    """

    def __init__(self, driver, connector, name_salt: str):
        self._driver = driver
        self.name = connector.name
        self._name_salt = name_salt
        # create_connector salts request.node.originalname; strip the salt to
        # recover the unsalted identity so the recreate targets the same
        # connector rather than spawning a fresh salted one each cycle.
        self._unsalted_name = connector.name[: -len(name_salt)]
        self._config = dict(connector.config)
        self._lock = threading.Lock()
        self._base_url = f"http://{driver.kafkaConnectAddress}/connectors/{self.name}"
        self._config_url = f"{self._base_url}/config"
        # Monotonic timestamp of the most recent successful recreate (None if
        # none yet). The put-duration timer resets on every recreate, so the
        # metric window is scoped to this.
        self.last_recreate_time: float | None = None

    def _lifecycle_request(self, method: str, path: str, params: str = ""):
        """Issue a Connect lifecycle REST call (pause/resume/restart) and raise
        on a non-2xx. These carry no config body, so -- unlike /config -- the
        response is safe to include in the error, but we still only surface the
        status code to keep the logging policy uniform.
        """
        url = f"{self._base_url}/{path}"
        if params:
            url += f"?{params}"
        r = requests.request(method, url, headers=self._driver.httpHeader, timeout=30)
        if not r.ok:
            raise RuntimeError(f"{method} /{path} returned {r.status_code}")

    def rebalance_tasks_max(self, choices) -> str:
        """Pick a tasks.max from *choices* different from the current value and
        PUT it. Returns a short detail string for the scheduler's log; raises
        on a non-2xx so the scheduler records it as an error.
        """
        with self._lock:
            current = int(self._config["tasks.max"])
            candidates = [t for t in choices if t != current]
            new = random.choice(candidates) if candidates else current
            self._config["tasks.max"] = str(new)
            r = requests.put(
                self._config_url,
                headers=self._driver.httpHeader,
                data=json.dumps(dict(self._config)),
                timeout=30,
            )
            if not r.ok:
                # Roll back the in-memory value so a later recreate doesn't
                # POST a tasks.max the server rejected. Never log r.text -- the
                # PUT body is the full config (incl. snowflake.private.key) and
                # Connect echoes it back in validation-error responses.
                self._config["tasks.max"] = str(current)
                raise RuntimeError(f"PUT /config returned {r.status_code}")
            return f"tasks.max {current} -> {new}"

    def recreate(self) -> str:
        """DELETE + POST the connector with the latest config (driver.
        createConnector handles the delete-then-post sequence with retries).
        Returns a short detail string for the scheduler's log.
        """
        with self._lock:
            tasks_max = self._config["tasks.max"]
            self._driver.createConnector(
                name_salt=self._name_salt,
                unsalted_name=self._unsalted_name,
                config_template=dict(self._config),
            )
            self.last_recreate_time = time.monotonic()
            return f"tasks.max={tasks_max}"

    def restart_in_place(self) -> str:
        """Graceful in-place task restart (POST /restart?includeTasks=true),
        distinct from the hard DELETE+POST recreate. Raises on non-2xx.
        """
        with self._lock:
            self._lifecycle_request(
                "POST", "restart", "includeTasks=true&onlyFailed=false"
            )
            return "includeTasks=true"

    def pause_resume(self, dwell_seconds: float) -> str:
        """Pause the connector, hold it paused for *dwell_seconds*, then resume.

        The dwell sleeps *outside* the lock so a pause doesn't block recreate/
        rebalance for its whole window; a recreate landing mid-pause simply
        rebuilds a running connector and the resume below becomes a no-op. The
        resume runs in a finally so an unexpected error during the dwell still
        leaves the connector running; ensure_resumed() after chaos stops is the
        final safety net (e.g. if resume raced an in-progress rebalance).
        """
        with self._lock:
            self._lifecycle_request("PUT", "pause")
        try:
            time.sleep(dwell_seconds)
        finally:
            with self._lock:
                self._lifecycle_request("PUT", "resume")
        return f"paused for {dwell_seconds:.0f}s"

    def ensure_resumed(self):
        """Best-effort resume, called once after chaos stops so the connector
        is never left paused going into the drain. Chaos is quiesced by now, so
        no rebalance can 409 the resume; any error is logged, not raised.
        """
        with self._lock:
            try:
                self._lifecycle_request("PUT", "resume")
            except Exception as e:
                logger.warning(f"ensure_resumed: {e}")


class TaskFailureWatchdog:
    """Tracks connector task failures, telling persistent connector-owned
    failures apart from transient, self-healing, or self-inflicted ones.

    The core soak invariant is that channel recovery never escapes to the
    framework as a *stuck* FAILED task. Two kinds of FAILED observations are
    NOT invariant violations and must not fail the soak:

      * self-healing blips -- a task that goes FAILED and then returns to a
        non-FAILED state on its own (Connect's normal task restart/backoff, or
        a task briefly rejected mid-reconfiguration). We only count a failure
        if it is *still* FAILED at the end, after a settle window.

      * framework guardrails we trip with our own chaos rather than the
        connector -- notably TooManyTasksException, which Kafka Connect's
        tasks.max.enforce raises when the rebalance chaos lowers tasks.max
        while a restart is (re)starting the task set generated under the old,
        higher tasks.max. That's a Connect-side race with our own chaos, not a
        recovery defect, so it is allowlisted as benign.

    The scheduling is a ScheduledGenerator wrapped around poll(). Transient
    non-RUNNING states (UNASSIGNED mid-rebalance, a missing connector between a
    recreate's DELETE and POST) are ignored; only FAILED is tracked. Call
    confirm_persistent() once the soak has quiesced to get the failures that
    actually violate the invariant.
    """

    # Substrings identifying framework guardrails tripped by our own tasks.max
    # chaos (not connector recovery bugs), matched against the task's trace.
    BENIGN_TRACE_MARKERS = ("TooManyTasksException",)

    def __init__(self, driver, connector_name: str):
        self._driver = driver
        self._connector_name = connector_name
        # task_id -> the current, not-yet-recovered failure episode.
        self._active: dict = {}
        # Every failure episode ever opened, for end-of-soak reporting.
        self.history: list[dict] = []
        # Episodes that later returned to a non-FAILED state (self-healed).
        self.recovered: list[dict] = []

    @classmethod
    def _is_benign(cls, trace: str) -> bool:
        return any(marker in trace for marker in cls.BENIGN_TRACE_MARKERS)

    def poll(self):
        status = self._driver.get_connector_status(self._connector_name)
        if status is None:
            # Connector absent (e.g. between a recreate's DELETE and POST) --
            # can't judge task state, so leave the active set untouched.
            return
        for task in status.get("tasks", []):
            task_id = task.get("id")
            if task.get("state") == "FAILED":
                self._record_failed(task_id, (task.get("trace") or "")[:2000])
            elif task_id in self._active:
                # Task left FAILED on its own -> this episode self-healed.
                episode = self._active.pop(task_id)
                episode["recovered_to"] = task.get("state")
                self.recovered.append(episode)
                logger.info(
                    f"Watchdog: task {task_id} recovered from FAILED -> "
                    f"{task.get('state')} (self-healed)"
                )

    def _record_failed(self, task_id, trace: str):
        first_line = trace.split("\n", 1)[0]
        prev = self._active.get(task_id)
        if prev is not None and prev["first_line"] == first_line:
            return  # same ongoing episode, already recorded
        benign = self._is_benign(trace)
        episode = {
            "timestamp": time.time(),
            "task_id": task_id,
            "state": "FAILED",
            "trace": trace,
            "first_line": first_line,
            "benign": benign,
        }
        self._active[task_id] = episode
        self.history.append(episode)
        logger.log(
            logging.WARNING if benign else logging.ERROR,
            f"Watchdog: task {task_id} entered FAILED state"
            f"{' (benign/self-inflicted)' if benign else ''}. "
            f"Trace: {trace[:300]}",
        )

    def confirm_persistent(
        self, grace_seconds: float, poll_interval: float = 5
    ) -> list:
        """Return the failures that genuinely violate the invariant: still
        FAILED and not allowlisted.

        Re-polls over a grace window (the soak has quiesced by now, so chaos
        can't reopen episodes) to let a task that was FAILED at the last poll
        self-heal before we judge it. Returns as soon as there are no
        non-benign active failures, or when the grace window elapses.
        """
        deadline = time.monotonic() + grace_seconds
        while True:
            self.poll()
            persistent = [e for e in self._active.values() if not e["benign"]]
            if not persistent or time.monotonic() >= deadline:
                return persistent
            time.sleep(poll_interval)


# ---------------------------------------------------------------------------
# Workload producer.
# ---------------------------------------------------------------------------


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
        self._seq = 0  # payload sequence (not asserted; content only)
        self._next_partition = 0
        self._stop_event = threading.Event()
        self._thread = None

    @property
    def num_partitions(self) -> int:
        """Current round-robin width. Grows when add-partitions chaos fires."""
        return self._num_partitions

    def set_num_partitions(self, n: int):
        """Widen the round-robin. Only call *after* the topic actually has n
        partitions, so we never produce to a partition that doesn't exist yet.
        An int assignment is atomic under the GIL, so the producer thread can
        read it without a lock.
        """
        self._num_partitions = n

    def _send_batch(self, batch_size: int):
        partition = self._next_partition
        self._next_partition = (self._next_partition + 1) % self._num_partitions
        records = []
        for _ in range(batch_size):
            self._seq += 1
            records.append(json.dumps({"number": str(self._seq)}).encode())
        # sendBytesData flushes and raises if any record wasn't delivered, so
        # we only advance records_produced once the whole batch is confirmed in
        # Kafka. A transient failure (e.g. producing to a just-added partition
        # before its metadata propagated) then neither inflates the drain
        # target nor leaves a gap: Kafka only assigns offsets to delivered
        # records, so surviving offsets stay contiguous per partition.
        self._driver.sendBytesData(self._topic, records, [], partition)
        self.records_produced += batch_size

    def send(self, n: int):
        """Send n records to the next partition in the round-robin sequence."""
        self._send_batch(n)

    def start_continuous(self, batch_size: int, interval: float):
        self._stop_event.clear()

        def _produce():
            while not self._stop_event.is_set():
                try:
                    self._send_batch(batch_size)
                except Exception as e:
                    # Don't let a transient Kafka error kill the producer
                    # thread (which would silently stop the workload); log and
                    # retry on the next tick.
                    logger.warning(f"producer batch failed (continuing): {e}")
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

    @contextlib.contextmanager
    def running(self, batch_size: int, interval: float):
        """Context manager wrapping start_continuous/stop_continuous so the
        producer's lifetime nests cleanly with the other soak phases.
        """
        self.start_continuous(batch_size, interval)
        try:
            yield self
        finally:
            self.stop_continuous()


# ---------------------------------------------------------------------------
# JMX (Jolokia) helpers for the put-duration assertion.
# ---------------------------------------------------------------------------


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


def _assert_chaos_source_healthy(
    gen: ScheduledGenerator, mean_interval: float, soak_seconds: float
):
    """Assert an enabled chaos source actually exercised its failure mode:
    it fired at least once (liveness) and didn't fail most of its attempts
    (error rate). Without this a chaos thread that silently dies -- or whose
    every attempt errors (bad REST auth, rejected config) -- would let the
    soak pass while quietly not testing what it claims to. Both checks are
    duration/attempt-gated so a short sanity run doesn't turn a transient blip
    into a flake; the real multi-hour soak makes enough attempts to catch a
    systematic breakage.
    """
    attempts = gen.count + gen.error_count
    expected = soak_seconds / mean_interval
    if expected >= CHAOS_LIVENESS_MIN_EXPECTED:
        assert gen.count > 0, (
            f"{gen.name} chaos never fired during the {soak_seconds:.0f}s soak "
            f"(expected ~{expected:.0f}); the {gen.name} thread is likely dead, "
            f"so that failure mode went unexercised"
        )
    if attempts >= CHAOS_MIN_ATTEMPTS_FOR_ERROR_CHECK:
        error_rate = gen.error_count / attempts
        assert error_rate <= CHAOS_MAX_ERROR_RATE, (
            f"{gen.name} chaos failed {gen.error_count}/{attempts} attempts "
            f"({error_rate * 100:.0f}% > {CHAOS_MAX_ERROR_RATE * 100:.0f}% "
            f"tolerance) -- its chaos isn't landing (systematic breakage?); "
            f"last error: {gen.last_error!r}"
        )


@pytest.mark.soak
@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
def test_soak_with_chaos(
    driver, credentials, name_salt, create_connector, wait_for_rows
):
    """Soak test: high-rate ingestion under random channel invalidations,
    periodic connector recreates, and tasks.max rebalances.

    Steps:
    1. Bring up the connector against a NUM_PARTITIONS-partition topic and
       send one record so the partition-0 channel exists -- this also lets
       invalidate_channel pytest.skip cleanly if
       SYSTEM$STREAMING_CHANNEL_INVALIDATE is not available on the account.
    2. Start the multi-partition round-robin producer at throttled throughput.
    3. Start chaos sources: random-partition channel invalidation, connector
       recreate (DELETE+POST), tasks.max rebalance (PUT /config), pause/resume,
       in-place restart (POST /restart?includeTasks=true), and add-partitions,
       plus a task-failure watchdog.
    4. Soak for SOAK_DURATION_SECONDS, then stop the chaos but keep the
       producer running for POST_CHAOS_QUIET_SECONDS so any tail-end
       invalidation can surface as an appendRow failure (and trigger the
       reactive recovery path) while data is still flowing.
    5. Drain: wait until row count catches up to records_produced. If the
       drain times out (e.g. chaos throttled channels for too long and we
       accumulated a backlog we can't burn down in DRAIN_TIMEOUT_SECONDS),
       we still run the integrity checks on whatever did land -- a backlog
       is a perf issue, but duplicates or gaps would mean the recovery
       path corrupted data.
    6. Assert per-partition offset integrity: no duplicates ((partition,
       offset) is unique), no gaps (max-min+1 == count per partition), and
       min offset == 0 (no records lost from the head). Drain timeout (if
       any) is re-raised at the end so the test still fails.
    7. Assert no task is stuck in FAILED at the end: transient failures that
       self-heal, and framework guardrails tripped by our own tasks.max chaos
       (TooManyTasksException), are tolerated; anything still FAILED after a
       settle window is a real recovery escape.
    """
    topic = f"test_soak_with_chaos{name_salt}"
    table_name = topic
    driver.createTopics(topic, partitionNum=NUM_PARTITIONS, replicationNum=1)

    connector = create_connector(v4_config=CONNECTOR_CONFIG)
    driver.wait_for_connector_running(connector.name)

    connector_mgr = ConnectorManager(driver, connector, name_salt)
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

    # -- Chaos sources. Each action is a plain callable returning an optional
    #    detail string; ScheduledGenerator owns the scheduling, counting, and
    #    error handling, so adding a new chaos type is one more entry here.
    def _invalidate() -> str:
        # Invalidate a random *currently existing* partition's channel (the set
        # grows if add-partitions chaos is enabled).
        partition = random.randrange(producer.num_partitions)
        invalidate_channel(driver, credentials, table_name, topic, partition=partition)
        return f"partition={partition}"

    def _add_partitions() -> str:
        current = producer.num_partitions
        if current >= ADD_PARTITIONS_MAX:
            return f"at cap ({ADD_PARTITIONS_MAX})"
        new_total = min(current + ADD_PARTITIONS_STEP, ADD_PARTITIONS_MAX)
        # Create in Kafka first (driver waits on the AdminClient future), then
        # widen the producer's round-robin -- never the other way round, or we'd
        # produce to a partition that doesn't exist yet.
        driver.createPartitions(topic, new_total)
        producer.set_num_partitions(new_total)
        return f"{current} -> {new_total} partitions"

    invalidation = ScheduledGenerator(
        "invalidation",
        _invalidate,
        lambda: random.uniform(INVALIDATION_INTERVAL_MIN, INVALIDATION_INTERVAL_MAX),
        join_timeout=5,
    )
    recreate = ScheduledGenerator(
        "recreate",
        connector_mgr.recreate,
        lambda: random.uniform(RESTART_INTERVAL_MIN, RESTART_INTERVAL_MAX),
        enabled=RESTART_CHAOS_ENABLED,
        join_timeout=30,  # createConnector can be slow to abort
    )
    rebalance = ScheduledGenerator(
        "rebalance",
        lambda: connector_mgr.rebalance_tasks_max(TASKS_MAX_CHOICES),
        lambda: random.uniform(REBALANCE_INTERVAL_MIN, REBALANCE_INTERVAL_MAX),
        enabled=REBALANCE_CHAOS_ENABLED,
        join_timeout=30,
    )
    pause_resume = ScheduledGenerator(
        "pause_resume",
        lambda: connector_mgr.pause_resume(PAUSE_RESUME_DWELL_SECONDS),
        lambda: random.uniform(PAUSE_RESUME_INTERVAL_MIN, PAUSE_RESUME_INTERVAL_MAX),
        enabled=PAUSE_RESUME_CHAOS_ENABLED,
        # Cover the dwell so stop() waits for the resume to run before we drain.
        join_timeout=PAUSE_RESUME_DWELL_SECONDS + 15,
    )
    restart_in_place = ScheduledGenerator(
        "restart_in_place",
        connector_mgr.restart_in_place,
        lambda: random.uniform(
            RESTART_IN_PLACE_INTERVAL_MIN, RESTART_IN_PLACE_INTERVAL_MAX
        ),
        enabled=RESTART_IN_PLACE_CHAOS_ENABLED,
        join_timeout=15,
    )
    add_partitions = ScheduledGenerator(
        "add_partitions",
        _add_partitions,
        lambda: random.uniform(
            ADD_PARTITIONS_INTERVAL_MIN, ADD_PARTITIONS_INTERVAL_MAX
        ),
        enabled=ADD_PARTITIONS_CHAOS_ENABLED,
        join_timeout=15,
    )
    chaos = ChaosOrchestrator(
        [
            invalidation,
            recreate,
            rebalance,
            pause_resume,
            restart_in_place,
            add_partitions,
        ]
    )

    watchdog = TaskFailureWatchdog(driver, connector.name)
    # The watchdog is a ScheduledGenerator too, just quieter: it polls on a
    # fixed cadence, logs nothing per-tick, and treats poll errors as
    # transient (DEBUG). It outlives the chaos on purpose -- "no task stuck in
    # FAILED" must hold for the whole test, including the drain.
    watchdog_gen = ScheduledGenerator(
        "watchdog",
        watchdog.poll,
        lambda: WATCHDOG_POLL_INTERVAL,
        log_each=False,
        error_log_level=logging.DEBUG,
        join_timeout=10,
    )

    # -- Lifecycle. Nested scopes make the ordering (and the tail-race
    #    handling) structural: the watchdog outlives the producer, which outlives
    #    the chaos. On any exit -- normal, assertion, or interrupt -- the
    #    context managers tear the threads down in the right order.
    soak_start = time.monotonic()
    with watchdog_gen:
        with producer.running(
            batch_size=PRODUCER_BATCH_SIZE, interval=PRODUCER_INTERVAL
        ):
            with chaos:
                logger.info(f"Soaking for {SOAK_DURATION_SECONDS}s...")
                time.sleep(SOAK_DURATION_SECONDS)
            # Chaos stopped. Guarantee the connector isn't left paused (if a
            # pause_resume cycle was interrupted mid-dwell) before we rely on it
            # ingesting through the quiet period and drain.
            connector_mgr.ensure_resumed()
            # Keep producing for the quiet period. See
            # POST_CHAOS_QUIET_SECONDS: this gives any tail-end invalidation's
            # STALE_CONTINUATION_TOKEN failure time to surface as an appendRow
            # exception (and trigger the reactive recovery path) while data is
            # still flowing, instead of leaving the channel orphaned in
            # ERR_CHANNEL_MUST_BE_REOPENED after the producer goes idle.
            logger.info(
                f"Chaos stopped; producer keeps running for "
                f"{POST_CHAOS_QUIET_SECONDS}s to flush any tail-end "
                f"invalidation through the reactive recovery path"
            )
            time.sleep(POST_CHAOS_QUIET_SECONDS)

        # Producer stopped; the produced count is now final.
        soak_elapsed = time.monotonic() - soak_start
        records_produced = producer.records_produced
        # The put-duration timer is registered on SnowflakeSinkTask.start()
        # and gets a fresh MetricRegistry on every recreate, so its Count
        # reflects only the time since the last restart. Scope the fraction
        # accordingly. _get_put_duration_stats already sums Count*Mean across
        # all task MBeans, so total_put_seconds is the cross-task aggregate.
        put_count_after, total_put_seconds = _get_put_duration_stats(connector.name)
        if connector_mgr.last_recreate_time is not None:
            put_window = max(time.monotonic() - connector_mgr.last_recreate_time, 1.0)
            window_label = f"since last recreate {put_window:.1f}s ago"
        else:
            put_window = soak_elapsed
            window_label = f"whole soak {put_window:.1f}s"
        put_fraction = total_put_seconds / put_window
        put_mean_seconds = (
            total_put_seconds / put_count_after if put_count_after else 0.0
        )
        logger.info(
            f"Soak complete in {soak_elapsed:.1f}s: produced {records_produced} "
            f"records ({records_produced / soak_elapsed:.0f} rec/s) across "
            f"{producer.num_partitions} partitions; chaos fired: "
            f"{invalidation.count} invalidations (plus 1 pre-flight), "
            f"{recreate.count} recreates, {rebalance.count} tasks.max rebalances, "
            f"{pause_resume.count} pause/resumes, "
            f"{restart_in_place.count} in-place restarts, "
            f"{add_partitions.count} add-partitions"
        )
        logger.info(
            f"JMX put-duration ({window_label}): {put_count_after} invocations "
            f"across all live task MBeans, mean={put_mean_seconds * 1000:.1f} ms, "
            f"total ~= {total_put_seconds:.1f}s "
            f"({put_fraction * 100:.1f}% of {put_window:.1f}s window)"
        )

        # -- Drain: wait until everything produced lands in Snowflake. If the
        # drain times out (chaos throttled the channel for too long and we
        # accumulated a backlog we can't burn down in DRAIN_TIMEOUT_SECONDS),
        # we still verify offset integrity on whatever did land -- a backlog
        # is a perf issue, but duplicates or gaps would mean the recovery
        # path corrupted data. So capture the drain failure, run the integrity
        # assertions first, then re-raise it at the end. The watchdog is still
        # running here so a task failure during the drain is captured too.
        logger.info(
            f"Draining: waiting for all {records_produced} records to land "
            f"in {table_name}..."
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
                f"Running offset-integrity assertions on the partial result "
                f"before failing."
            )

    # Watchdog stopped here (its context exited). Chaos and the drain are done,
    # so give Connect a brief grace window to finish any in-flight task
    # (re)start before asserting current health -- under heavy chaos a task can
    # still be momentarily restarting right at the end, and a benign
    # tasks.max.enforce guardrail (see TaskFailureWatchdog) self-heals within
    # seconds. A genuine recovery bug stays FAILED well past this, and
    # _assert_task_running then fails with the offending task's trace.
    with contextlib.suppress(TimeoutError):
        driver.wait_for_connector_running(
            connector.name, timeout=WATCHDOG_SETTLE_SECONDS
        )
    _assert_task_running(driver, connector.name)

    # -- Integrity assertions: per-partition contiguity --
    # Kafka offsets are per-partition (each partition starts at 0), and with
    # round-robin production every partition gets data. We assert: (a) no
    # duplicates across the whole table -- (partition, offset) must be
    # globally unique -- and (b) no gaps within each partition -- offsets
    # 0..N must be contiguous. These hold regardless of whether the drain
    # caught up; they're the core correctness property this soak guards.
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
    # producer or topic configuration is broken. Use the producer's final
    # partition count (add-partitions chaos may have grown it past
    # NUM_PARTITIONS); the producer keeps running through the quiet period, so
    # even a partition added late in the soak receives data before we stop.
    final_partitions = producer.num_partitions
    observed_partitions = {row[0] for row in per_partition}
    assert observed_partitions == set(range(final_partitions)), (
        f"Expected data on partitions {set(range(final_partitions))} but saw "
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
    # Each enabled chaos source must have actually exercised its failure mode:
    # fired often enough to be alive (liveness) and not failed most of its
    # attempts (error rate). Otherwise the soak silently degrades into a weaker
    # test that still passes. Pass each source's mean interval so the liveness
    # gate scales with the soak duration.
    for gen, mean_interval in (
        (invalidation, (INVALIDATION_INTERVAL_MIN + INVALIDATION_INTERVAL_MAX) / 2),
        (recreate, (RESTART_INTERVAL_MIN + RESTART_INTERVAL_MAX) / 2),
        (rebalance, (REBALANCE_INTERVAL_MIN + REBALANCE_INTERVAL_MAX) / 2),
        (pause_resume, (PAUSE_RESUME_INTERVAL_MIN + PAUSE_RESUME_INTERVAL_MAX) / 2),
        (
            restart_in_place,
            (RESTART_IN_PLACE_INTERVAL_MIN + RESTART_IN_PLACE_INTERVAL_MAX) / 2,
        ),
        (
            add_partitions,
            (ADD_PARTITIONS_INTERVAL_MIN + ADD_PARTITIONS_INTERVAL_MAX) / 2,
        ),
    ):
        if gen.enabled:
            _assert_chaos_source_healthy(gen, mean_interval, SOAK_DURATION_SECONDS)

    assert put_count_after > 0, (
        "Task did not invoke put() during the soak -- producer/connector wiring "
        "broken or JMX timer not registered"
    )
    assert put_fraction > PUT_TIME_FRACTION_MIN, (
        f"Task only spent {put_fraction * 100:.1f}% of the soak inside put() "
        f"(threshold {PUT_TIME_FRACTION_MIN * 100:.1f}%); "
        f"raise the producer rate or batch size so the recovery path "
        f"actually gets hammered"
    )

    # The soak invariant: channel recovery must never leave a task *stuck* in
    # FAILED. A task that blipped FAILED and self-healed, or one that Connect's
    # tasks.max.enforce guardrail failed because our own rebalance chaos raced a
    # restart (TooManyTasksException), is expected under this much chaos and is
    # not a violation -- see TaskFailureWatchdog. confirm_persistent() waits out
    # a settle window, then returns only failures still FAILED and not
    # allowlisted.
    persistent_failures = watchdog.confirm_persistent(WATCHDOG_SETTLE_SECONDS)
    benign_count = sum(1 for e in watchdog.history if e["benign"])
    logger.info(
        f"Watchdog summary: {len(watchdog.history)} failure episode(s) "
        f"({benign_count} benign/self-inflicted), {len(watchdog.recovered)} "
        f"self-healed, {len(persistent_failures)} persistent + non-allowlisted"
    )
    assert not persistent_failures, (
        f"{len(persistent_failures)} task(s) stuck in FAILED at end of soak "
        f"(not self-healed within {WATCHDOG_SETTLE_SECONDS}s, not allowlisted):\n"
        + "\n".join(
            f"  - t={f['timestamp']:.0f} task={f['task_id']} "
            f"state={f['state']}: {f['trace']}"
            for f in persistent_failures
        )
    )

    # The chaos and integrity checks all passed -- if the drain timed out
    # earlier, the test still failed (we just held the AssertionError until
    # after the integrity checks so a drain failure can't mask a data bug).
    if drain_failure is not None:
        raise drain_failure
