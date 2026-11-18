"""Scale and soak tests for server-side evolve-schema.

These measure what schema changes cost under load: a very wide schema, many tasks evolving at
once, the throughput dip around each pipe restart, steady state with the flag on and off, and
schema churn. Measurements are logged; assertions cover data loss and the call counts.
Evolve-schema call counts come from the connector's JMX counters (run_tests.sh --jmx).
"""

import logging
import threading
import time
from typing import Callable

import pytest

from lib.driver import KafkaDriver
from lib.evolve_schema import (
    ID_FIELD,
    column_types,
    envelope,
    field,
    require,
    send,
    start,
    struct,
)

logger = logging.getLogger(__name__)

pytestmark = [pytest.mark.parametrize("connector_version", ["v4"], indirect=True)]

# Kafka Connect drops a consumer that doesn't poll within max.poll.interval.ms (5 minutes).
MAX_POLL_INTERVAL_S = 300


def _schema(version: int):
    """Schema version `version` has ID and fields F0..F{version}."""
    return struct(ID_FIELD, *(field(f"F{k}", "int32") for k in range(version + 1)))


def _payload(version: int, record_id: int):
    return {"ID": record_id, **{f"F{k}": k for k in range(version + 1)}}


class _SchemaProducer:
    """Sends numbered records in the background, moving to a new schema version on a schedule.

    `version_for(elapsed_s, sent)` returns the schema version for the next batch. With
    `records_per_s`, sending is paced to that rate; otherwise it's as fast as Kafka accepts.
    """

    def __init__(
        self, driver, topic, version_for, batch=500, partitions=1, records_per_s=None
    ):
        self._driver, self._topic = driver, topic
        self._version_for = version_for
        self._batch, self._partitions = batch, partitions
        self._records_per_s = records_per_s
        self.sent = 0
        self.versions = set()
        self.version_changes = []  # (monotonic time, version)
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def _run(self):
        started = time.monotonic()
        while not self._stop.is_set():
            version = self._version_for(time.monotonic() - started, self.sent)
            if version not in self.versions:
                self.versions.add(version)
                self.version_changes.append((time.monotonic(), version))
            schema = _schema(version)
            values = [
                envelope(schema, _payload(version, self.sent + i))
                for i in range(self._batch)
            ]
            partition = (self.sent // self._batch) % self._partitions
            self._driver.sendBytesData(self._topic, values, [], partition=partition)
            self.sent += self._batch
            if self._records_per_s:
                ahead = self.sent / self._records_per_s - (time.monotonic() - started)
                if ahead > 0:
                    self._stop.wait(ahead)

    def start(self):
        self._thread.start()
        return self

    def stop(self):
        self._stop.set()
        self._thread.join(timeout=60)


def _count(table) -> int:
    return table.select_scalar("count(*)")


def _sample_counts(table, until: Callable[[], bool], interval=5):
    """Polls the row count until `until()`; returns [(monotonic time, count)]."""
    samples = []
    while not until():
        samples.append((time.monotonic(), _count(table)))
        time.sleep(interval)
    samples.append((time.monotonic(), _count(table)))
    return samples


def _rate(samples, start, end) -> float:
    """Rows per second landed between two monotonic times, from the samples."""
    inside = [(t, c) for t, c in samples if start <= t <= end]
    if len(inside) < 2:
        return 0.0
    (t0, c0), (t1, c1) = inside[0], inside[-1]
    return (c1 - c0) / (t1 - t0) if t1 > t0 else 0.0


def _ingest_timed(driver, table, connector, wait_for_rows, records, schema):
    started = time.monotonic()
    for offset in range(0, records, 10_000):
        send(
            driver,
            table.name,
            schema,
            [{"ID": i, "F0": 0} for i in range(offset, min(offset + 10_000, records))],
        )
    wait_for_rows(table.name, records, connector_name=connector.name, timeout=1800)
    return time.monotonic() - started


# create_custom_connector is last in each signature so the connector is torn down before the
# table and its topic.


@pytest.mark.pressure
def test_wide_schema(
    driver: KafkaDriver,
    create_table: Callable,
    wait_for_rows: Callable,
    create_custom_connector: Callable,
):
    """One schema adds 1,999 columns; the call finishes well within max.poll.interval.ms."""
    schema = struct(ID_FIELD, *(field(f"C{k}", "int32") for k in range(1998)))
    table, connector = start(
        driver, create_table, create_custom_connector, "evolve_schema_wide"
    )
    started = time.monotonic()
    send(
        driver,
        table.name,
        schema,
        [{"ID": i, **{f"C{k}": k for k in range(1998)}} for i in range(10)],
    )
    wait_for_rows(
        table.name, 10, connector_name=connector.name, timeout=MAX_POLL_INTERVAL_S
    )
    elapsed = time.monotonic() - started
    logger.info("1,999 columns: first rows landed after %.1fs", elapsed)

    assert len(column_types(table)) == 2000
    assert not driver.get_failed_tasks(connector.name)
    assert elapsed < MAX_POLL_INTERVAL_S


@pytest.mark.pressure
def test_many_tasks_same_new_schema(
    driver: KafkaDriver,
    create_table: Callable,
    wait_for_rows: Callable,
    jolokia,
    create_custom_connector: Callable,
):
    """Eight tasks see the same new schema at once.

    Each task's evolver calls once, so there are at most eight calls; the first adds the
    columns and the others find them present. Measures the time until rows flow again.
    """
    jolokia = require(jolokia, "--jmx")
    partitions = 8
    table, connector = start(
        driver,
        create_table,
        create_custom_connector,
        "evolve_schema_many_tasks",
        partitions=partitions,
        **{"tasks.max": str(partitions)},
    )
    v0, v1 = _schema(0), _schema(1)
    for p in range(partitions):
        send(
            driver,
            table.name,
            v0,
            [_payload(0, p * 1000 + i) for i in range(100)],
            partition=p,
        )
    wait_for_rows(table.name, 100 * partitions, connector_name=connector.name)

    started = time.monotonic()
    threads = [
        threading.Thread(
            target=send,
            args=(
                driver,
                table.name,
                v1,
                [_payload(1, 100_000 + p * 1000 + i) for i in range(100)],
            ),
            kwargs={"partition": p},
        )
        for p in range(partitions)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    wait_for_rows(table.name, 200 * partitions, connector_name=connector.name)
    elapsed = time.monotonic() - started
    calls = jolokia.evolve_schema_counters(connector.name)["call-count"]
    logger.info(
        "8 tasks, one new schema: %d calls, rows flowing after %.1fs", calls, elapsed
    )

    assert column_types(table)["F1"] == "NUMBER(10,0)"
    assert calls <= 2 * partitions  # one call per task per schema, two schemas


@pytest.mark.pressure
def test_throughput_across_pipe_restarts(
    driver: KafkaDriver,
    create_table: Callable,
    wait_for_rows: Callable,
    create_custom_connector: Callable,
):
    """Steady load for three minutes with a new schema each minute.

    Each new column restarts the pipe. Logs the throughput in the 30 seconds before and after
    each change. Every record lands once, and rows flow again after each change.
    """
    table, connector = start(
        driver, create_table, create_custom_connector, "evolve_schema_restart_dip"
    )
    producer = _SchemaProducer(
        driver, table.name, lambda elapsed, _: int(elapsed // 60)
    ).start()
    deadline = time.monotonic() + 180
    samples = _sample_counts(table, until=lambda: time.monotonic() >= deadline)
    producer.stop()
    wait_for_rows(table.name, producer.sent, connector_name=connector.name, timeout=900)

    for changed_at, version in producer.version_changes[1:]:
        if changed_at + 30 > deadline:
            continue  # The load stopped before a full window after this change.
        before = _rate(samples, changed_at - 30, changed_at)
        after = _rate(samples, changed_at, changed_at + 30)
        logger.info(
            "Schema v%d: %.0f rows/s before, %.0f rows/s after", version, before, after
        )
    assert table.select_scalar('count(DISTINCT "ID")') == producer.sent


@pytest.mark.pressure
def test_steady_state_flag_on_vs_off(
    driver: KafkaDriver,
    create_table: Callable,
    wait_for_rows: Callable,
    jolokia,
    create_custom_connector: Callable,
):
    """With one schema and no changes, the flag costs one call and no throughput.

    The same 100,000 records go through a connector with the flag off and one with it on.
    """
    jolokia = require(jolokia, "--jmx")
    records = 100_000
    schema = _schema(0)
    off_table, off = start(
        driver,
        create_table,
        create_custom_connector,
        "evolve_schema_steady_off",
        evolve_schema=False,
    )
    off_s = _ingest_timed(driver, off_table, off, wait_for_rows, records, schema)
    on_table, on = start(
        driver, create_table, create_custom_connector, "evolve_schema_steady_on"
    )
    on_s = _ingest_timed(driver, on_table, on, wait_for_rows, records, schema)
    logger.info(
        "100,000 records: %.1fs with the flag off, %.1fs with it on", off_s, on_s
    )

    assert jolokia.evolve_schema_counters(on.name)["call-count"] == 1
    assert on_s <= off_s * 1.25 + 30


@pytest.mark.pressure
def test_schema_churn(
    driver: KafkaDriver,
    create_table: Callable,
    wait_for_rows: Callable,
    jolokia,
    create_custom_connector: Callable,
):
    """A new field every 100,000 records for 10 minutes, at 10,000 records/s.

    That's about 60 schemas, each restarting the pipe. Logs throughput and the number of
    schemas; asserts one call per schema and no data loss.
    """
    jolokia = require(jolokia, "--jmx")
    table, connector = start(
        driver, create_table, create_custom_connector, "evolve_schema_churn"
    )
    producer = _SchemaProducer(
        driver, table.name, lambda _, sent: sent // 100_000, records_per_s=10_000
    ).start()
    started = time.monotonic()
    time.sleep(600)
    producer.stop()
    wait_for_rows(
        table.name, producer.sent, connector_name=connector.name, timeout=1800
    )
    elapsed = time.monotonic() - started
    calls = jolokia.evolve_schema_counters(connector.name)["call-count"]
    logger.info(
        "Churn: %d records, %d schemas, %d calls, %.0f rows/s",
        producer.sent,
        len(producer.versions),
        calls,
        producer.sent / elapsed,
    )

    assert calls == len(producer.versions)
    assert table.select_scalar('count(DISTINCT "ID")') == producer.sent


@pytest.mark.soak
def test_soak_with_periodic_schema_changes(
    driver: KafkaDriver,
    create_table: Callable,
    wait_for_rows: Callable,
    jolokia,
    create_custom_connector: Callable,
):
    """One hour at 10,000 records/s with a new schema every five minutes.

    The rate is one the connector sustains, so lag stays flat: each pipe restart's backlog
    drains before the next one. One call per schema, and no record lost or duplicated.
    """
    jolokia = require(jolokia, "--jmx")
    table, connector = start(
        driver, create_table, create_custom_connector, "evolve_schema_soak"
    )
    producer = _SchemaProducer(
        driver,
        table.name,
        lambda elapsed, _: int(elapsed // 300),
        batch=500,
        records_per_s=10_000,
    ).start()
    deadline = time.monotonic() + 3600
    lags = []

    def sample_lag():
        lags.append(producer.sent - _count(table))
        return time.monotonic() >= deadline

    _sample_counts(table, until=sample_lag, interval=60)
    producer.stop()
    wait_for_rows(
        table.name, producer.sent, connector_name=connector.name, timeout=1800
    )
    logger.info("Soak lag samples (records): %s", lags)

    assert jolokia.evolve_schema_counters(connector.name)["call-count"] == len(
        producer.versions
    )
    assert table.select_scalar('count(DISTINCT "ID")') == producer.sent
    # The second half's worst lag is no more than twice the first half's plus a minute of load.
    half = len(lags) // 2
    assert max(lags[half:]) <= 2 * max(lags[:half]) + 60 * 10_000
