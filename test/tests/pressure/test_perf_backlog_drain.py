"""
P1 Backlog Drain — profiling-friendly performance test.

Defaults: 4 partitions × 1M records × ~250 bytes = 4M rows (~1 GB).
All parameters are tunable via environment variables (see below).

Scenario:
  1. Create a single topic with DRAIN_PARTITIONS partitions (default 4).
  2. Loader phase: pre-populate the topic with DRAIN_RECORDS_PER_PARTITION
     records per partition.  Each message is a ~250-byte JSON row.
  3. KC phase: start a Snowflake Streaming connector with DRAIN_TASKS_MAX
     tasks (default 8) to drain the full topic from offset 0 ("cold start").
     Runs for up to DRAIN_KC_TIMEOUT seconds (default 900).
  4. Post-run: log final offsets, row counts, and drain time.

Usage:
  ./run_tests.sh --platform=confluent --platform-version=7.8.0 --profile --keep \\
      -- tests/pressure/test_perf_backlog_drain.py

  # Larger run (e.g. ~144M rows / ~38 GB):
  DRAIN_PARTITIONS=4 DRAIN_RECORDS_PER_PARTITION=36000000 \\
  ./run_tests.sh ... -- tests/pressure/test_perf_backlog_drain.py
"""

import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest

from lib.config_migration import V4_CONFIG_TEMPLATE

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Tunables — override via environment variables for quick profiling runs
# ---------------------------------------------------------------------------
PARTITION_COUNT = int(os.environ.get("DRAIN_PARTITIONS", "4"))
TASKS_MAX = int(os.environ.get("DRAIN_TASKS_MAX", "8"))
RECORDS_PER_PARTITION = int(os.environ.get("DRAIN_RECORDS_PER_PARTITION", "1_000_000"))
LOADER_THREADS = int(os.environ.get("DRAIN_LOADER_THREADS", "4"))
BATCH_SIZE = int(os.environ.get("DRAIN_BATCH_SIZE", "50_000"))
KC_TIMEOUT = int(os.environ.get("DRAIN_KC_TIMEOUT", "900"))
ROW_SIZE_APPROX = 250  # bytes per JSON message


def _make_row(partition: int, offset: int) -> bytes:
    """Build a single JSON message (~250 bytes)."""
    return json.dumps(
        {
            "MESSAGE": f"p{partition}-{offset}",
            "TIMESTAMP": int(time.time() * 1000),
            "ID": offset,
            "PARTITION": partition,
            "ROW_SIZE_IN_BYTES": ROW_SIZE_APPROX,
        }
    ).encode("utf-8")


def _load_partition(driver, topic: str, partition: int, total: int, batch: int):
    """Send `total` records to a single partition in batches."""
    sent = 0
    while sent < total:
        chunk = min(batch, total - sent)
        values = [_make_row(partition, sent + i) for i in range(chunk)]
        driver.sendBytesData(topic, values, key=None, partition=partition)
        sent += chunk
        if sent % 200_000 == 0 or sent == total:
            logger.info(
                "Loader p%d: %d / %d (%.1f%%)",
                partition,
                sent,
                total,
                100 * sent / total,
            )
    return sent


@pytest.mark.pressure
@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
def test_perf_backlog_drain(
    driver,
    name_salt,
    create_topics,
    create_custom_connector,
    wait_for_rows,
):
    total_records = PARTITION_COUNT * RECORDS_PER_PARTITION
    total_bytes = total_records * ROW_SIZE_APPROX
    logger.info(
        "=== P1 Backlog Drain: %d partitions × %d records = %d total (%.1f GB) ===",
        PARTITION_COUNT,
        RECORDS_PER_PARTITION,
        total_records,
        total_bytes / 1e9,
    )

    # -----------------------------------------------------------------------
    # 1. Topic setup
    # -----------------------------------------------------------------------
    topic_unsalted = "perf_backlog_drain"
    topics = create_topics(
        [topic_unsalted],
        num_partitions=PARTITION_COUNT,
    )
    topic = topics[0]
    logger.info("Topic created: %s (%d partitions)", topic, PARTITION_COUNT)

    # -----------------------------------------------------------------------
    # 2. Loader phase — fill the topic before starting KC
    # -----------------------------------------------------------------------
    logger.info(
        "=== Loader phase: %d threads, %d records/partition, batch=%d ===",
        LOADER_THREADS,
        RECORDS_PER_PARTITION,
        BATCH_SIZE,
    )
    load_start = time.time()
    with ThreadPoolExecutor(max_workers=LOADER_THREADS) as pool:
        futures = {
            pool.submit(
                _load_partition,
                driver,
                topic,
                p,
                RECORDS_PER_PARTITION,
                BATCH_SIZE,
            ): p
            for p in range(PARTITION_COUNT)
        }
        for fut in as_completed(futures):
            p = futures[fut]
            count = fut.result()
            logger.info("Partition %d loaded: %d records", p, count)

    load_elapsed = time.time() - load_start
    load_throughput = total_bytes / load_elapsed / 1e6
    logger.info(
        "=== Loader done: %.1fs, %.1f MB/s ===",
        load_elapsed,
        load_throughput,
    )

    # -----------------------------------------------------------------------
    # 3. KC phase — connector starts cold against a full topic
    # -----------------------------------------------------------------------
    logger.info(
        "=== KC phase: %d tasks, timeout=%ds ===",
        TASKS_MAX,
        KC_TIMEOUT,
    )
    kc_start = time.time()

    connector = create_custom_connector(
        "perf_backlog_drain",
        {
            **V4_CONFIG_TEMPLATE,
            "tasks.max": str(TASKS_MAX),
            "topics": topic,
            "key.converter": "org.apache.kafka.connect.storage.StringConverter",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter.schemas.enable": "false",
            "snowflake.validation": "server_side",
            "consumer.override.max.poll.interval.ms": "600000",
            "consumer.override.auto.offset.reset": "earliest",
        },
    )

    driver.wait_for_connector_running(connector.name, timeout=120)
    logger.info("Connector %s is RUNNING", connector.name)

    # -----------------------------------------------------------------------
    # 4. Wait for all rows to land in Snowflake
    # -----------------------------------------------------------------------
    table_name = topic.upper()
    wait_for_rows(
        table_name,
        total_records,
        timeout=KC_TIMEOUT,
        interval=10,
        connector_name=connector.name,
    )

    kc_elapsed = time.time() - kc_start
    drain_throughput = total_bytes / kc_elapsed / 1e6
    logger.info(
        "=== KC drain complete: %.1fs, %.1f MB/s ===",
        kc_elapsed,
        drain_throughput,
    )

    # -----------------------------------------------------------------------
    # 5. Post-run stats
    # -----------------------------------------------------------------------
    logger.info("=== Post-run stats ===")
    logger.info("  Total records:      %d", total_records)
    logger.info(
        "  Load time:          %.1fs (%.1f MB/s)", load_elapsed, load_throughput
    )
    logger.info("  Drain time:         %.1fs (%.1f MB/s)", kc_elapsed, drain_throughput)
    logger.info(
        "  Rows/sec (drain):   %.0f",
        total_records / kc_elapsed if kc_elapsed > 0 else 0,
    )
    row_count = driver.select_number_of_records(table_name)
    logger.info("  Snowflake rows:     %s", row_count)
    assert row_count == total_records, (
        f"Expected {total_records} rows but got {row_count}"
    )
