import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest

logger = logging.getLogger(__name__)

FILE_NAME = "travis_pressure_restart"
CONFIG_FILE = f"{FILE_NAME}.json"
TOPIC_COUNT = 10
PARTITION_COUNT = 3
RECORD_COUNT = 200_000
EXPECTED_PER_TOPIC = PARTITION_COUNT * RECORD_COUNT
THREAD_COUNT = 10


def _send_partition(driver, topic, partition, record_count):
    values = [
        json.dumps(
            {
                "numbernumbernumbernumbernumbernumbernumbernumbernumbernumbernumbernumber": str(
                    e
                )
            }
        ).encode("utf-8")
        for e in range(record_count)
    ]
    driver.sendBytesData(topic, values, [], partition)


def _chaos_operation(driver, connector_name, name_salt, counter):
    """Cycle through connector lifecycle operations, matching the original mod-7 pattern."""
    phase = counter % 7
    if phase in (2, 3):
        driver.restartConnector(connector_name)
    elif phase == 4:
        driver.pauseConnector(connector_name)
    elif phase == 5:
        driver.resumeConnector(connector_name)
    elif phase == 6:
        driver.deleteConnector(connector_name)
    elif phase == 0 and counter > 0:
        driver.createConnector(CONFIG_FILE, name_salt)


@pytest.mark.pressure
def test_pressure_restart(driver, name_salt, create_topic, create_connector):
    connector_name = f"{FILE_NAME}{name_salt}"
    topics = create_topic(
        [f"{FILE_NAME}{i}" for i in range(TOPIC_COUNT)], num_partitions=PARTITION_COUNT
    )

    create_connector(CONFIG_FILE)
    driver.startConnectorWaitTime()

    total = TOPIC_COUNT * PARTITION_COUNT
    with ThreadPoolExecutor(max_workers=THREAD_COUNT) as executor:
        futures = [
            executor.submit(_send_partition, driver, topics[t], p, RECORD_COUNT)
            for t in range(TOPIC_COUNT)
            for p in range(PARTITION_COUNT)
        ]
        for i, future in enumerate(as_completed(futures), 1):
            future.result()
            if i % 10 == 0 or i == total:
                logger.info(f"Sent {i}/{total} partitions")

    chaos_counter = 0
    for i, topic in enumerate(topics):
        logger.info(f"Verifying topic {i + 1}/{TOPIC_COUNT}: {topic}")
        deadline = time.monotonic() + 600
        while True:
            chaos_counter += 1
            _chaos_operation(driver, connector_name, name_salt, chaos_counter)

            count = driver.select_number_of_records(topic)
            if count == EXPECTED_PER_TOPIC:
                break
            if time.monotonic() >= deadline:
                raise AssertionError(
                    f"Timed out waiting for {EXPECTED_PER_TOPIC} rows in {topic} "
                    f"(got {count} after 600s)"
                )
            logger.info(
                f"Topic {topic}: {count}/{EXPECTED_PER_TOPIC} rows, retrying in {driver.VERIFY_INTERVAL}s..."
            )
            time.sleep(driver.VERIFY_INTERVAL)
