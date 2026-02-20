import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest

logger = logging.getLogger(__name__)

FILE_NAME = "travis_pressure_string_json"
CONFIG_FILE = f"{FILE_NAME}.json"
TOPIC_COUNT = 200
PARTITION_COUNT = 12
RECORD_COUNT = 10_000
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


@pytest.mark.pressure
def test_pressure(driver, create_topic, create_connector, wait_for_rows):
    topics = create_topic(
        [f"{FILE_NAME}_{i}" for i in range(TOPIC_COUNT)], num_partitions=PARTITION_COUNT
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
            if i % 100 == 0 or i == total:
                logger.info(f"Sent {i}/{total} partitions")

    for i, topic in enumerate(topics):
        logger.info("Verifying topic %d/%d: %s", i + 1, TOPIC_COUNT, topic)
        wait_for_rows(topic, PARTITION_COUNT * RECORD_COUNT, timeout=1800)
