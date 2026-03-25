import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest

from lib.config_migration import V4_CONFIG_TEMPLATE
from lib.driver import KafkaDriver

logger = logging.getLogger(__name__)

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


@pytest.mark.pressure
@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
def test_pressure_restart(driver: KafkaDriver, create_topics, create_custom_connector):
    test_name = "test_pressure_restart"

    topics = create_topics(
        [f"{test_name}{i}" for i in range(TOPIC_COUNT)], num_partitions=PARTITION_COUNT
    )

    config = {
        **V4_CONFIG_TEMPLATE,
        "tasks.max": "10",
        "topics.regex": f"{test_name}.*",
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
        "snowflake.client.validation.enabled": "false",
    }
    connector = create_custom_connector(test_name, config)

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

    phase = 0
    for i, topic in enumerate(topics):
        table_name = topic.upper()
        logger.info(f"Verifying topic {i + 1}/{TOPIC_COUNT}: {table_name}")
        deadline = time.monotonic() + 600
        while True:
            phase = (phase + 1) % 7
            match phase:
                case 2 | 3:
                    driver.restartConnector(connector.name)
                case 4:
                    driver.pauseConnector(connector.name)
                case 5:
                    driver.resumeConnector(connector.name)
                case 6:
                    connector.close()
                case 0:
                    connector = create_custom_connector(test_name, config)

            count = driver.select_number_of_records(table_name)
            if count == EXPECTED_PER_TOPIC:
                break
            if time.monotonic() >= deadline:
                raise AssertionError(
                    f"Timed out waiting for {EXPECTED_PER_TOPIC} rows in {table_name} "
                    f"(got {count} after 600s)"
                )
            logger.info(
                f"Topic {table_name}: {count}/{EXPECTED_PER_TOPIC} rows, retrying in {driver.VERIFY_INTERVAL}s..."
            )
            time.sleep(driver.VERIFY_INTERVAL)
