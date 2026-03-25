import json
from time import sleep

import pytest

FILE_NAME = "test_kc_restart"
CONFIG_FILE = f"{FILE_NAME}.json"
RECORD_COUNT = 1000
SLEEP_TIME = 10


def _send_batch(driver, topic, record_count):
    values = [
        json.dumps({"column1": str(i)}).encode("utf-8") for i in range(record_count)
    ]
    driver.sendBytesData(topic, values, [], 0)
    sleep(2)


@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
def test_kc_restart(
    driver, name_salt, create_connector_from_file, create_table, wait_for_rows
):
    table = create_table(
        FILE_NAME,
        columns="(record_metadata variant, column1 varchar)",
    )
    topic = table.name

    connector_name = f"{FILE_NAME}{name_salt}"
    driver.createTopics(topic, partitionNum=1, replicationNum=1)

    create_connector_from_file(CONFIG_FILE)
    driver.startConnectorWaitTime()

    # -- Send 1/3, restart connector, send 2/3, restart connector+tasks, send 3/3 --
    _send_batch(driver, topic, RECORD_COUNT)

    driver.restartConnector(connector_name)
    driver.wait_for_connector_running(connector_name)

    _send_batch(driver, topic, RECORD_COUNT)

    driver.restartConnectorAndTasks(connector_name)
    driver.wait_for_connector_running(connector_name)

    _send_batch(driver, topic, RECORD_COUNT)

    # -- Verify --
    wait_for_rows(table.name, RECORD_COUNT * 3, connector_name=connector_name)
