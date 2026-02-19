import json
from time import sleep

FILE_NAME = "test_kc_recreate_chaos"
CONFIG_FILE = f"{FILE_NAME}.json"
RECORD_COUNT = 100
SLEEP_TIME = 10


def _send_batch(driver, topic, record_count):
    values = [
        json.dumps({"column1": str(i)}).encode("utf-8") for i in range(record_count)
    ]
    driver.sendBytesData(topic, values, [], 0)
    sleep(2)


def test_kc_recreate_chaos(
    driver, name_salt, create_connector, snowflake_table, wait_for_rows
):
    topic = snowflake_table(
        FILE_NAME,
        f"CREATE OR REPLACE TABLE {FILE_NAME}{name_salt} "
        f"(record_metadata variant, column1 varchar)",
    )

    driver.createTopics(topic, partitionNum=1, replicationNum=1)

    create_connector(CONFIG_FILE)
    driver.startConnectorWaitTime()

    # -- Send 1/2, create (idempotent) twice with pressure, send 2/2 --
    _send_batch(driver, topic, RECORD_COUNT)

    driver.createConnector(CONFIG_FILE, name_salt)
    sleep(SLEEP_TIME)

    driver.createConnector(CONFIG_FILE, name_salt)
    sleep(SLEEP_TIME)

    _send_batch(driver, topic, RECORD_COUNT)

    # -- Verify --
    wait_for_rows(topic, RECORD_COUNT * 2)
