import json
from time import sleep

FILE_NAME = "test_kc_delete_resume"
CONFIG_FILE = f"{FILE_NAME}.json"
RECORD_COUNT = 1000
SLEEP_TIME = 10


def _send_batch(driver, topic, record_count):
    values = [
        json.dumps({"column1": str(i)}).encode("utf-8") for i in range(record_count)
    ]
    driver.sendBytesData(topic, values, [], 0)
    sleep(2)


def test_kc_delete_resume(
    driver, name_salt, create_connector, snowflake_table, wait_for_rows
):
    """Verify that resuming a deleted connector is a no-op.

    Sequence:
      1. Send batch 1 → ingested
      2. Delete connector
      3. Resume connector → fails silently (connector was deleted)
      4. Send batch 2 → NOT ingested (no running connector)

    Only batch 1 should appear in the table (RECORD_COUNT rows).
    """
    topic = snowflake_table(
        FILE_NAME,
        f"CREATE OR REPLACE TABLE {FILE_NAME}{name_salt} "
        f"(record_metadata variant, column1 varchar)",
    )

    connector_name = f"{FILE_NAME}{name_salt}"
    driver.createTopics(topic, partitionNum=1, replicationNum=1)

    create_connector(CONFIG_FILE)
    driver.startConnectorWaitTime()

    # -- Send batch 1 --
    _send_batch(driver, topic, RECORD_COUNT)

    # -- Delete connector --
    driver.deleteConnector(connector_name)
    sleep(SLEEP_TIME)

    # -- Resume (should fail since connector was deleted) --
    driver.resumeConnector(connector_name)
    sleep(SLEEP_TIME)

    # -- Send batch 2 (no connector running, so this won't be ingested) --
    _send_batch(driver, topic, RECORD_COUNT)

    # -- Verify only batch 1 was ingested --
    wait_for_rows(topic, RECORD_COUNT)
