import json
import time
import pytest
from time import sleep

FILE_NAME = "test_kc_delete_resume_chaos"
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
def test_kc_delete_resume_chaos(driver, name_salt, create_connector, snowflake_table):
    """Verify connector behavior during delete with pressure and a failed resume.

    Sequence:
      1. Send batch 1 → ingested
      2. Delete connector
      3. Send batch 2 (pressure during deletion) → partially ingested
      4. Resume connector → fails silently (connector was deleted)
      5. Send batch 3 → NOT ingested (no running connector)

    Expected: between RECORD_COUNT and 2 × RECORD_COUNT rows
    (batch 1 always ingested, some of batch 2 may be ingested before
    the deletion completes; batch 3 is never ingested because resume
    cannot recreate a deleted connector).
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

    # -- Delete connector + pressure (batch 2 sent during deletion) --
    driver.deleteConnector(connector_name)
    _send_batch(driver, topic, RECORD_COUNT)
    sleep(SLEEP_TIME)

    # -- Resume (should fail since connector was deleted) --
    driver.resumeConnector(connector_name)
    sleep(SLEEP_TIME)

    # -- Send batch 3 (no connector running) --
    _send_batch(driver, topic, RECORD_COUNT)

    # -- Verify: between 1 and 2 batches ingested --
    # Cannot use wait_for_rows (exact match) since batch 2 may partially arrive,
    # making the total non-deterministic. Poll until count >= RECORD_COUNT instead.
    deadline = time.monotonic() + 600
    while driver.select_number_of_records(topic) < RECORD_COUNT:
        if time.monotonic() >= deadline:
            raise AssertionError(
                f"Timed out waiting for at least {RECORD_COUNT} rows in {topic}"
            )
        sleep(10)

    count = driver.select_number_of_records(topic)
    upper_bound = RECORD_COUNT * 2
    assert count <= upper_bound, (
        f"Expected at most {upper_bound} rows, got {count} — "
        f"unexpected duplication or batch 3 was ingested"
    )
