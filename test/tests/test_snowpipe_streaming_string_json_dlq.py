import time

import pytest

pytestmark = pytest.mark.correctness

FILE_NAME = "snowpipe_streaming_string_json_dlq"
CONFIG_FILE = f"{FILE_NAME}.json"
RECORD_COUNT = 5
EXPECTED_IN_TABLE = 0
EXPECTED_IN_DLQ = 5


def test_snowpipe_streaming_string_json_dlq(
    driver, name_salt, create_connector_from_file, create_table
):
    table = create_table(
        FILE_NAME.upper(),
        columns="(record_metadata variant, record_content variant)",
    )
    topic = f"{FILE_NAME}{name_salt}"

    driver.createTopics(topic, partitionNum=1, replicationNum=1)

    config = create_connector_from_file(CONFIG_FILE)
    driver.startConnectorWaitTime()

    # -- Send invalid data that cannot be deserialized --
    invalid = b'{invalid_string"}'
    values = [invalid for _ in range(RECORD_COUNT)]
    driver.sendBytesData(topic, values, [], partition=0)

    # -- Verify: no rows should land in the table --
    time.sleep(30)
    count = table.select_scalar("count(*)")
    assert count == EXPECTED_IN_TABLE, (
        f"Expected {EXPECTED_IN_TABLE} rows but got {count}"
    )

    # -- Verify: records should appear in the DLQ topic --
    offsets_in_dlq = driver.consume_messages_dlq(config, 0, EXPECTED_IN_DLQ - 1)
    assert offsets_in_dlq == EXPECTED_IN_DLQ, (
        f"Expected {EXPECTED_IN_DLQ} offsets in DLQ, got {offsets_in_dlq}"
    )
