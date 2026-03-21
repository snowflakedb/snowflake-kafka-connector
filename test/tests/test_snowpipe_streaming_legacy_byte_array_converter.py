import base64
import json

FILE_NAME = "snowpipe_streaming_legacy_byte_array_converter"
CONFIG_FILE = f"{FILE_NAME}.json"
RECORD_COUNT = 100


def test_snowpipe_streaming_legacy_byte_array_converter(
    driver, create_connector_from_file, create_table, wait_for_rows
):
    """Verify that ByteArrayConverter is accepted when enable.schematization=false
    and that raw byte payloads land (base64-encoded) in the legacy RECORD_CONTENT column.
    """
    table = create_table(
        FILE_NAME,
        columns="(RECORD_METADATA variant, RECORD_CONTENT variant)",
    )
    topic = table.name

    driver.createTopics(topic, partitionNum=1, replicationNum=1)

    create_connector_from_file(CONFIG_FILE)
    driver.startConnectorWaitTime()

    # -- Send raw byte records --
    values = [f"binary payload {i}".encode("utf-8") for i in range(RECORD_COUNT)]
    driver.sendBytesData(topic, values, [], partition=0)

    # -- Verify row count --
    wait_for_rows(table.name, RECORD_COUNT)

    # -- Verify RECORD_CONTENT contains base64-encoded data --
    row = table.select(
        "RECORD_CONTENT, RECORD_METADATA",
        'WHERE RECORD_METADATA:"offset"::number = 0',
    )[0]

    content = str(row["RECORD_CONTENT"])
    expected_b64 = base64.b64encode(b"binary payload 0").decode("utf-8")
    assert expected_b64 in content, (
        f"Expected base64 '{expected_b64}' in RECORD_CONTENT, got: {row['RECORD_CONTENT']}"
    )

    metadata = json.loads(row["RECORD_METADATA"])
    if isinstance(metadata, str):
        metadata = json.loads(metadata)
    for key in ("offset", "partition", "topic"):
        assert key in metadata, (
            f"RECORD_METADATA missing '{key}': {row['RECORD_METADATA']}"
        )
