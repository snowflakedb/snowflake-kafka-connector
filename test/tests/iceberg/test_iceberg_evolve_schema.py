"""Server-side evolve-schema e2e tests on Snowflake-managed Iceberg v2 and v3 tables.

The connector auto-creates the metadata-only Iceberg table (structured RECORD_METADATA on v2,
where VARIANT isn't allowed), then sends each record's Kafka Connect schema to evolveSchema.
Columns get the Iceberg types for the table's format version. v4-only.
"""

from typing import Callable

import pytest

from lib.driver import KafkaDriver
from lib.evolve_schema import (
    ID_FIELD,
    TIMESTAMP_NAME,
    column_types,
    config,
    field,
    send,
    struct,
)
from lib.fixtures.table import ICEBERG_EXTERNAL_VOLUME, IcebergTable

pytestmark = [
    pytest.mark.iceberg,
    pytest.mark.schema_evolution,
    pytest.mark.parametrize("connector_version", ["v4"], indirect=True),
]

SCHEMA = struct(
    ID_FIELD,
    field("SMALL", "int16"),
    field("NAME", "string"),
    field("EVENT_TIME", "int64", name=TIMESTAMP_NAME, version=1),
)

MAP_SCHEMA = struct(
    ID_FIELD,
    {
        "field": "ATTRS",
        "type": "map",
        "optional": True,
        "keys": {"type": "string"},
        "values": {"type": "int32"},
    },
)

# Iceberg timestamps are microseconds, so a millisecond Timestamp becomes TIMESTAMP_NTZ(6).
ICEBERG_COLUMNS = {
    "ID": "NUMBER(19,0)",
    "SMALL": "NUMBER(5,0)",
    "EVENT_TIME": "TIMESTAMP_NTZ(6)",
}


@pytest.mark.parametrize("iceberg_version", [2, 3])
def test_iceberg_columns_use_format_version_types(
    driver: KafkaDriver,
    create_topics: Callable,
    wait_for_rows: Callable,
    iceberg_version: int,
    create_custom_connector: Callable,
):
    """Columns get Iceberg types; a map field is VARIANT on v3 and goes to the DLQ on v2."""
    topic = create_topics(
        [f"evolve_schema_iceberg_v{iceberg_version}"], with_tables=False
    )[0]
    dlq_topic = f"dlq_{topic.lower()}"
    driver.createTopics(dlq_topic, partitionNum=1, replicationNum=1)
    table = IcebergTable(driver, topic, iceberg_version)
    try:
        connector = create_custom_connector(
            f"evolve_schema_iceberg_v{iceberg_version}",
            config(
                dlq_topic,
                topics=topic,
                **{
                    "snowflake.autocreate.table.type": "iceberg",
                    "snowflake.iceberg.create.table.options": (
                        f"EXTERNAL_VOLUME='{ICEBERG_EXTERNAL_VOLUME}' "
                        f"ICEBERG_VERSION={iceberg_version}"
                    ),
                },
            ),
        )
        driver.startConnectorWaitTime()

        payloads = [
            {"ID": i, "SMALL": 1, "NAME": "n", "EVENT_TIME": 1700000000000}
            for i in range(10)
        ]
        send(driver, topic, SCHEMA, payloads)
        wait_for_rows(topic, 10, connector_name=connector.name)
        types = column_types(table)
        assert {name: types[name] for name in ICEBERG_COLUMNS} == ICEBERG_COLUMNS
        assert types["NAME"].startswith("VARCHAR")

        send(driver, topic, MAP_SCHEMA, [{"ID": 10, "ATTRS": {"a": 1}}])
        if iceberg_version == 3:
            wait_for_rows(topic, 11, connector_name=connector.name)
            assert column_types(table)["ATTRS"] == "VARIANT"
        else:
            assert driver.consume_messages_dlq({"config": connector.config}, 0, 0) == 1
            assert "ATTRS" not in column_types(table)
    finally:
        table.drop()
        driver.deleteTopic(dlq_topic)
