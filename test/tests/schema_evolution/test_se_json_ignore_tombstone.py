"""Schema evolution with tombstone filtering (behavior.on.null.values=IGNORE).

Migrated from v3 ``TestSchemaEvolutionJsonIgnoreTombstone``.

Two topics feed one table.  Each topic sends (RECORD_COUNT - 2)
real records plus a null and an empty-string tombstone.  With
``behavior.on.null.values=IGNORE`` the tombstones are dropped, so
the expected row count is ``2 * (RECORD_COUNT - 2)``.
Schema evolution must still create all expected columns.
"""

import json

import pytest

from lib.config_migration import V4_CONFIG_TEMPLATE

RECORD_COUNT = 100

RECORDS = [
    {
        "PERFORMANCE_STRING": "Excellent",
        "PERFORMANCE_CHAR": "A",
        "RATING_INT": 100,
    },
    {
        "PERFORMANCE_STRING": "Excellent",
        "RATING_DOUBLE": 0.99,
        "APPROVAL": True,
    },
]

GOLD_TYPES = {
    "PERFORMANCE_STRING": "VARCHAR",
    "PERFORMANCE_CHAR": "VARCHAR",
    "RATING_INT": "NUMBER",
    "RATING_DOUBLE": "FLOAT",
    "APPROVAL": "BOOLEAN",
    "RECORD_METADATA": "VARIANT",
}


@pytest.mark.schema_evolution
@pytest.mark.compatibility
def test_se_json_ignore_tombstone(
    driver,
    connector_version,
    name_salt,
    create_connector,
    snowflake_table,
    wait_for_rows,
):
    table_name = f"se_json_ignore_tombstone{name_salt}"
    topics = [f"{table_name}{i}" for i in range(2)]

    for t in topics:
        driver.createTopics(t, partitionNum=1, replicationNum=1)

    connector = create_connector(
        v4_config={
            **V4_CONFIG_TEMPLATE,
            "topics": ",".join(topics),
            "snowflake.topic2table.map": ",".join(f"{t}:{table_name}" for t in topics),
            "tasks.max": "1",
            "key.converter": "org.apache.kafka.connect.storage.StringConverter",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter.schemas.enable": "false",
            "errors.tolerance": "none",
            "errors.log.enable": "true",
            "behavior.on.null.values": "IGNORE",
            "snowflake.validation": "client_side",
        }
    )
    connector_name = connector.name
    driver.startConnectorWaitTime()

    for i, topic in enumerate(topics):
        real_count = RECORD_COUNT - 2
        keys = [
            json.dumps({"number": str(e)}).encode("utf-8") for e in range(real_count)
        ]
        values = [json.dumps(RECORDS[i]).encode("utf-8") for _ in range(real_count)]

        # Tombstones
        keys.append(json.dumps({"number": str(real_count)}).encode("utf-8"))
        values.append(None)
        keys.append(json.dumps({"number": str(real_count + 1)}).encode("utf-8"))
        values.append(b"")

        driver.sendBytesData(topic, values, keys)

    expected_rows = len(topics) * (RECORD_COUNT - 2)
    wait_for_rows(table_name, expected_rows, connector_name=connector_name)

    cols = {
        row[0]: row[1]
        for row in driver.snowflake_conn.cursor()
        .execute(f"DESCRIBE TABLE {table_name}")
        .fetchall()
    }
    for col_name, expected_prefix in GOLD_TYPES.items():
        assert col_name in cols, f"Missing column {col_name}, got: {list(cols.keys())}"
        assert cols[col_name].startswith(expected_prefix), (
            f"Column {col_name}: expected {expected_prefix}, got {cols[col_name]}"
        )
