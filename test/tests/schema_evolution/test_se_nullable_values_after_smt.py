"""Schema evolution with nullable values produced by an SMT.

Migrated from v3 ``TestSchemaEvolutionNullableValuesAfterSmt``.

An ``ExtractField$Value`` SMT extracts the ``optionalField``
sub-object.  Only every other event contains ``optionalField``, so
half the events produce null values and are dropped by
``behavior.on.null.values=IGNORE``.

Schema evolution creates ``INDEX`` (from the table DDL) and adds
``FROM_OPTIONAL_FIELD`` from the record payload.  The original
``INDEX`` column is NOT NULL; evolved columns must be nullable.
"""

import json

import pytest
from snowflake.connector import DictCursor

from lib.config_migration import V4_CONFIG_TEMPLATE

TOTAL_EVENTS = 200
EXPECTED_ROWS = 100


@pytest.mark.schema_evolution
@pytest.mark.compatibility
def test_se_nullable_values_after_smt(
    driver,
    connector_version,
    name_salt,
    create_connector,
    snowflake_table,
    wait_for_rows,
):
    table_name = f"se_nullable_values_after_smt{name_salt}"

    driver.snowflake_conn.cursor().execute(
        f"CREATE OR REPLACE TABLE {table_name} "
        f"(RECORD_METADATA VARIANT, INDEX NUMBER NOT NULL) "
        f"ENABLE_SCHEMA_EVOLUTION = TRUE"
    )
    driver.createTopics(table_name, partitionNum=1, replicationNum=1)

    connector = create_connector(
        v4_config={
            **V4_CONFIG_TEMPLATE,
            "topics": table_name,
            "tasks.max": "1",
            "key.converter": "org.apache.kafka.connect.storage.StringConverter",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter.schemas.enable": "false",
            "errors.tolerance": "none",
            "errors.log.enable": "true",
            "behavior.on.null.values": "IGNORE",
            "transforms": "extractField",
            "transforms.extractField.type": "org.apache.kafka.connect.transforms.ExtractField$Value",
            "transforms.extractField.field": "optionalField",
            "snowflake.client.validation.enabled": "true",
        }
    )
    connector_name = connector.name
    driver.startConnectorWaitTime()

    values = []
    for idx in range(TOTAL_EVENTS):
        event = {"index": idx, "someKey": "someValue"}
        if idx % 2 == 0:
            event["optionalField"] = {"index": idx, "from_optional_field": True}
        values.append(json.dumps(event).encode("utf-8"))
    driver.sendBytesData(table_name, values)

    wait_for_rows(table_name, EXPECTED_ROWS, connector_name=connector_name)

    # --- Verify table schema ---
    desc = (
        driver.snowflake_conn.cursor(DictCursor)
        .execute(f"DESCRIBE TABLE {table_name}")
        .fetchall()
    )

    gold = {
        "INDEX": {"type_prefix": "NUMBER", "nullable": "N"},
        "FROM_OPTIONAL_FIELD": {"type_prefix": "BOOLEAN", "nullable": "Y"},
        "RECORD_METADATA": {"type_prefix": "VARIANT", "nullable": "Y"},
    }
    col_map = {row["name"]: row for row in desc}
    for col_name, expected in gold.items():
        assert col_name in col_map, (
            f"Missing column {col_name}, got: {list(col_map.keys())}"
        )
        assert col_map[col_name]["type"].startswith(expected["type_prefix"]), (
            f"Column {col_name}: expected type starting with "
            f"{expected['type_prefix']}, got {col_map[col_name]['type']}"
        )
        assert col_map[col_name]["null?"] == expected["nullable"], (
            f"Column {col_name}: expected null?={expected['nullable']}, "
            f"got {col_map[col_name]['null?']}"
        )

    # --- Verify data ---
    rows = (
        driver.snowflake_conn.cursor(DictCursor)
        .execute(
            f"SELECT INDEX, FROM_OPTIONAL_FIELD, "
            f'RECORD_METADATA:"offset"::number AS OFFSET '
            f"FROM {table_name} ORDER BY OFFSET"
        )
        .fetchall()
    )

    assert len(rows) == EXPECTED_ROWS

    expected_indices = list(range(0, TOTAL_EVENTS, 2))
    for row, expected_idx in zip(rows, expected_indices):
        assert row["INDEX"] == expected_idx, (
            f"Expected INDEX={expected_idx}, got {row['INDEX']}"
        )
        assert row["FROM_OPTIONAL_FIELD"] is True
