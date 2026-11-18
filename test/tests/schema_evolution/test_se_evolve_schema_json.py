"""Evolve-schema e2e tests for JsonConverter records: type mapping and records it skips.

Each record is a JsonConverter envelope (schemas.enable=true), so the connector has a Kafka
Connect schema to send. The tests that check evolve-schema calls read the connector's
evolve-schema JMX counters, so they need run_tests.sh --jmx.
"""

import base64
from typing import Callable

import pytest

from lib.driver import KafkaDriver
from lib.evolve_schema import (
    BASE_MS,
    DATE_NAME,
    DECIMAL_NAME,
    ID_FIELD,
    TIME_NAME,
    TIMESTAMP_NAME,
    column_types,
    envelope,
    field,
    require,
    send,
    start,
    struct,
)

pytestmark = [
    pytest.mark.schema_evolution,
    pytest.mark.parametrize("connector_version", ["v4"], indirect=True),
]

ALL_TYPES_SCHEMA = struct(
    ID_FIELD,
    field("F_BOOLEAN", "boolean"),
    field("F_INT8", "int8"),
    field("F_INT16", "int16"),
    field("F_INT32", "int32"),
    field("F_INT64", "int64"),
    field("F_FLOAT32", "float"),
    field("F_FLOAT64", "double"),
    field("F_STRING", "string"),
    field("F_BYTES", "bytes"),
    field(
        "F_DECIMAL",
        "bytes",
        name=DECIMAL_NAME,
        version=1,
        parameters={"scale": "2", "connect.decimal.precision": "10"},
    ),
    field("F_DATE", "int32", name=DATE_NAME, version=1),
    field("F_TIME", "int32", name=TIME_NAME, version=1),
    field("F_TIMESTAMP", "int64", name=TIMESTAMP_NAME, version=1),
    {"field": "F_ARRAY", "type": "array", "optional": True, "items": {"type": "int32"}},
    {
        "field": "F_MAP",
        "type": "map",
        "optional": True,
        "keys": {"type": "string"},
        "values": {"type": "int32"},
    },
    {
        "field": "F_STRUCT",
        "type": "struct",
        "optional": True,
        "fields": [field("A", "int32")],
    },
)

ALL_TYPES_COLUMNS = {
    "RECORD_METADATA": "VARIANT",
    "ID": "NUMBER(19,0)",
    "F_BOOLEAN": "BOOLEAN",
    "F_INT8": "NUMBER(3,0)",
    "F_INT16": "NUMBER(5,0)",
    "F_INT32": "NUMBER(10,0)",
    "F_INT64": "NUMBER(19,0)",
    "F_FLOAT32": "FLOAT",
    "F_FLOAT64": "FLOAT",
    "F_DECIMAL": "NUMBER(10,2)",
    "F_DATE": "DATE",
    "F_TIME": "TIME(3)",
    "F_TIMESTAMP": "TIMESTAMP_NTZ(3)",
    "F_ARRAY": "ARRAY",
    "F_MAP": "VARIANT",
    "F_STRUCT": "VARIANT",
}


def _all_types_payload(i):
    return {
        "ID": i,
        "F_BOOLEAN": True,
        "F_INT8": 1,
        "F_INT16": 2,
        "F_INT32": 3,
        "F_INT64": 4,
        "F_FLOAT32": 1.5,
        "F_FLOAT64": 2.5,
        "F_STRING": "s",
        "F_BYTES": base64.b64encode(b"\x01\x02").decode(),
        # Decimal(12.34, scale 2) is the unscaled 1234 as big-endian bytes.
        "F_DECIMAL": base64.b64encode((1234).to_bytes(2, "big")).decode(),
        "F_DATE": 19675,
        "F_TIME": 3_600_000,
        "F_TIMESTAMP": BASE_MS,
        "F_ARRAY": [1, 2],
        "F_MAP": {"k": 1},
        "F_STRUCT": {"A": 1},
    }


# create_custom_connector is last in each signature so the connector is torn down before the
# table and its topic.


def test_json_converter_types(
    driver: KafkaDriver,
    create_table: Callable,
    wait_for_rows: Callable,
    create_custom_connector: Callable,
):
    """Every Kafka Connect type JsonConverter can carry gets its mapped column type."""
    table, connector = start(
        driver, create_table, create_custom_connector, "evolve_schema_json_types"
    )
    send(
        driver, table.name, ALL_TYPES_SCHEMA, [_all_types_payload(i) for i in range(10)]
    )
    wait_for_rows(table.name, 10, connector_name=connector.name)

    types = column_types(table)
    # The default lengths of VARCHAR and BINARY columns depend on the account.
    assert types.pop("F_STRING").startswith("VARCHAR")
    assert types.pop("F_BYTES").startswith("BINARY")
    assert types == ALL_TYPES_COLUMNS
    row = table.select('"F_DECIMAL", "F_DATE"::string AS D', 'WHERE "ID" = 0')[0]
    assert float(row["F_DECIMAL"]) == 12.34
    assert row["D"] == "2023-11-14"


def test_debezium_types_map_by_physical_type(
    driver: KafkaDriver,
    create_table: Callable,
    wait_for_rows: Callable,
    create_custom_connector: Callable,
):
    """Debezium's own logical types aren't Kafka Connect ones, so their physical type decides.

    io.debezium.time.Date is an int32 and MicroTimestamp an int64, so they become NUMBER columns,
    and ZonedTimestamp is a string.
    """
    schema = struct(
        ID_FIELD,
        field("D_DATE", "int32", name="io.debezium.time.Date", version=1),
        field("D_MICROS", "int64", name="io.debezium.time.MicroTimestamp", version=1),
        field("D_ZONED", "string", name="io.debezium.time.ZonedTimestamp", version=1),
    )
    table, connector = start(
        driver, create_table, create_custom_connector, "evolve_schema_debezium"
    )
    payloads = [
        {
            "ID": i,
            "D_DATE": 19675,
            "D_MICROS": BASE_MS * 1000,
            "D_ZONED": "2023-11-14T22:13:20Z",
        }
        for i in range(10)
    ]
    send(driver, table.name, schema, payloads)
    wait_for_rows(table.name, 10, connector_name=connector.name)

    types = column_types(table)
    assert types["D_DATE"] == "NUMBER(10,0)"
    assert types["D_MICROS"] == "NUMBER(19,0)"
    assert types["D_ZONED"].startswith("VARCHAR")


def test_field_names_are_used_exactly(
    driver: KafkaDriver,
    create_table: Callable,
    wait_for_rows: Callable,
    create_custom_connector: Callable,
):
    """Columns are added under each field's exact name; KC sends normalize_column_names=false.

    That covers spaces, dots, non-ASCII characters, reserved words, and two names that differ
    only by case. Column-identifier normalization is off, as it is by default in v4.
    """
    names = ["first name", "a.b", "naïve", "SELECT", "Mixed", "MIXED"]
    schema = struct(ID_FIELD, *(field(n, "int32") for n in names))
    table, connector = start(
        driver,
        create_table,
        create_custom_connector,
        "evolve_schema_names",
        **{"snowflake.compatibility.enable.column.identifier.normalization": "false"},
    )
    payloads = [{"ID": i, **{n: k for k, n in enumerate(names)}} for i in range(10)]
    send(driver, table.name, schema, payloads)
    wait_for_rows(table.name, 10, connector_name=connector.name)

    types = column_types(table)
    for name in names:
        assert types.get(name) == "NUMBER(10,0)", f"{name!r} missing: {sorted(types)}"
    row = table.select(", ".join(f'"{n}"' for n in names), 'WHERE "ID" = 0')[0]
    assert [row[n] for n in names] == list(range(len(names)))


def test_nested_struct_change_adds_no_column(
    driver: KafkaDriver,
    create_table: Callable,
    wait_for_rows: Callable,
    jolokia,
    create_custom_connector: Callable,
):
    """A new sub-field inside a nested struct is a new schema but needs no DDL.

    INFO stays VARIANT, no column is added, and each schema is sent once: sending both again
    makes no further evolve-schema call.
    """
    jolokia = require(jolokia, "--jmx")

    def info_schema(*sub_fields):
        return struct(
            ID_FIELD,
            {
                "field": "INFO",
                "type": "struct",
                "optional": True,
                "fields": list(sub_fields),
            },
        )

    first = info_schema(field("A", "int32"))
    second = info_schema(field("A", "int32"), field("B", "string"))
    table, connector = start(
        driver, create_table, create_custom_connector, "evolve_schema_nested"
    )

    send(driver, table.name, first, [{"ID": i, "INFO": {"A": i}} for i in range(5)])
    wait_for_rows(table.name, 5, connector_name=connector.name)
    columns = column_types(table)
    assert columns["INFO"] == "VARIANT"

    send(
        driver,
        table.name,
        second,
        [{"ID": i, "INFO": {"A": i, "B": "b"}} for i in range(5)],
    )
    send(driver, table.name, first, [{"ID": i, "INFO": {"A": i}} for i in range(5)])
    send(
        driver,
        table.name,
        second,
        [{"ID": i, "INFO": {"A": i, "B": "b"}} for i in range(5)],
    )
    wait_for_rows(table.name, 20, connector_name=connector.name)

    assert column_types(table) == columns
    assert jolokia.evolve_schema_counters(connector.name)["call-count"] == 2
    assert table.select_scalar("count(*)", "WHERE \"INFO\":B::string = 'b'") == 10


def test_key_schema_is_not_sent(
    driver: KafkaDriver,
    create_table: Callable,
    wait_for_rows: Callable,
    jolokia,
    create_custom_connector: Callable,
):
    """Only the value schema is sent: a key with its own struct schema adds no column."""
    jolokia = require(jolokia, "--jmx")
    value_schema = struct(ID_FIELD, field("V", "int16"))
    key_schema = struct(field("KEY_ONLY", "int64", optional=False), name="Key")
    table, connector = start(
        driver,
        create_table,
        create_custom_connector,
        "evolve_schema_key",
        **{
            "key.converter": "org.apache.kafka.connect.json.JsonConverter",
            "key.converter.schemas.enable": "true",
        },
    )
    keys = [envelope(key_schema, {"KEY_ONLY": i}) for i in range(10)]
    values = [envelope(value_schema, {"ID": i, "V": 1}) for i in range(10)]
    driver.sendBytesData(table.name, values, keys, partition=0)
    wait_for_rows(table.name, 10, connector_name=connector.name)

    types = column_types(table)
    assert "KEY_ONLY" not in types
    assert types["V"] == "NUMBER(5,0)"
    assert jolokia.evolve_schema_counters(connector.name)["call-count"] == 1


def _ingest_with_flag_on_and_off(
    driver,
    create_table,
    wait_for_rows,
    create_custom_connector,
    name,
    values,
    expected_rows,
    **config_kwargs,
):
    """Sends `values` through a connector with evolve-schema on and one with it off.

    Returns (on_connector, on_table, off_table) after both have ingested `expected_rows` rows.
    """
    on_table, on_connector = start(
        driver, create_table, create_custom_connector, f"{name}_on", **config_kwargs
    )
    off_table, off_connector = start(
        driver,
        create_table,
        create_custom_connector,
        f"{name}_off",
        evolve_schema=False,
        **config_kwargs,
    )
    for table in (on_table, off_table):
        driver.sendBytesData(table.name, values, [], partition=0)
    wait_for_rows(on_table.name, expected_rows, connector_name=on_connector.name)
    wait_for_rows(off_table.name, expected_rows, connector_name=off_connector.name)
    return on_connector, on_table, off_table


def test_schemaless_null_and_tombstone_records_skip_evolve_schema(
    driver: KafkaDriver,
    create_table: Callable,
    wait_for_rows: Callable,
    jolokia,
    create_custom_connector: Callable,
):
    """Records with no value schema make no evolve-schema call and ingest as with the flag off.

    The connector reads schemaless JSON (schemas.enable=false), so no record has a value schema.
    Null values and tombstones are mixed in; with the default behavior.on.null.values they
    ingest as rows too.
    """
    jolokia = require(jolokia, "--jmx")
    values = [f'{{"ID": {i}, "SMALL": 1}}'.encode() for i in range(10)]
    values += [b"null", None, b'{"ID": 99, "SMALL": 2}']
    on_connector, on_table, off_table = _ingest_with_flag_on_and_off(
        driver,
        create_table,
        wait_for_rows,
        create_custom_connector,
        "evolve_schema_schemaless",
        values,
        expected_rows=13,
        **{"value.converter.schemas.enable": "false"},
    )

    assert column_types(on_table) == column_types(off_table)
    assert jolokia.evolve_schema_counters(on_connector.name)["call-count"] == 0


def test_non_struct_value_schema_skips_evolve_schema(
    driver: KafkaDriver,
    create_table: Callable,
    jolokia,
    create_custom_connector: Callable,
):
    """A bare STRING value schema makes no evolve-schema call; ingestion matches the flag off.

    With schematization on, the connector can't map a non-struct value to columns, so it treats
    the record as broken before the evolve-schema step: both connectors send it to the DLQ.
    """
    jolokia = require(jolokia, "--jmx")
    values = [
        envelope({"type": "string", "optional": False}, f"v{i}") for i in range(10)
    ]
    on_table, on = start(
        driver,
        create_table,
        create_custom_connector,
        "evolve_schema_string_on",
        dlq=True,
    )
    off_table, off = start(
        driver,
        create_table,
        create_custom_connector,
        "evolve_schema_string_off",
        dlq=True,
        evolve_schema=False,
    )
    for table in (on_table, off_table):
        driver.sendBytesData(table.name, values, [], partition=0)
    for connector, table in ((on, on_table), (off, off_table)):
        assert driver.consume_messages_dlq({"config": connector.config}, 0, 9) == 10
        assert table.select_scalar("count(*)") == 0
    assert column_types(on_table) == column_types(off_table)
    assert jolokia.evolve_schema_counters(on.name)["call-count"] == 0
