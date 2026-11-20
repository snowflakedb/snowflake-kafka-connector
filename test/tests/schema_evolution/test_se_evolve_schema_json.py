"""Evolve-schema e2e tests for JsonConverter records: type mapping and records it skips.

Records are JsonConverter envelopes (schemas.enable=true). Tests that count evolve-schema calls
read the connector's JMX counters, so they need run_tests.sh --jmx.
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
    # Debezium's logical types aren't Kafka Connect ones, so their physical type decides.
    field("D_DATE", "int32", name="io.debezium.time.Date", version=1),
    field("D_MICROS", "int64", name="io.debezium.time.MicroTimestamp", version=1),
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
    "D_DATE": "NUMBER(10,0)",
    "D_MICROS": "NUMBER(19,0)",
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
        "D_DATE": 19675,
        "D_MICROS": BASE_MS * 1000,
    }


# create_custom_connector is last in each signature so the connector is torn down before the
# table and its topic.


def test_json_converter_types(
    driver: KafkaDriver,
    create_table: Callable,
    wait_for_rows: Callable,
    create_custom_connector: Callable,
):
    """Each Kafka Connect type JsonConverter can carry gets its mapped column type."""
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


def test_field_names_are_used_exactly(
    driver: KafkaDriver,
    create_table: Callable,
    wait_for_rows: Callable,
    create_custom_connector: Callable,
):
    """Columns get each field's exact name: KC sends normalize_column_names=false."""
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


def test_nested_struct_change_and_key_schema_add_no_column(
    driver: KafkaDriver,
    create_table: Callable,
    wait_for_rows: Callable,
    jolokia,
    create_custom_connector: Callable,
):
    """A new nested sub-field needs no DDL, and the key's schema is never sent.

    Each of the two value schemas is sent once, however often it repeats.
    """
    jolokia = require(jolokia, "--jmx")

    def info_schema(*sub_fields):
        info = {
            "field": "INFO",
            "type": "struct",
            "optional": True,
            "fields": list(sub_fields),
        }
        return struct(ID_FIELD, info)

    first = info_schema(field("A", "int32"))
    second = info_schema(field("A", "int32"), field("B", "string"))
    key_schema = struct(field("KEY_ONLY", "int64", optional=False), name="Key")
    table, connector = start(
        driver,
        create_table,
        create_custom_connector,
        "evolve_schema_nested",
        **{
            "key.converter": "org.apache.kafka.connect.json.JsonConverter",
            "key.converter.schemas.enable": "true",
        },
    )

    def send_with_keys(schema, payloads):
        keys = [envelope(key_schema, {"KEY_ONLY": p["ID"]}) for p in payloads]
        values = [envelope(schema, p) for p in payloads]
        driver.sendBytesData(table.name, values, keys, partition=0)

    send_with_keys(first, [{"ID": i, "INFO": {"A": i}} for i in range(5)])
    wait_for_rows(table.name, 5, connector_name=connector.name)
    columns = column_types(table)
    assert columns["INFO"] == "VARIANT"
    assert "KEY_ONLY" not in columns

    for schema in (second, first, second):
        send_with_keys(
            schema, [{"ID": i, "INFO": {"A": i, "B": "b"}} for i in range(5)]
        )
    wait_for_rows(table.name, 20, connector_name=connector.name)

    assert column_types(table) == columns
    assert jolokia.evolve_schema_counters(connector.name)["call-count"] == 2


def test_schemaless_null_and_tombstone_records_skip_evolve_schema(
    driver: KafkaDriver,
    create_table: Callable,
    wait_for_rows: Callable,
    jolokia,
    create_custom_connector: Callable,
):
    """Records without a value schema make no call and ingest as they do with the flag off.

    With the default behavior.on.null.values, null values and tombstones ingest as rows too.
    """
    jolokia = require(jolokia, "--jmx")
    values = [f'{{"ID": {i}, "SMALL": 1}}'.encode() for i in range(10)]
    values += [b"null", None, b'{"ID": 99, "SMALL": 2}']
    schemaless = {"value.converter.schemas.enable": "false"}
    tables = {}
    for flag in (True, False):
        table, connector = start(
            driver,
            create_table,
            create_custom_connector,
            f"evolve_schema_schemaless_{'on' if flag else 'off'}",
            evolve_schema=flag,
            **schemaless,
        )
        driver.sendBytesData(table.name, values, [], partition=0)
        wait_for_rows(table.name, 13, connector_name=connector.name)
        tables[flag] = (table, connector)

    assert column_types(tables[True][0]) == column_types(tables[False][0])
    assert jolokia.evolve_schema_counters(tables[True][1].name)["call-count"] == 0


def test_non_struct_value_schema_skips_evolve_schema(
    driver: KafkaDriver,
    create_table: Callable,
    jolokia,
    create_custom_connector: Callable,
):
    """A bare STRING value makes no call. It's a broken record with the flag on or off."""
    jolokia = require(jolokia, "--jmx")
    values = [
        envelope({"type": "string", "optional": False}, f"v{i}") for i in range(10)
    ]
    connectors = []
    for flag in (True, False):
        table, connector = start(
            driver,
            create_table,
            create_custom_connector,
            f"evolve_schema_string_{'on' if flag else 'off'}",
            dlq=True,
            evolve_schema=flag,
        )
        driver.sendBytesData(table.name, values, [], partition=0)
        assert driver.consume_messages_dlq({"config": connector.config}, 0, 9) == 10
        assert table.select_scalar("count(*)") == 0
        connectors.append(connector)
    assert jolokia.evolve_schema_counters(connectors[0].name)["call-count"] == 0
