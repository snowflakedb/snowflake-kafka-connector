"""Evolve-schema e2e tests with the Schema Registry Avro converter. Confluent platform only.

Subjects use compatibility NONE so every schema change can be registered.
"""

import datetime
import decimal
import json
import socket
import threading
import uuid
from typing import Callable

import pytest
from confluent_kafka import avro
from confluent_kafka.schema_registry import SchemaRegistryClient

from lib.driver import KafkaDriver
from lib.evolve_schema import (
    AVRO_CONVERTER,
    column_types,
    nullable,
    require,
    start,
    wait_for_task_failure,
)

pytestmark = [
    pytest.mark.schema_evolution,
    pytest.mark.confluent_only,
    pytest.mark.parametrize("connector_version", ["v4"], indirect=True),
]

ID = {"name": "ID", "type": "long"}


def _avro(*fields):
    return avro.loads(
        json.dumps({"type": "record", "name": "V", "fields": [ID, *fields]})
    )


def _f(name, type_, **extra):
    return {"name": name, "type": type_, **extra}


def _optional(name, type_):
    return {"name": name, "type": ["null", type_], "default": None}


def _decimal(scale, precision=10):
    return {
        "type": "bytes",
        "logicalType": "decimal",
        "precision": precision,
        "scale": scale,
    }


def _start_avro(driver, create_table, create_custom_connector, name, **config_kwargs):
    table, connector = start(
        driver,
        create_table,
        create_custom_connector,
        name,
        **{**AVRO_CONVERTER, **config_kwargs},
    )
    SchemaRegistryClient({"url": driver.schemaRegistryAddress}).set_compatibility(
        subject_name=f"{table.name}-value", level="NONE"
    )
    return table, connector


def _send(driver, topic, schema, records):
    driver.sendAvroSRData(topic, records, schema, key=[], key_schema="", partition=0)


# create_custom_connector is last in each signature so the connector is torn down before the
# table and its topic.


def test_added_renamed_and_tightened_fields(
    driver: KafkaDriver,
    create_table: Callable,
    wait_for_rows: Callable,
    create_custom_connector: Callable,
):
    """v2 adds an optional field and one with a default, renames one, and makes one required.

    New columns are added before the first v2 record lands, and earlier rows read NULL in them:
    a default isn't applied to existing rows. A renamed field (Avro alias) is a new column and
    the old one is kept. NOT NULL is never added for a field that becomes required.
    """
    table, connector = _start_avro(
        driver, create_table, create_custom_connector, "evolve_schema_sr_compatible"
    )
    v1 = _avro(_f("OLD_NAME", "int"), _optional("O", "int"))
    v2 = _avro(
        _f("NEW_NAME", "int", aliases=["OLD_NAME"]),
        _f("O", "int"),
        _optional("B", "int"),
        _f("C", "int", default=7),
    )
    _send(
        driver, table.name, v1, [{"ID": i, "OLD_NAME": 1, "O": None} for i in range(10)]
    )
    wait_for_rows(table.name, 10, connector_name=connector.name)
    _send(
        driver,
        table.name,
        v2,
        [{"ID": i, "NEW_NAME": 2, "O": 1, "B": 3, "C": 7} for i in range(10, 20)],
    )
    wait_for_rows(table.name, 20, connector_name=connector.name)

    types, nulls = column_types(table), nullable(table)
    assert {c: types[c] for c in ("OLD_NAME", "NEW_NAME", "B", "C")} == dict.fromkeys(
        ("OLD_NAME", "NEW_NAME", "B", "C"), "NUMBER(10,0)"
    )
    assert nulls["O"] and nulls["B"] and nulls["C"]
    assert table.select_scalar("count(*)", 'WHERE "B" IS NULL AND "C" IS NULL') == 10
    assert table.select_scalar("count(*)", 'WHERE "NEW_NAME" = 2') == 10


def test_removed_and_loosened_not_null_fields(
    driver: KafkaDriver,
    create_table: Callable,
    wait_for_rows: Callable,
    create_custom_connector: Callable,
):
    """v2 drops required field R and makes required field S optional.

    Evolve-schema adds columns as nullable even for required fields, so the NOT NULL columns
    are pre-created. Evolve-schema drops S's NOT NULL; the backend drops R's when records
    arrive without it. Both columns are kept, and v2 rows land with NULL in them.
    """
    table, connector = _start_avro(
        driver,
        create_table,
        create_custom_connector,
        "evolve_schema_sr_not_null",
        columns="(RECORD_METADATA VARIANT, ID NUMBER(19,0), R NUMBER(10,0) NOT NULL, "
        "S NUMBER(10,0) NOT NULL) ENABLE_SCHEMA_EVOLUTION = TRUE",
    )
    v1 = _avro(_f("R", "int"), _f("S", "int"))
    v2 = _avro(_optional("S", "int"))
    _send(driver, table.name, v1, [{"ID": i, "R": 1, "S": 1} for i in range(10)])
    wait_for_rows(table.name, 10, connector_name=connector.name)
    assert not any(nullable(table)[c] for c in ("R", "S"))

    _send(driver, table.name, v2, [{"ID": i, "S": None} for i in range(10, 20)])
    wait_for_rows(table.name, 20, connector_name=connector.name)
    assert all(nullable(table)[c] for c in ("R", "S"))
    assert table.select_scalar("count(*)", 'WHERE "R" IS NULL AND "S" IS NULL') == 10


def test_widened_and_incompatible_types(
    driver: KafkaDriver,
    create_table: Callable,
    wait_for_rows: Callable,
    create_custom_connector: Callable,
):
    """Column types never change: int to long, float to double, decimal scale up, int to string.

    Values that don't fit the column are per-row errors and the channel keeps going: a long
    past NUMBER(10,0), and a string that isn't a number. Fractional values written into the
    NUMBER(10,0) column round half away from zero.
    """
    table, connector = _start_avro(
        driver,
        create_table,
        create_custom_connector,
        "evolve_schema_sr_types_change",
        errors_tolerance="all",
    )
    v1 = _avro(_f("I", "int"), _f("F", "float"), _f("D", _decimal(0)), _f("N", "int"))
    v2 = _avro(
        _f("I", "long"), _f("F", "double"), _f("D", _decimal(1)), _f("N", "string")
    )
    one = decimal.Decimal(1)
    _send(
        driver,
        table.name,
        v1,
        [{"ID": i, "I": 1, "F": 1.5, "D": one, "N": 1} for i in range(10)],
    )
    wait_for_rows(table.name, 10, connector_name=connector.name)
    columns = column_types(table)

    _send(
        driver,
        table.name,
        v2,
        [
            {"ID": 10, "I": 2, "F": 2.5, "D": decimal.Decimal("2.5"), "N": "42"},
            {"ID": 11, "I": 3, "F": 2.5, "D": decimal.Decimal("-2.5"), "N": "7"},
            {"ID": 12, "I": 1 << 40, "F": 2.5, "D": one, "N": "8"},
            {"ID": 13, "I": 4, "F": 2.5, "D": one, "N": "abc"},
            {"ID": 14, "I": 5, "F": 2.5, "D": one, "N": "9"},
        ],
    )
    wait_for_rows(table.name, 13, connector_name=connector.name)

    assert column_types(table) == columns
    assert columns["I"] == columns["D"] == columns["N"] == "NUMBER(10,0)"
    landed = {r["ID"]: r for r in table.select('"ID", "D", "N"', 'WHERE "ID" >= 10')}
    assert sorted(landed) == [10, 11, 14]
    assert (int(landed[10]["D"]), int(landed[11]["D"])) == (3, -3)
    assert landed[10]["N"] == 42


def test_avro_type_mapping(
    driver: KafkaDriver,
    create_table: Callable,
    wait_for_rows: Callable,
    create_custom_connector: Callable,
):
    """Avro primitives, containers, and logical types get their mapped column types."""
    table, connector = _start_avro(
        driver, create_table, create_custom_connector, "evolve_schema_sr_types"
    )
    schema = _avro(
        _f("P_INT", "int"),
        _f("P_LONG", "long"),
        _f("P_FLOAT", "float"),
        _f("P_DOUBLE", "double"),
        _f("P_ENUM", {"type": "enum", "name": "Color", "symbols": ["RED", "BLUE"]}),
        _f("P_ARRAY", {"type": "array", "items": "int"}),
        _f("P_MAP", {"type": "map", "values": "int"}),
        _f("P_UNION", ["null", "int", "string"], default=None),
        _f("P_UUID", {"type": "string", "logicalType": "uuid"}),
        _f("L_DECIMAL", _decimal(2)),
        _f("L_DATE", {"type": "int", "logicalType": "date"}),
        _f("L_TIME", {"type": "int", "logicalType": "time-millis"}),
        _f("L_TS_MILLIS", {"type": "long", "logicalType": "timestamp-millis"}),
        _f("L_TS_MICROS", {"type": "long", "logicalType": "timestamp-micros"}),
        _f("L_FIXED", {"type": "fixed", "name": "F16", "size": 16}),
    )
    now = datetime.datetime(2023, 11, 14, 22, 13, 20, tzinfo=datetime.timezone.utc)
    records = [
        {
            "ID": i,
            "P_INT": 1,
            "P_LONG": 2,
            "P_FLOAT": 1.5,
            "P_DOUBLE": 2.5,
            "P_ENUM": "RED",
            "P_ARRAY": [1, 2],
            "P_MAP": {"k": 1},
            "P_UNION": "u" if i % 2 else 3,
            "P_UUID": str(uuid.UUID(int=i)),
            "L_DECIMAL": decimal.Decimal("12.34"),
            "L_DATE": now.date(),
            "L_TIME": datetime.time(1, 2, 3),
            "L_TS_MILLIS": now,
            "L_TS_MICROS": now,
            "L_FIXED": bytes(16),
        }
        for i in range(10)
    ]
    _send(driver, table.name, schema, records)
    wait_for_rows(table.name, 10, connector_name=connector.name)

    types = column_types(table)
    # The default length of VARCHAR columns depends on the account.
    assert all(types.pop(c).startswith("VARCHAR") for c in ("P_ENUM", "P_UUID"))
    assert {c: t for c, t in types.items() if c[:2] in ("P_", "L_")} == {
        "P_INT": "NUMBER(10,0)",
        "P_LONG": "NUMBER(19,0)",
        "P_FLOAT": "FLOAT",
        "P_DOUBLE": "FLOAT",
        "P_ARRAY": "ARRAY",
        "P_MAP": "VARIANT",
        "P_UNION": "VARIANT",
        "L_DECIMAL": "NUMBER(10,2)",
        "L_DATE": "DATE",
        "L_TIME": "TIME(3)",
        "L_TS_MILLIS": "TIMESTAMP_NTZ(3)",
        # The AvroConverter has no Kafka Connect type for timestamp-micros, so the Connect
        # schema it hands the connector is a plain int64. Mapping it to TIMESTAMP_NTZ(6) is
        # planned; update this when it lands.
        "L_TS_MICROS": "NUMBER(19,0)",
        "L_FIXED": "BINARY(16)",
    }


class _TcpForwarder:
    """Forwards TCP connections from this container to the Schema Registry until stopped.

    Once stopped, the Connect worker sees the registry as down, while producers in this
    container still reach it directly.
    """

    def __init__(self, target_host: str, target_port: int):
        self._target = (target_host, target_port)
        self._listener = socket.create_server(("0.0.0.0", 0))
        self.port = self._listener.getsockname()[1]
        self._open = []
        self._stopped = threading.Event()
        threading.Thread(target=self._accept, daemon=True).start()

    def _accept(self):
        while True:
            try:
                client, _ = self._listener.accept()
            except OSError:
                return
            if self._stopped.is_set():
                client.close()
                return
            upstream = socket.create_connection(self._target)
            self._open += [client, upstream]
            for src, dst in ((client, upstream), (upstream, client)):
                threading.Thread(
                    target=self._pipe, args=(src, dst), daemon=True
                ).start()

    @staticmethod
    def _pipe(src, dst):
        try:
            while data := src.recv(65536):
                dst.sendall(data)
        except OSError:
            pass

    def stop(self):
        self._stopped.set()
        # Closing alone doesn't wake a thread blocked in accept(); shutting down does.
        for s in [self._listener, *self._open]:
            try:
                s.shutdown(socket.SHUT_RDWR)
                s.close()
            except OSError:
                pass


@pytest.mark.parametrize("tolerance", ["all", "none"])
def test_schema_registry_outage(
    driver: KafkaDriver,
    create_table: Callable,
    wait_for_rows: Callable,
    jolokia,
    tolerance: str,
    create_custom_connector: Callable,
):
    """The Schema Registry goes down mid-run.

    Schema IDs the converter has cached keep flowing. A new schema ID fails in the converter:
    the record goes to the DLQ with errors.tolerance=all, otherwise the task fails. Either way
    there's no evolve-schema call for the new schema.
    """
    jolokia = require(jolokia, "--jmx")
    host, port = driver.schemaRegistryAddress.split("://")[-1].split(":")
    forwarder = _TcpForwarder(host, int(port))
    own_address = socket.gethostbyname(socket.gethostname())
    try:
        table, connector = _start_avro(
            driver,
            create_table,
            create_custom_connector,
            f"evolve_schema_sr_outage_{tolerance}",
            dlq=tolerance == "all",
            errors_tolerance=tolerance,
            **{
                "value.converter.schema.registry.url": f"http://{own_address}:{forwarder.port}"
            },
        )
        v1 = _avro(_f("A", "int"))
        _send(driver, table.name, v1, [{"ID": i, "A": 1} for i in range(10)])
        wait_for_rows(table.name, 10, connector_name=connector.name)

        forwarder.stop()
        _send(driver, table.name, v1, [{"ID": i, "A": 1} for i in range(10, 20)])
        wait_for_rows(table.name, 20, connector_name=connector.name)
        _send(
            driver,
            table.name,
            _avro(_f("A", "int"), _f("B", "int")),
            [{"ID": 20, "A": 1, "B": 2}],
        )

        if tolerance == "all":
            assert driver.consume_messages_dlq({"config": connector.config}, 0, 0) == 1
        else:
            wait_for_task_failure(driver, connector.name)
        assert "B" not in column_types(table)
        assert jolokia.evolve_schema_counters(connector.name)["call-count"] <= 1
    finally:
        forwarder.stop()
