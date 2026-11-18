"""Evolve-schema e2e tests with the Schema Registry Avro converter.

Covers how schema changes made through the registry show up in the table: new, defaulted,
removed, loosened, tightened, widened, incompatible, and renamed fields. Subjects use
compatibility NONE so every change can be registered. Confluent platform only.
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


def _avro(name, *fields):
    return avro.loads(
        json.dumps({"type": "record", "name": name, "fields": list(fields)})
    )


ID = {"name": "ID", "type": "long"}

# A table whose R column is NOT NULL, for the tests that loosen or drop a required field.
NOT_NULL_R_COLUMNS = (
    "(RECORD_METADATA VARIANT, ID NUMBER(19,0), R NUMBER(10,0) NOT NULL) "
    "ENABLE_SCHEMA_EVOLUTION = TRUE"
)


def _optional(name, type_):
    return {"name": name, "type": ["null", type_], "default": None}


def _no_compatibility_check(driver: KafkaDriver, topic: str):
    SchemaRegistryClient({"url": driver.schemaRegistryAddress}).set_compatibility(
        subject_name=f"{topic}-value", level="NONE"
    )


def _start_avro(driver, create_table, create_custom_connector, name, **config_kwargs):
    table, connector = start(
        driver,
        create_table,
        create_custom_connector,
        name,
        **{**AVRO_CONVERTER, **config_kwargs},
    )
    _no_compatibility_check(driver, table.name)
    return table, connector


def _send_avro(driver, topic, schema, records):
    driver.sendAvroSRData(topic, records, schema, key=[], key_schema="", partition=0)


# create_custom_connector is last in each signature so the connector is torn down before the
# table and its topic.


def test_add_optional_field(
    driver: KafkaDriver,
    create_table: Callable,
    wait_for_rows: Callable,
    create_custom_connector: Callable,
):
    """The column is added before the first v2 record lands; v1 records read NULL in it."""
    table, connector = _start_avro(
        driver, create_table, create_custom_connector, "evolve_schema_sr_add_optional"
    )
    v1 = _avro("V", ID, {"name": "A", "type": "int"})
    v2 = _avro("V", ID, {"name": "A", "type": "int"}, _optional("B", "int"))
    _send_avro(driver, table.name, v1, [{"ID": i, "A": 1} for i in range(10)])
    _send_avro(
        driver, table.name, v2, [{"ID": i, "A": 1, "B": 2} for i in range(10, 20)]
    )
    _send_avro(driver, table.name, v1, [{"ID": i, "A": 1} for i in range(20, 25)])
    wait_for_rows(table.name, 25, connector_name=connector.name)

    assert column_types(table)["B"] == "NUMBER(10,0)"
    assert nullable(table)["B"]
    assert table.select_scalar("count(*)", 'WHERE "B" = 2') == 10
    assert table.select_scalar("count(*)", 'WHERE "B" IS NULL') == 15


def test_add_field_with_default(
    driver: KafkaDriver,
    create_table: Callable,
    wait_for_rows: Callable,
    create_custom_connector: Callable,
):
    """A field with a default is added as a column; existing rows don't get the default."""
    table, connector = _start_avro(
        driver, create_table, create_custom_connector, "evolve_schema_sr_default"
    )
    v1 = _avro("V", ID)
    v2 = _avro("V", ID, {"name": "C", "type": "int", "default": 7})
    _send_avro(driver, table.name, v1, [{"ID": i} for i in range(10)])
    wait_for_rows(table.name, 10, connector_name=connector.name)
    _send_avro(driver, table.name, v2, [{"ID": i, "C": 7} for i in range(10, 20)])
    wait_for_rows(table.name, 20, connector_name=connector.name)

    assert column_types(table)["C"] == "NUMBER(10,0)"
    assert table.select_scalar("count(*)", 'WHERE "C" IS NULL') == 10


def test_remove_field_of_not_null_column(
    driver: KafkaDriver,
    create_table: Callable,
    wait_for_rows: Callable,
    create_custom_connector: Callable,
):
    """Removing the field of a NOT NULL column leaves the column alone.

    Evolve-schema adds every column as nullable, so the NOT NULL column is pre-created. The
    backend then drops NOT NULL for records without the field, and they land with NULL.
    """
    table, connector = _start_avro(
        driver,
        create_table,
        create_custom_connector,
        "evolve_schema_sr_remove",
        columns=NOT_NULL_R_COLUMNS,
    )
    v1 = _avro("V", ID, {"name": "R", "type": "int"})
    v2 = _avro("V", ID)
    _send_avro(driver, table.name, v1, [{"ID": i, "R": 1} for i in range(10)])
    wait_for_rows(table.name, 10, connector_name=connector.name)
    assert not nullable(table)["R"]

    _send_avro(driver, table.name, v2, [{"ID": i} for i in range(10, 20)])
    wait_for_rows(table.name, 20, connector_name=connector.name)
    assert column_types(table)["R"] == "NUMBER(10,0)"
    assert nullable(table)["R"]
    assert table.select_scalar("count(*)", 'WHERE "R" IS NULL') == 10


def test_required_field_becomes_optional(
    driver: KafkaDriver,
    create_table: Callable,
    wait_for_rows: Callable,
    create_custom_connector: Callable,
):
    """Evolve-schema drops NOT NULL when a required field becomes optional.

    The NOT NULL column is pre-created: evolve-schema adds columns as nullable even for
    required fields.
    """
    table, connector = _start_avro(
        driver,
        create_table,
        create_custom_connector,
        "evolve_schema_sr_loosen",
        columns=NOT_NULL_R_COLUMNS,
    )
    v1 = _avro("V", ID, {"name": "R", "type": "int"})
    v2 = _avro("V", ID, _optional("R", "int"))
    _send_avro(driver, table.name, v1, [{"ID": i, "R": 1} for i in range(10)])
    wait_for_rows(table.name, 10, connector_name=connector.name)
    assert not nullable(table)["R"]

    _send_avro(driver, table.name, v2, [{"ID": i, "R": None} for i in range(10, 20)])
    wait_for_rows(table.name, 20, connector_name=connector.name)
    assert nullable(table)["R"]
    assert table.select_scalar("count(*)", 'WHERE "R" IS NULL') == 10


def test_optional_field_becomes_required(
    driver: KafkaDriver,
    create_table: Callable,
    wait_for_rows: Callable,
    create_custom_connector: Callable,
):
    """NOT NULL is never added: an optional field turned required leaves the column nullable."""
    table, connector = _start_avro(
        driver, create_table, create_custom_connector, "evolve_schema_sr_tighten"
    )
    v1 = _avro("V", ID, _optional("O", "int"))
    v2 = _avro("V", ID, {"name": "O", "type": "int"})
    _send_avro(driver, table.name, v1, [{"ID": i, "O": None} for i in range(10)])
    _send_avro(driver, table.name, v2, [{"ID": i, "O": 1} for i in range(10, 20)])
    wait_for_rows(table.name, 20, connector_name=connector.name)

    assert nullable(table)["O"]


def test_widened_types(
    driver: KafkaDriver,
    create_table: Callable,
    wait_for_rows: Callable,
    create_custom_connector: Callable,
):
    """int to long, float to double, and a decimal scale increase keep the column types.

    A long that doesn't fit NUMBER(10,0) is a per-row error and the channel keeps going.
    Fractional values written into the NUMBER(10,0) decimal column round half away from zero.
    """
    table, connector = _start_avro(
        driver,
        create_table,
        create_custom_connector,
        "evolve_schema_sr_widen",
        errors_tolerance="all",
    )

    def dec(scale):
        return {
            "type": "bytes",
            "logicalType": "decimal",
            "precision": 10,
            "scale": scale,
        }

    v1 = _avro(
        "V",
        ID,
        {"name": "I", "type": "int"},
        {"name": "F", "type": "float"},
        {"name": "D", "type": dec(0)},
    )
    v2 = _avro(
        "V",
        ID,
        {"name": "I", "type": "long"},
        {"name": "F", "type": "double"},
        {"name": "D", "type": dec(1)},
    )
    _send_avro(
        driver,
        table.name,
        v1,
        [{"ID": i, "I": 1, "F": 1.5, "D": decimal.Decimal(1)} for i in range(10)],
    )
    wait_for_rows(table.name, 10, connector_name=connector.name)
    columns = column_types(table)

    v2_records = [
        {"ID": 10, "I": 2, "F": 2.5, "D": decimal.Decimal("2.5")},
        {"ID": 11, "I": 3, "F": 2.5, "D": decimal.Decimal("-2.5")},
        {"ID": 12, "I": 1 << 40, "F": 2.5, "D": decimal.Decimal(1)},
        {"ID": 13, "I": 4, "F": 2.5, "D": decimal.Decimal(1)},
    ]
    _send_avro(driver, table.name, v2, v2_records)
    wait_for_rows(table.name, 13, connector_name=connector.name)

    assert column_types(table) == columns
    assert columns["I"] == "NUMBER(10,0)"
    assert columns["D"] == "NUMBER(10,0)"
    assert table.select_scalar("count(*)", 'WHERE "ID" = 12') == 0
    rounded = {
        r["ID"]: int(r["D"])
        for r in table.select('"ID", "D"', 'WHERE "ID" IN (10, 11)')
    }
    assert rounded == {10: 3, 11: -3}
    assert table.select_scalar("count(*)", 'WHERE "ID" = 13') == 1


def test_incompatible_type_change(
    driver: KafkaDriver,
    create_table: Callable,
    wait_for_rows: Callable,
    create_custom_connector: Callable,
):
    """An int field changed to string keeps NUMBER(10,0); uncastable strings are row errors."""
    table, connector = _start_avro(
        driver,
        create_table,
        create_custom_connector,
        "evolve_schema_sr_incompatible",
        errors_tolerance="all",
    )
    v1 = _avro("V", ID, {"name": "N", "type": "int"})
    v2 = _avro("V", ID, {"name": "N", "type": "string"})
    _send_avro(driver, table.name, v1, [{"ID": i, "N": 1} for i in range(10)])
    wait_for_rows(table.name, 10, connector_name=connector.name)
    _send_avro(
        driver,
        table.name,
        v2,
        [{"ID": 10, "N": "abc"}, {"ID": 11, "N": "42"}, {"ID": 12, "N": "7"}],
    )
    wait_for_rows(table.name, 12, connector_name=connector.name)

    assert column_types(table)["N"] == "NUMBER(10,0)"
    assert table.select_scalar("count(*)", 'WHERE "ID" = 10') == 0
    assert table.select_scalar("count(*)", 'WHERE "N" = 42') == 1


def test_renamed_field(
    driver: KafkaDriver,
    create_table: Callable,
    wait_for_rows: Callable,
    create_custom_connector: Callable,
):
    """A field renamed with an Avro alias is a new column; the old column is kept."""
    table, connector = _start_avro(
        driver, create_table, create_custom_connector, "evolve_schema_sr_rename"
    )
    v1 = _avro("V", ID, {"name": "OLD_NAME", "type": "int"})
    v2 = _avro("V", ID, {"name": "NEW_NAME", "type": "int", "aliases": ["OLD_NAME"]})
    _send_avro(driver, table.name, v1, [{"ID": i, "OLD_NAME": 1} for i in range(10)])
    wait_for_rows(table.name, 10, connector_name=connector.name)
    _send_avro(
        driver, table.name, v2, [{"ID": i, "NEW_NAME": 2} for i in range(10, 20)]
    )
    wait_for_rows(table.name, 20, connector_name=connector.name)

    types = column_types(table)
    assert types["OLD_NAME"] == "NUMBER(10,0)"
    assert types["NEW_NAME"] == "NUMBER(10,0)"
    assert table.select_scalar("count(*)", 'WHERE "NEW_NAME" = 2') == 10


def test_avro_logical_types(
    driver: KafkaDriver,
    create_table: Callable,
    wait_for_rows: Callable,
    create_custom_connector: Callable,
):
    """Avro logical types through the AvroConverter get their mapped column types."""
    table, connector = _start_avro(
        driver, create_table, create_custom_connector, "evolve_schema_sr_logical"
    )
    schema = _avro(
        "V",
        ID,
        {
            "name": "L_DECIMAL",
            "type": {
                "type": "bytes",
                "logicalType": "decimal",
                "precision": 10,
                "scale": 2,
            },
        },
        {"name": "L_DATE", "type": {"type": "int", "logicalType": "date"}},
        {"name": "L_TIME", "type": {"type": "int", "logicalType": "time-millis"}},
        {
            "name": "L_TS_MILLIS",
            "type": {"type": "long", "logicalType": "timestamp-millis"},
        },
        {
            "name": "L_TS_MICROS",
            "type": {"type": "long", "logicalType": "timestamp-micros"},
        },
        {"name": "L_FIXED", "type": {"type": "fixed", "name": "F16", "size": 16}},
    )
    now = datetime.datetime(2023, 11, 14, 22, 13, 20, tzinfo=datetime.timezone.utc)
    records = [
        {
            "ID": i,
            "L_DECIMAL": decimal.Decimal("12.34"),
            "L_DATE": now.date(),
            "L_TIME": datetime.time(1, 2, 3),
            "L_TS_MILLIS": now,
            "L_TS_MICROS": now,
            "L_FIXED": bytes(16),
        }
        for i in range(10)
    ]
    _send_avro(driver, table.name, schema, records)
    wait_for_rows(table.name, 10, connector_name=connector.name)

    types = column_types(table)
    assert {name: t for name, t in types.items() if name.startswith("L_")} == {
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


def test_avro_primitives_and_containers(
    driver: KafkaDriver,
    create_table: Callable,
    wait_for_rows: Callable,
    create_custom_connector: Callable,
):
    """Avro primitives, enums, arrays, maps, and a three-type union get their mapped types."""
    table, connector = _start_avro(
        driver, create_table, create_custom_connector, "evolve_schema_sr_primitives"
    )
    schema = _avro(
        "V",
        ID,
        {"name": "P_INT", "type": "int"},
        {"name": "P_LONG", "type": "long"},
        {"name": "P_FLOAT", "type": "float"},
        {"name": "P_DOUBLE", "type": "double"},
        {"name": "P_STRING", "type": "string"},
        {"name": "P_UUID", "type": {"type": "string", "logicalType": "uuid"}},
        {
            "name": "P_ENUM",
            "type": {"type": "enum", "name": "Color", "symbols": ["RED", "BLUE"]},
        },
        {"name": "P_ARRAY", "type": {"type": "array", "items": "int"}},
        {"name": "P_MAP", "type": {"type": "map", "values": "int"}},
        {"name": "P_UNION", "type": ["null", "int", "string"], "default": None},
    )
    records = [
        {
            "ID": i,
            "P_INT": 1,
            "P_LONG": 2,
            "P_FLOAT": 1.5,
            "P_DOUBLE": 2.5,
            "P_STRING": "s",
            "P_UUID": str(uuid.UUID(int=i)),
            "P_ENUM": "RED",
            "P_ARRAY": [1, 2],
            "P_MAP": {"k": 1},
            "P_UNION": "u" if i % 2 else 3,
        }
        for i in range(10)
    ]
    _send_avro(driver, table.name, schema, records)
    wait_for_rows(table.name, 10, connector_name=connector.name)

    types = column_types(table)
    assert types["P_INT"] == "NUMBER(10,0)"
    assert types["P_LONG"] == "NUMBER(19,0)"
    assert types["P_FLOAT"] == "FLOAT"
    assert types["P_DOUBLE"] == "FLOAT"
    assert types["P_STRING"].startswith("VARCHAR")
    assert types["P_UUID"].startswith("VARCHAR")
    assert types["P_ENUM"].startswith("VARCHAR")
    assert types["P_ARRAY"] == "ARRAY"
    assert types["P_MAP"] == "VARIANT"
    assert types["P_UNION"] == "VARIANT"


class _TcpForwarder:
    """Forwards TCP connections from this container to the Schema Registry, until stopped.

    Stopping it closes the listener and every open connection, so the Connect worker sees the
    registry as down while producers in this container still reach it directly.
    """

    def __init__(self, target_host: str, target_port: int):
        self._target = (target_host, target_port)
        self._listener = socket.create_server(("0.0.0.0", 0))
        self.port = self._listener.getsockname()[1]
        self._open = []
        self._stopped = threading.Event()
        threading.Thread(target=self._accept, daemon=True).start()

    def _accept(self):
        while not self._stopped.is_set():
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
        finally:
            for s in (src, dst):
                try:
                    s.close()
                except OSError:
                    pass

    def stop(self):
        self._stopped.set()
        # Closing alone doesn't wake a thread blocked in accept(); shutting down does.
        try:
            self._listener.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self._listener.close()
        for s in self._open:
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

    Records with a schema ID the converter has cached keep flowing. A new schema ID fails in the
    converter: the record goes to the DLQ with errors.tolerance=all, otherwise the task fails.
    No evolve-schema call is made for the new schema.
    """
    jolokia = require(jolokia, "--jmx")
    registry_host, registry_port = driver.schemaRegistryAddress.split("://")[-1].split(
        ":"
    )
    forwarder = _TcpForwarder(registry_host, int(registry_port))
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
        v1 = _avro("V", ID, {"name": "A", "type": "int"})
        v2 = _avro("V", ID, {"name": "A", "type": "int"}, {"name": "B", "type": "int"})
        _send_avro(driver, table.name, v1, [{"ID": i, "A": 1} for i in range(10)])
        wait_for_rows(table.name, 10, connector_name=connector.name)

        forwarder.stop()
        _send_avro(driver, table.name, v1, [{"ID": i, "A": 1} for i in range(10, 20)])
        wait_for_rows(table.name, 20, connector_name=connector.name)
        _send_avro(driver, table.name, v2, [{"ID": 20, "A": 1, "B": 2}])

        if tolerance == "all":
            assert driver.consume_messages_dlq({"config": connector.config}, 0, 0) == 1
        else:
            wait_for_task_failure(driver, connector.name)
        assert "B" not in column_types(table)
        assert jolokia.evolve_schema_counters(connector.name)["call-count"] <= 1
    finally:
        forwarder.stop()
