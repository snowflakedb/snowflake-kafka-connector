"""Evolve-schema e2e tests for the per-pipe schema cache, restarts, and the outcome counters.

The connector sends each distinct value schema to evolveSchema once per pipe, and clears the
pipe's cache when a channel opens or reopens. Tests that count calls read the connector's
evolve-schema JMX counters (run_tests.sh --jmx) or mitmproxy's request counts
(run_tests.sh --with-mitmproxy).
"""

import time
from typing import Callable

import pytest

from lib.driver import KafkaDriver
from lib.evolve_schema import (
    ERR_NOT_ENABLED,
    ID_FIELD,
    MITMPROXY_OVERRIDE,
    TABLE_COLUMNS,
    column_types,
    config,
    envelope,
    field,
    require,
    send,
    start,
    struct,
    update_config,
    wait_for,
)

pytestmark = [
    pytest.mark.schema_evolution,
    pytest.mark.parametrize("connector_version", ["v4"], indirect=True),
]


def _schema(*extra_fields):
    return struct(ID_FIELD, *extra_fields)


# create_custom_connector is last in each signature so the connector is torn down before the
# table and its topic.


def test_interleaved_schema_versions_call_once_per_schema(
    driver: KafkaDriver,
    create_table: Callable,
    wait_for_rows: Callable,
    jolokia,
    create_custom_connector: Callable,
):
    """Two producers on two schema versions, interleaved record by record.

    The cache holds both schemas, so there are exactly two calls, not one per switch.
    """
    jolokia = require(jolokia, "--jmx")
    v1 = _schema(field("A", "int32"))
    v2 = _schema(field("A", "int32"), field("B", "int16"))
    table, connector = start(
        driver, create_table, create_custom_connector, "evolve_schema_interleaved"
    )
    values = [
        envelope(v1, {"ID": i, "A": i})
        if i % 2 == 0
        else envelope(v2, {"ID": i, "A": i, "B": 1})
        for i in range(200)
    ]
    driver.sendBytesData(table.name, values, [], partition=0)
    wait_for_rows(table.name, 200, connector_name=connector.name)

    assert column_types(table)["B"] == "NUMBER(5,0)"
    assert jolokia.evolve_schema_counters(connector.name)["call-count"] == 2


def test_many_schema_versions_call_once_each(
    driver: KafkaDriver,
    create_table: Callable,
    wait_for_rows: Callable,
    jolokia,
    create_custom_connector: Callable,
):
    """Twenty schema versions in quick succession, then all of them again.

    Each version adds one field. There's one call per version; the second pass makes none.
    """
    jolokia = require(jolokia, "--jmx")
    versions = [
        _schema(*(field(f"F{k}", "int16") for k in range(n + 1))) for n in range(20)
    ]
    table, connector = start(
        driver, create_table, create_custom_connector, "evolve_schema_versions"
    )

    def payload(version, i):
        return {"ID": i, **{f"F{k}": k for k in range(version + 1)}}

    for _ in range(2):
        values = [
            envelope(s, payload(n, n)) for n, s in enumerate(versions) for _ in range(5)
        ]
        driver.sendBytesData(table.name, values, [], partition=0)
    wait_for_rows(table.name, 200, connector_name=connector.name)

    types = column_types(table)
    assert all(types[f"F{k}"] == "NUMBER(5,0)" for k in range(20))
    assert jolokia.evolve_schema_counters(connector.name)["call-count"] == 20


def test_two_topics_into_one_table(
    driver: KafkaDriver,
    create_table: Callable,
    wait_for_rows: Callable,
    name_salt: str,
    create_custom_connector: Callable,
):
    """Two topics with different schemas map to one table, read by two tasks at once.

    The table ends with the union of both schemas' columns, each with its declared type.
    """
    # topic2table.map names are unquoted identifiers, so the table name must be uppercase.
    table = create_table("EVOLVE_SCHEMA_TWO_TOPICS", columns=TABLE_COLUMNS)
    topics = [f"evolve_schema_two_topics_{n}{name_salt}" for n in ("a", "b")]
    for topic in topics:
        driver.createTopics(topic, partitionNum=1, replicationNum=1)
    try:
        connector = create_custom_connector(
            "evolve_schema_two_topics",
            config(
                topics=",".join(topics),
                **{
                    "snowflake.topic2table.map": ",".join(
                        f"{t}:{table.name}" for t in topics
                    ),
                    "tasks.max": "2",
                },
            ),
        )
        driver.startConnectorWaitTime()

        schema_a = _schema(field("SHARED", "int32"), field("ONLY_A", "int8"))
        schema_b = _schema(field("SHARED", "int32"), field("ONLY_B", "boolean"))
        send(
            driver,
            topics[0],
            schema_a,
            [{"ID": i, "SHARED": 1, "ONLY_A": 2} for i in range(100)],
        )
        send(
            driver,
            topics[1],
            schema_b,
            [{"ID": i, "SHARED": 1, "ONLY_B": True} for i in range(100)],
        )
        wait_for_rows(table.name, 200, connector_name=connector.name)

        types = column_types(table)
        assert types["SHARED"] == "NUMBER(10,0)"
        assert types["ONLY_A"] == "NUMBER(3,0)"
        assert types["ONLY_B"] == "BOOLEAN"
        assert table.select_scalar("count(*)", 'WHERE "ONLY_A" IS NOT NULL') == 100
        assert table.select_scalar("count(*)", 'WHERE "ONLY_B" IS NOT NULL') == 100
    finally:
        for topic in topics:
            driver.deleteTopic(topic)


@pytest.mark.fault_injection
@pytest.mark.parametrize("restart", ["connector", "rebalance"])
def test_restart_mid_evolve(
    driver: KafkaDriver,
    create_table: Callable,
    wait_for_rows: Callable,
    mitmproxy,
    restart: str,
    create_custom_connector: Callable,
):
    """The connector restarts, or its tasks rebalance, while an evolve-schema call is retrying.

    The call is held with 503s; the restart rebuilds the cache, and the repeated call after the
    fault clears is a no-op for columns already added. No record is lost or duplicated.
    """
    mitmproxy = require(mitmproxy, "--with-mitmproxy")
    name = f"evolve_schema_restart_{restart}"
    schema = _schema(field("SMALL", "int16"))
    table, connector = start(
        driver,
        create_table,
        create_custom_connector,
        name,
        partitions=2,
        **MITMPROXY_OVERRIDE,
        **{"tasks.max": "2"},
    )
    mitmproxy.reset_counters()
    mitmproxy.set_evolve_schema_fault(503, "ERR_SERVICE_UNAVAILABLE", pipe=name)
    try:
        for partition in (0, 1):
            send(
                driver,
                table.name,
                schema,
                [
                    {"ID": i, "SMALL": 1}
                    for i in range(partition * 100, partition * 100 + 100)
                ],
                partition=partition,
            )
        wait_for(
            lambda: mitmproxy.get_evolve_schema_injected_count() >= 2,
            "the evolve-schema call to be retrying",
            timeout=120,
            interval=2,
        )
        if restart == "connector":
            driver.restartConnectorAndTasks(connector.name)
        else:
            update_config(
                driver, connector.name, {**connector.config, "tasks.max": "1"}
            )
        time.sleep(10)
    finally:
        mitmproxy.clear_evolve_schema_fault()

    wait_for_rows(table.name, 200, connector_name=connector.name, timeout=600)
    assert column_types(table)["SMALL"] == "NUMBER(5,0)"
    assert table.select_scalar('count(DISTINCT "ID")') == 200


@pytest.mark.fault_injection
@pytest.mark.parametrize(
    "error_code, cached",
    [
        # Snowflake declined the schema itself: the same schema isn't sent again.
        ("ERR_EVOLVE_SCHEMA_INVALID_SCHEMA", True),
        # Any other failure isn't cached: the next record with the schema tries again.
        ("ERR_EVOLVE_SCHEMA_DISABLED_ON_TABLE", False),
    ],
)
def test_failed_call_then_same_schema(
    driver: KafkaDriver,
    create_table: Callable,
    mitmproxy,
    error_code: str,
    cached: bool,
    create_custom_connector: Callable,
):
    """The first call for a schema fails with a 400; then more records with that schema arrive.

    The failed record goes to the DLQ. If Snowflake rejected the schema itself, the rejection
    is cached and every later record with it goes to the DLQ without another call. Otherwise the
    next record calls again, succeeds, and lands.
    """
    mitmproxy = require(mitmproxy, "--with-mitmproxy")
    name = f"evolve_schema_fail_{'cached' if cached else 'retried'}"
    schema = _schema(field("SMALL", "int16"))
    table, connector = start(
        driver,
        create_table,
        create_custom_connector,
        name,
        dlq=True,
        **MITMPROXY_OVERRIDE,
    )
    mitmproxy.reset_counters()
    mitmproxy.set_evolve_schema_fault(400, error_code, pipe=name, times=1)
    try:
        send(driver, table.name, schema, [{"ID": 0, "SMALL": 1}])
        assert driver.consume_messages_dlq({"config": connector.config}, 0, 0) == 1
        send(driver, table.name, schema, [{"ID": i, "SMALL": 1} for i in range(1, 10)])
        if cached:
            assert driver.consume_messages_dlq({"config": connector.config}, 0, 9) == 10
            assert table.select_scalar("count(*)") == 0
            assert mitmproxy.get_evolve_schema_requests(name) == 1
        else:
            wait_for(
                lambda: table.select_scalar("count(*)") == 9,
                "the 9 later records to land",
            )
            assert column_types(table)["SMALL"] == "NUMBER(5,0)"
            assert mitmproxy.get_evolve_schema_requests(name) == 2
    finally:
        mitmproxy.clear_evolve_schema_fault()


def test_outcome_counters(
    driver: KafkaDriver,
    create_table: Callable,
    wait_for_rows: Callable,
    jolokia,
    mitmproxy,
    create_custom_connector: Callable,
):
    """Each evolve-schema outcome increments its JMX counter, which telemetry also reports.

    A new schema counts a call. A schema Snowflake declines counts a rejection. With mitmproxy,
    a 403 not-enabled counts once, and exhausted retries count a redelivery.
    """
    jolokia = require(jolokia, "--jmx")
    name = "evolve_schema_counters"
    config_kwargs = MITMPROXY_OVERRIDE if mitmproxy else {}
    table, connector = start(
        driver, create_table, create_custom_connector, name, dlq=True, **config_kwargs
    )

    send(driver, table.name, _schema(field("A", "int16")), [{"ID": 0, "A": 1}])
    wait_for_rows(table.name, 1, connector_name=connector.name)
    declined = _schema(
        field(
            "AMOUNT",
            "bytes",
            name="org.apache.kafka.connect.data.Decimal",
            version=1,
            parameters={"scale": "0", "connect.decimal.precision": "39"},
        )
    )
    send(driver, table.name, declined, [{"ID": 1, "AMOUNT": "AQ=="}])
    driver.consume_messages_dlq({"config": connector.config}, 0, 0)
    counters = jolokia.evolve_schema_counters(connector.name)
    assert counters["call-count"] == 1
    assert counters["rejected-count"] == 1
    if mitmproxy is None:
        return

    mitmproxy.set_evolve_schema_fault(503, "ERR_SERVICE_UNAVAILABLE", pipe=name)
    try:
        send(driver, table.name, _schema(field("B", "int16")), [{"ID": 2, "B": 1}])
        wait_for(
            lambda: (
                jolokia.evolve_schema_counters(connector.name)["redelivered-count"] >= 1
            ),
            "a redelivery after the SDK's retries ran out",
            timeout=300,
        )
        mitmproxy.set_evolve_schema_fault(403, ERR_NOT_ENABLED, pipe=name)
        send(driver, table.name, _schema(field("C", "int16")), [{"ID": 3, "C": 1}])
        wait_for(
            lambda: (
                jolokia.evolve_schema_counters(connector.name)["not-enabled-count"] == 1
            ),
            "a not-enabled answer",
            timeout=300,
        )
    finally:
        mitmproxy.clear_evolve_schema_fault()
    wait_for_rows(table.name, 3, connector_name=connector.name)
