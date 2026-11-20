"""Evolve-schema e2e tests for the per-pipe schema cache, restarts, and the outcome counters.

The connector sends each distinct value schema to evolveSchema once per pipe. Tests count calls
with the connector's JMX counters (--jmx) or mitmproxy's request counts (--with-mitmproxy).
"""

import time
from typing import Callable

import pytest

from lib.driver import KafkaDriver
from lib.evolve_schema import (
    DECIMAL_NAME,
    ERR_NOT_ENABLED,
    ERR_UNAVAILABLE,
    ID_FIELD,
    MITMPROXY_OVERRIDE,
    TABLE_COLUMNS,
    column_types,
    config,
    envelope,
    field,
    injected_fault,
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

SCHEMA = struct(ID_FIELD, field("SMALL", "int16"))


# create_custom_connector is last in each signature so the connector is torn down before the
# table and its topic.


def test_interleaved_schema_versions_call_once_each(
    driver: KafkaDriver,
    create_table: Callable,
    wait_for_rows: Callable,
    jolokia,
    create_custom_connector: Callable,
):
    """Twenty schema versions interleaved record by record, ten rounds: one call per version."""
    jolokia = require(jolokia, "--jmx")
    versions = [
        struct(ID_FIELD, *(field(f"F{k}", "int16") for k in range(n + 1)))
        for n in range(20)
    ]
    table, connector = start(
        driver, create_table, create_custom_connector, "evolve_schema_versions"
    )
    values = [
        envelope(schema, {"ID": i, **{f"F{k}": k for k in range(n + 1)}})
        for i in range(10)
        for n, schema in enumerate(versions)
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
    """Two topics with different schemas, read by two tasks, end with the union of columns."""
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

        for topic, (only, type_, value) in zip(
            topics, (("ONLY_A", "int8", 2), ("ONLY_B", "boolean", True))
        ):
            schema = struct(ID_FIELD, field("SHARED", "int32"), field(only, type_))
            send(
                driver,
                topic,
                schema,
                [{"ID": i, "SHARED": 1, only: value} for i in range(100)],
            )
        wait_for_rows(table.name, 200, connector_name=connector.name)

        types = column_types(table)
        assert (types["SHARED"], types["ONLY_A"], types["ONLY_B"]) == (
            "NUMBER(10,0)",
            "NUMBER(3,0)",
            "BOOLEAN",
        )
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
    """A restart or rebalance while a call is retrying rebuilds the cache; nothing is lost.

    After the fault clears, the repeated call finds the columns present, and every record lands
    exactly once.
    """
    mitmproxy = require(mitmproxy, "--with-mitmproxy")
    name = f"evolve_schema_restart_{restart}"
    table, connector = start(
        driver,
        create_table,
        create_custom_connector,
        name,
        partitions=2,
        **MITMPROXY_OVERRIDE,
        **{"tasks.max": "2"},
    )
    with injected_fault(mitmproxy, name, 503, ERR_UNAVAILABLE):
        for p in (0, 1):
            payloads = [{"ID": i, "SMALL": 1} for i in range(p * 100, p * 100 + 100)]
            send(driver, table.name, SCHEMA, payloads, partition=p)
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
    """The first call fails with a 400 and the record goes to the DLQ; then the schema repeats.

    A rejected schema is cached, so later records go to the DLQ without a call. After any other
    failure, the next record calls again and lands.
    """
    mitmproxy = require(mitmproxy, "--with-mitmproxy")
    name = f"evolve_schema_fail_{'cached' if cached else 'retried'}"
    table, connector = start(
        driver,
        create_table,
        create_custom_connector,
        name,
        dlq=True,
        **MITMPROXY_OVERRIDE,
    )
    dlq = {"config": connector.config}
    with injected_fault(mitmproxy, name, 400, error_code, times=1):
        send(driver, table.name, SCHEMA, [{"ID": 0, "SMALL": 1}])
        assert driver.consume_messages_dlq(dlq, 0, 0) == 1
        send(driver, table.name, SCHEMA, [{"ID": i, "SMALL": 1} for i in range(1, 10)])
        if cached:
            assert driver.consume_messages_dlq(dlq, 0, 9) == 10
            assert table.select_scalar("count(*)") == 0
        else:
            wait_for(
                lambda: table.select_scalar("count(*)") == 9,
                "the later records to land",
            )
            assert column_types(table)["SMALL"] == "NUMBER(5,0)"
        assert mitmproxy.get_evolve_schema_requests(name) == (1 if cached else 2)


def test_outcome_counters(
    driver: KafkaDriver,
    create_table: Callable,
    wait_for_rows: Callable,
    jolokia,
    mitmproxy,
    create_custom_connector: Callable,
):
    """Each evolve-schema outcome increments its JMX counter, which telemetry also reports.

    A new schema counts a call and a declined one a rejection. With mitmproxy, exhausted
    retries count a redelivery and a 403 counts not-enabled.
    """
    jolokia = require(jolokia, "--jmx")
    name = "evolve_schema_counters"
    table, connector = start(
        driver,
        create_table,
        create_custom_connector,
        name,
        dlq=True,
        **(MITMPROXY_OVERRIDE if mitmproxy else {}),
    )

    def counter(key):
        return jolokia.evolve_schema_counters(connector.name)[key]

    send(driver, table.name, struct(ID_FIELD, field("A", "int16")), [{"ID": 0, "A": 1}])
    wait_for_rows(table.name, 1, connector_name=connector.name)
    declined = struct(
        ID_FIELD,
        field(
            "AMOUNT",
            "bytes",
            name=DECIMAL_NAME,
            version=1,
            parameters={"scale": "0", "connect.decimal.precision": "39"},
        ),
    )
    send(driver, table.name, declined, [{"ID": 1, "AMOUNT": "AQ=="}])
    driver.consume_messages_dlq({"config": connector.config}, 0, 0)
    assert (counter("call-count"), counter("rejected-count")) == (1, 1)
    if mitmproxy is None:
        return

    with injected_fault(mitmproxy, name, 503, ERR_UNAVAILABLE):
        send(
            driver,
            table.name,
            struct(ID_FIELD, field("B", "int16")),
            [{"ID": 2, "B": 1}],
        )
        wait_for(lambda: counter("redelivered-count") >= 1, "a redelivery", timeout=300)
    with injected_fault(mitmproxy, name, 403, ERR_NOT_ENABLED):
        send(
            driver,
            table.name,
            struct(ID_FIELD, field("C", "int16")),
            [{"ID": 3, "C": 1}],
        )
        wait_for(
            lambda: counter("not-enabled-count") == 1,
            "a not-enabled answer",
            timeout=300,
        )
    wait_for_rows(table.name, 3, connector_name=connector.name)
