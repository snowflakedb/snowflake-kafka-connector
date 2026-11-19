"""Evolve-schema e2e tests for failed calls, retries, flags, and partial rollout.

A declined schema or missing privilege sends the record to the error handler: the DLQ with
errors.tolerance=all, otherwise the task fails. A 403 not-enabled falls back to value-inferred
types. Fault tests route the SDK through mitmproxy (run_tests.sh --with-mitmproxy).
"""

import time
from typing import Callable

import pytest
from snowflake.connector.errors import ProgrammingError

from lib.driver import KafkaDriver
from lib.evolve_schema import (
    DECIMAL_NAME,
    ERR_NOT_ENABLED,
    ERR_UNAVAILABLE,
    ID_FIELD,
    MITMPROXY_OVERRIDE,
    column_types,
    config,
    field,
    injected_fault,
    require,
    send,
    start,
    struct,
    update_config,
    wait_for_task_failure,
)

pytestmark = [
    pytest.mark.schema_evolution,
    pytest.mark.parametrize("connector_version", ["v4"], indirect=True),
]

SCHEMA = struct(ID_FIELD, field("SMALL", "int16"))
WITH_TINY = struct(ID_FIELD, field("SMALL", "int16"), field("TINY", "int8"))
PAYLOADS = [{"ID": i, "SMALL": 1} for i in range(10)]

# A decimal precision above 38 is one Snowflake can't store, so Snowflake declines the schema.
DECLINED_SCHEMA = struct(
    ID_FIELD,
    field(
        "AMOUNT",
        "bytes",
        name=DECIMAL_NAME,
        version=1,
        parameters={"scale": "0", "connect.decimal.precision": "39"},
    ),
)

# The SDK retries an evolve-schema call for up to 90 seconds.
SDK_RETRY_BUDGET_S = 90


# create_custom_connector is last in each signature so the connector is torn down before the
# table and its topic.


@pytest.mark.parametrize("tolerance", ["all", "none"])
@pytest.mark.parametrize("cause", ["declined_schema", "disabled_on_table"])
def test_rejected_call_by_errors_tolerance(
    driver: KafkaDriver,
    create_table: Callable,
    wait_for_rows: Callable,
    cause: str,
    tolerance: str,
    create_custom_connector: Callable,
):
    """A 400 from evolve-schema, for a declined schema or ENABLE_SCHEMA_EVOLUTION=FALSE.

    No DDL runs. With errors.tolerance=all the record goes to the DLQ and the task keeps going;
    with none, the task fails.
    """
    disabled = cause == "disabled_on_table"
    table, connector = start(
        driver,
        create_table,
        create_custom_connector,
        f"evolve_schema_{cause}_{tolerance}",
        columns=f"(RECORD_METADATA VARIANT) ENABLE_SCHEMA_EVOLUTION = {not disabled}",
        dlq=tolerance == "all",
        errors_tolerance=tolerance,
    )
    if disabled:
        send(driver, table.name, SCHEMA, [{"ID": 0, "SMALL": 1}])
    else:
        send(driver, table.name, DECLINED_SCHEMA, [{"ID": 0, "AMOUNT": "AQ=="}])
        send(driver, table.name, SCHEMA, [{"ID": 1, "SMALL": 1}])

    if tolerance == "all":
        assert driver.consume_messages_dlq({"config": connector.config}, 0, 0) == 1
        if not disabled:
            wait_for_rows(table.name, 1, connector_name=connector.name)
            assert column_types(table)["SMALL"] == "NUMBER(5,0)"
    else:
        trace = wait_for_task_failure(driver, connector.name)[0].get("trace", "")
        expected = (
            "ERR_EVOLVE_SCHEMA_DISABLED_ON_TABLE" if disabled else "SfApiUserError"
        )
        assert expected in trace, trace
        assert table.select_scalar("count(*)") == 0
    if disabled:
        assert list(column_types(table)) == ["RECORD_METADATA"]


def test_pipe_owner_without_evolve_schema_privilege(
    driver: KafkaDriver,
    create_table: Callable,
    name_salt: str,
    create_custom_connector: Callable,
):
    """The pipe's owner has SELECT and INSERT on the table but not EVOLVE SCHEMA.

    Evolve-schema answers 400 ERR_EVOLVE_SCHEMA_MISSING_PRIVILEGE, adds no column, and the
    record goes to the DLQ. The connector uses the user-defined pipe named after the table.
    """
    table = create_table(
        "evolve_schema_privilege",
        columns="(RECORD_METADATA VARIANT, ID NUMBER(19,0)) ENABLE_SCHEMA_EVOLUTION = TRUE",
    )
    cursor = driver.snowflake_conn.cursor()
    creds = driver.credentials
    pipe_owner = f"EVOLVE_SCHEMA_PIPE_OWNER{name_salt}"
    try:
        cursor.execute(f"CREATE ROLE {pipe_owner}")
    except ProgrammingError as e:
        pytest.skip(f"The test role can't create roles: {e.msg}")
    try:
        for statement in (
            f"GRANT USAGE ON DATABASE {creds.database} TO ROLE {pipe_owner}",
            f"GRANT USAGE, CREATE PIPE ON SCHEMA {creds.database}.{creds.schema} "
            f"TO ROLE {pipe_owner}",
            f'GRANT SELECT, INSERT ON TABLE "{table.name}" TO ROLE {pipe_owner}',
            f"GRANT ROLE {pipe_owner} TO USER {creds.user}",
            f"USE ROLE {pipe_owner}",
            f'CREATE PIPE "{table.name}" AS COPY INTO "{table.name}" '
            "FROM TABLE(DATA_SOURCE(TYPE => 'STREAMING')) MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE",
            f'GRANT OPERATE, MONITOR ON PIPE "{table.name}" TO ROLE {creds.role}',
            f"USE ROLE {creds.role}",
        ):
            cursor.execute(statement)

        driver.createTopics(table.name, partitionNum=1, replicationNum=1)
        dlq_topic = f"dlq_{table.name.lower()}"
        driver.createTopics(dlq_topic, partitionNum=1, replicationNum=1)
        connector = create_custom_connector(
            "evolve_schema_privilege", config(dlq_topic)
        )
        driver.startConnectorWaitTime()
        send(driver, table.name, SCHEMA, PAYLOADS)

        assert driver.consume_messages_dlq({"config": connector.config}, 0, 9) == 10
        assert list(column_types(table)) == ["RECORD_METADATA", "ID"]
    finally:
        cursor.execute(f"USE ROLE {creds.role}")
        cursor.execute(f"DROP ROLE IF EXISTS {pipe_owner}")


def test_flag_change_restarts_tasks(
    driver: KafkaDriver,
    create_table: Callable,
    wait_for_rows: Callable,
    create_custom_connector: Callable,
):
    """Turning snowflake.feature.evolve.schema.enabled on restarts the tasks with it.

    Before, SMALL gets a value-inferred type; after, a new field gets its declared type.
    """
    table, connector = start(
        driver,
        create_table,
        create_custom_connector,
        "evolve_schema_flag_toggle",
        evolve_schema=False,
    )
    send(driver, table.name, SCHEMA, PAYLOADS)
    wait_for_rows(table.name, 10, connector_name=connector.name)
    assert column_types(table)["SMALL"] != "NUMBER(5,0)"

    update_config(
        driver,
        connector.name,
        {**connector.config, "snowflake.feature.evolve.schema.enabled": "true"},
    )
    driver.wait_for_connector_running(connector.name)
    send(
        driver,
        table.name,
        WITH_TINY,
        [{"ID": 10 + i, "SMALL": 1, "TINY": 1} for i in range(10)],
    )
    wait_for_rows(table.name, 20, connector_name=connector.name)
    assert column_types(table)["TINY"] == "NUMBER(3,0)"


@pytest.mark.fault_injection
def test_not_enabled_then_enabled_after_cooldown(
    driver: KafkaDriver,
    create_table: Callable,
    wait_for_rows: Callable,
    mitmproxy,
    create_custom_connector: Callable,
):
    """A 403 not-enabled turns the evolver off for 5 minutes; then KC calls again.

    During the cooldown, new fields get value-inferred types; after it, declared types.
    """
    mitmproxy = require(mitmproxy, "--with-mitmproxy")
    name = "evolve_schema_cooldown"
    with injected_fault(mitmproxy, name, 403, ERR_NOT_ENABLED):
        table, connector = start(
            driver, create_table, create_custom_connector, name, **MITMPROXY_OVERRIDE
        )
        send(driver, table.name, SCHEMA, PAYLOADS)
        wait_for_rows(table.name, 10, connector_name=connector.name)
    assert column_types(table)["SMALL"] != "NUMBER(5,0)"
    turned_off_at = time.monotonic()

    send(driver, table.name, WITH_TINY, [{"ID": 10, "SMALL": 1, "TINY": 1}])
    wait_for_rows(table.name, 11, connector_name=connector.name)
    assert column_types(table)["TINY"] != "NUMBER(3,0)", (
        "Called again within the cooldown"
    )
    assert mitmproxy.get_evolve_schema_requests(name) == 1

    time.sleep(max(0, 5 * 60 + 10 - (time.monotonic() - turned_off_at)))
    with_flag = struct(*WITH_TINY["fields"], field("FLAG", "boolean"))
    send(
        driver, table.name, with_flag, [{"ID": 11, "SMALL": 1, "TINY": 1, "FLAG": True}]
    )
    wait_for_rows(table.name, 12, connector_name=connector.name)
    assert column_types(table)["FLAG"] == "BOOLEAN"
    assert mitmproxy.get_evolve_schema_requests(name) == 2


# Retryable failures by HTTP status. 503 stands in for a GS owner-node restart or delegation
# timeout; 401 is an expired token, which the SDK refreshes before retrying.
RETRYABLE_FAULTS = {
    503: (ERR_UNAVAILABLE, 3),
    429: ("ERR_TOO_MANY_REQUESTS", 3),
    401: ("ERR_AUTH_TOKEN_EXPIRED", 1),
}


@pytest.mark.fault_injection
@pytest.mark.parametrize("status", list(RETRYABLE_FAULTS))
def test_retryable_failures_are_retried(
    driver: KafkaDriver,
    create_table: Callable,
    wait_for_rows: Callable,
    mitmproxy,
    status: int,
    create_custom_connector: Callable,
):
    """Retryable failures are retried within the SDK's budget; each record lands once."""
    mitmproxy = require(mitmproxy, "--with-mitmproxy")
    error_code, times = RETRYABLE_FAULTS[status]
    name = f"evolve_schema_retry_{status}"
    with injected_fault(mitmproxy, name, status, error_code, times):
        table, connector = start(
            driver, create_table, create_custom_connector, name, **MITMPROXY_OVERRIDE
        )
        send(driver, table.name, SCHEMA, PAYLOADS)
        wait_for_rows(table.name, 10, connector_name=connector.name)
    assert mitmproxy.get_evolve_schema_injected_count() == times
    assert column_types(table)["SMALL"] == "NUMBER(5,0)"
    assert table.select_scalar('count(DISTINCT "ID")') == 10


@pytest.mark.fault_injection
def test_retries_run_out_then_redelivered(
    driver: KafkaDriver,
    create_table: Callable,
    wait_for_rows: Callable,
    mitmproxy,
    create_custom_connector: Callable,
):
    """Failures past the SDK's 90-second budget redeliver the record instead of appending it.

    Once the fault clears, the call succeeds and every record lands once.
    """
    mitmproxy = require(mitmproxy, "--with-mitmproxy")
    name = "evolve_schema_exhausted"
    with injected_fault(mitmproxy, name, 503, ERR_UNAVAILABLE):
        table, connector = start(
            driver, create_table, create_custom_connector, name, **MITMPROXY_OVERRIDE
        )
        send(driver, table.name, SCHEMA, PAYLOADS)
        time.sleep(SDK_RETRY_BUDGET_S + 30)
        assert table.select_scalar("count(*)") == 0, (
            "Appended before evolve-schema succeeded"
        )
        assert not driver.get_failed_tasks(connector.name), (
            "Task failed on the first redelivery"
        )

    wait_for_rows(table.name, 10, connector_name=connector.name)
    assert column_types(table)["SMALL"] == "NUMBER(5,0)"
    assert table.select_scalar('count(DISTINCT "ID")') == 10


def test_old_and_new_connectors_side_by_side(
    driver: KafkaDriver,
    create_table: Callable,
    wait_for_rows: Callable,
    jolokia,
    create_custom_connector: Callable,
):
    """An old client_side connector next to a new server_side one, both with the flag off.

    Both ingest, and neither makes an evolve-schema call.
    """
    jolokia = require(jolokia, "--jmx")
    connectors = []
    for name, overrides in (
        ("evolve_schema_rollout_old", {"snowflake.validation": "client_side"}),
        ("evolve_schema_rollout_new", {}),
    ):
        table, connector = start(
            driver,
            create_table,
            create_custom_connector,
            name,
            evolve_schema=False,
            **overrides,
        )
        send(driver, table.name, SCHEMA, PAYLOADS)
        connectors.append((table, connector))
    for table, connector in connectors:
        wait_for_rows(table.name, 10, connector_name=connector.name)
        assert jolokia.evolve_schema_counters(connector.name)["call-count"] == 0


@pytest.mark.fault_injection
def test_enabled_for_some_accounts_only(
    driver: KafkaDriver,
    create_table: Callable,
    wait_for_rows: Callable,
    mitmproxy,
    create_custom_connector: Callable,
):
    """With evolve-schema off for one account (403), that connector falls back; others evolve.

    The e2e run has one account, so a pipe-scoped 403 stands in for the disabled account.
    """
    mitmproxy = require(mitmproxy, "--with-mitmproxy")
    tables = {}
    with injected_fault(mitmproxy, "evolve_schema_partial_off", 403, ERR_NOT_ENABLED):
        for name in ("evolve_schema_partial_on", "evolve_schema_partial_off"):
            table, connector = start(
                driver,
                create_table,
                create_custom_connector,
                name,
                **MITMPROXY_OVERRIDE,
            )
            send(driver, table.name, SCHEMA, PAYLOADS)
            tables[name] = (table, connector)
        for table, connector in tables.values():
            wait_for_rows(table.name, 10, connector_name=connector.name)
    assert column_types(tables["evolve_schema_partial_on"][0])["SMALL"] == "NUMBER(5,0)"
    assert (
        column_types(tables["evolve_schema_partial_off"][0])["SMALL"] != "NUMBER(5,0)"
    )
