"""Evolve-schema e2e tests for failed calls, retries, flags, and partial rollout.

A schema Snowflake declines, or a missing pipe or table, sends the record to the error handler:
the DLQ with errors.tolerance=all, otherwise the task fails. 403 not-enabled falls back to the
backend's value-based schema evolution. Retryable failures are retried by the SDK, and a record
whose call ran out of retries is redelivered.

Fault tests route the SDK through mitmproxy (run_tests.sh --with-mitmproxy).
"""

import time
from typing import Callable

import pytest
from snowflake.connector.errors import ProgrammingError

from lib.driver import KafkaDriver
from lib.evolve_schema import (
    DECIMAL_NAME,
    ERR_NOT_ENABLED,
    ID_FIELD,
    MITMPROXY_OVERRIDE,
    column_types,
    config,
    field,
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
def test_declined_schema_by_errors_tolerance(
    driver: KafkaDriver,
    create_table: Callable,
    wait_for_rows: Callable,
    tolerance: str,
    create_custom_connector: Callable,
):
    """A 400 from evolve-schema: with all, the record goes to the DLQ and the next one ingests;
    with none, the task fails and nothing lands."""
    table, connector = start(
        driver,
        create_table,
        create_custom_connector,
        f"evolve_schema_tolerance_{tolerance}",
        dlq=tolerance == "all",
        errors_tolerance=tolerance,
    )
    send(driver, table.name, DECLINED_SCHEMA, [{"ID": 0, "AMOUNT": "AQ=="}])
    send(driver, table.name, SCHEMA, [{"ID": 1, "SMALL": 1}])

    if tolerance == "all":
        assert driver.consume_messages_dlq({"config": connector.config}, 0, 0) == 1
        wait_for_rows(table.name, 1, connector_name=connector.name)
        assert column_types(table)["SMALL"] == "NUMBER(5,0)"
    else:
        failed = wait_for_task_failure(driver, connector.name)
        assert "SfApiUserError" in failed[0].get("trace", ""), failed
        assert table.select_scalar("count(*)") == 0


@pytest.mark.parametrize("tolerance", ["all", "none"])
def test_schema_evolution_disabled_on_table(
    driver: KafkaDriver,
    create_table: Callable,
    tolerance: str,
    create_custom_connector: Callable,
):
    """ENABLE_SCHEMA_EVOLUTION=FALSE: 400 ERR_EVOLVE_SCHEMA_DISABLED_ON_TABLE and no DDL.

    The record goes to the DLQ with errors.tolerance=all; otherwise the task fails.
    """
    table, connector = start(
        driver,
        create_table,
        create_custom_connector,
        f"evolve_schema_disabled_{tolerance}",
        columns="(RECORD_METADATA VARIANT) ENABLE_SCHEMA_EVOLUTION = FALSE",
        dlq=tolerance == "all",
        errors_tolerance=tolerance,
    )
    send(driver, table.name, SCHEMA, PAYLOADS)

    if tolerance == "all":
        assert driver.consume_messages_dlq({"config": connector.config}, 0, 9) == 10
    else:
        failed = wait_for_task_failure(driver, connector.name)
        assert "ERR_EVOLVE_SCHEMA_DISABLED_ON_TABLE" in failed[0].get("trace", ""), (
            failed
        )
    assert list(column_types(table)) == ["RECORD_METADATA"]


def test_pipe_owner_without_evolve_schema_privilege(
    driver: KafkaDriver,
    create_table: Callable,
    name_salt: str,
    create_custom_connector: Callable,
):
    """The pipe's owner role has SELECT and INSERT on the table but not EVOLVE SCHEMA.

    The connector uses a user-defined pipe named after the table. Evolve-schema answers 400
    ERR_EVOLVE_SCHEMA_MISSING_PRIVILEGE, adds no column, and the record goes to the DLQ.
    """
    table = create_table(
        "evolve_schema_privilege", columns="(RECORD_METADATA VARIANT, ID NUMBER(19,0))"
    )
    cursor = driver.snowflake_conn.cursor()
    cursor.execute(f'ALTER TABLE "{table.name}" SET ENABLE_SCHEMA_EVOLUTION = TRUE')
    connector_role = driver.credentials.role
    pipe_owner = f"EVOLVE_SCHEMA_PIPE_OWNER{name_salt}"
    database, schema = driver.credentials.database, driver.credentials.schema
    try:
        cursor.execute(f"CREATE ROLE {pipe_owner}")
    except ProgrammingError as e:
        pytest.skip(f"The test role can't create roles: {e.msg}")
    try:
        for grant in (
            f"GRANT USAGE ON DATABASE {database} TO ROLE {pipe_owner}",
            f"GRANT USAGE, CREATE PIPE ON SCHEMA {database}.{schema} TO ROLE {pipe_owner}",
            f'GRANT SELECT, INSERT ON TABLE "{table.name}" TO ROLE {pipe_owner}',
            f"GRANT ROLE {pipe_owner} TO USER {driver.credentials.user}",
        ):
            cursor.execute(grant)
        cursor.execute(f"USE ROLE {pipe_owner}")
        cursor.execute(
            f'CREATE PIPE "{table.name}" AS COPY INTO "{table.name}" '
            f"FROM TABLE(DATA_SOURCE(TYPE => 'STREAMING')) "
            f"MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE"
        )
        cursor.execute(
            f'GRANT OPERATE, MONITOR ON PIPE "{table.name}" TO ROLE {connector_role}'
        )
        cursor.execute(f"USE ROLE {connector_role}")

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
        cursor.execute(f"USE ROLE {connector_role}")
        cursor.execute(f"DROP ROLE IF EXISTS {pipe_owner}")


def test_flag_change_restarts_tasks(
    driver: KafkaDriver,
    create_table: Callable,
    wait_for_rows: Callable,
    create_custom_connector: Callable,
):
    """Turning snowflake.feature.evolve.schema.enabled on restarts the tasks with it.

    With the flag off, the backend infers the type from the JSON value, so SMALL isn't
    NUMBER(5,0). After the config update, a new field gets its declared type.
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
    with_tiny = struct(ID_FIELD, field("SMALL", "int16"), field("TINY", "int8"))
    send(
        driver,
        table.name,
        with_tiny,
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
    """403 not-enabled turns the evolver off; once the account allows it, KC retries.

    The retry waits for the 5-minute cooldown. A schema sent before then falls back to
    value-inferred types; one sent after it gets its declared types.
    """
    mitmproxy = require(mitmproxy, "--with-mitmproxy")
    name = "evolve_schema_cooldown"
    mitmproxy.reset_counters()
    mitmproxy.set_evolve_schema_fault(403, ERR_NOT_ENABLED, pipe=name)
    try:
        table, connector = start(
            driver, create_table, create_custom_connector, name, **MITMPROXY_OVERRIDE
        )
        send(driver, table.name, SCHEMA, PAYLOADS)
        wait_for_rows(table.name, 10, connector_name=connector.name)
    finally:
        mitmproxy.clear_evolve_schema_fault()
    assert column_types(table)["SMALL"] != "NUMBER(5,0)"
    turned_off_at = time.monotonic()

    with_tiny = struct(ID_FIELD, field("SMALL", "int16"), field("TINY", "int8"))
    send(driver, table.name, with_tiny, [{"ID": 10, "SMALL": 1, "TINY": 1}])
    wait_for_rows(table.name, 11, connector_name=connector.name)
    assert column_types(table)["TINY"] != "NUMBER(3,0)", (
        "Called again within the cooldown"
    )
    assert mitmproxy.get_evolve_schema_requests(name) == 1

    time.sleep(max(0, 5 * 60 + 10 - (time.monotonic() - turned_off_at)))
    with_flag = struct(
        ID_FIELD,
        field("SMALL", "int16"),
        field("TINY", "int8"),
        field("FLAG", "boolean"),
    )
    send(
        driver, table.name, with_flag, [{"ID": 11, "SMALL": 1, "TINY": 1, "FLAG": True}]
    )
    wait_for_rows(table.name, 12, connector_name=connector.name)
    assert column_types(table)["FLAG"] == "BOOLEAN"
    assert mitmproxy.get_evolve_schema_requests(name) == 2


# Retryable evolve-schema failures, by HTTP status. 503 stands in for a GS owner-node restart or
# delegation timeout; 401 is an expired token, which the SDK refreshes before retrying.
RETRYABLE_FAULTS = {
    503: {"status": 503, "error_code": "ERR_SERVICE_UNAVAILABLE", "times": 3},
    429: {"status": 429, "error_code": "ERR_TOO_MANY_REQUESTS", "times": 3},
    401: {"status": 401, "error_code": "ERR_AUTH_TOKEN_EXPIRED", "times": 1},
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
    """Retryable evolve-schema failures are retried inside the SDK's budget.

    The call then succeeds, the column is added once, and each record is appended once.
    """
    mitmproxy = require(mitmproxy, "--with-mitmproxy")
    fault = RETRYABLE_FAULTS[status]
    name = f"evolve_schema_retry_{status}"
    mitmproxy.reset_counters()
    mitmproxy.set_evolve_schema_fault(**fault, pipe=name)
    try:
        table, connector = start(
            driver, create_table, create_custom_connector, name, **MITMPROXY_OVERRIDE
        )
        send(driver, table.name, SCHEMA, PAYLOADS)
        wait_for_rows(table.name, 10, connector_name=connector.name)
    finally:
        mitmproxy.clear_evolve_schema_fault()
    assert mitmproxy.get_evolve_schema_injected_count() == fault["times"]
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
    """Evolve-schema keeps failing past the SDK's 90-second retry budget, then recovers.

    The record is redelivered rather than appended without its columns. Once the fault clears,
    the next call succeeds; every record lands once, with its declared types.
    """
    mitmproxy = require(mitmproxy, "--with-mitmproxy")
    name = "evolve_schema_exhausted"
    mitmproxy.reset_counters()
    mitmproxy.set_evolve_schema_fault(503, "ERR_SERVICE_UNAVAILABLE", pipe=name)
    try:
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
    finally:
        mitmproxy.clear_evolve_schema_fault()

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
    """During rollout, an old client_side connector runs next to a new server_side one.

    Both have evolve-schema off. Both ingest, and neither makes an evolve-schema call.
    """
    jolokia = require(jolokia, "--jmx")
    old_table, old = start(
        driver,
        create_table,
        create_custom_connector,
        "evolve_schema_rollout_old",
        evolve_schema=False,
        **{"snowflake.validation": "client_side"},
    )
    new_table, new = start(
        driver,
        create_table,
        create_custom_connector,
        "evolve_schema_rollout_new",
        evolve_schema=False,
    )
    for table in (old_table, new_table):
        send(driver, table.name, SCHEMA, PAYLOADS)
    wait_for_rows(old_table.name, 10, connector_name=old.name)
    wait_for_rows(new_table.name, 10, connector_name=new.name)
    for connector in (old, new):
        assert jolokia.evolve_schema_counters(connector.name)["call-count"] == 0


@pytest.mark.fault_injection
def test_enabled_for_some_accounts_only(
    driver: KafkaDriver,
    create_table: Callable,
    wait_for_rows: Callable,
    mitmproxy,
    create_custom_connector: Callable,
):
    """The account parameter is on for some accounts only.

    The e2e account is one account, so a pipe-scoped 403 stands in for a disabled account:
    that connector falls back to value-inferred types while the other evolves.
    """
    mitmproxy = require(mitmproxy, "--with-mitmproxy")
    mitmproxy.reset_counters()
    mitmproxy.set_evolve_schema_fault(
        403, ERR_NOT_ENABLED, pipe="evolve_schema_partial_off"
    )
    try:
        on_table, on = start(
            driver,
            create_table,
            create_custom_connector,
            "evolve_schema_partial_on",
            **MITMPROXY_OVERRIDE,
        )
        off_table, off = start(
            driver,
            create_table,
            create_custom_connector,
            "evolve_schema_partial_off",
            **MITMPROXY_OVERRIDE,
        )
        for table in (on_table, off_table):
            send(driver, table.name, SCHEMA, PAYLOADS)
        wait_for_rows(on_table.name, 10, connector_name=on.name)
        wait_for_rows(off_table.name, 10, connector_name=off.name)
    finally:
        mitmproxy.clear_evolve_schema_fault()
    assert column_types(on_table)["SMALL"] == "NUMBER(5,0)"
    assert column_types(off_table)["SMALL"] != "NUMBER(5,0)"
