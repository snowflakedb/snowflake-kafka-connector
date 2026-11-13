"""
E2E tests for routing connector traffic through a forward proxy.

Requires --with-https-proxy (Apache platform). The Connect worker can only
reach Snowflake through the https-proxy container, which counts calls per
listener port:

  8080  connector tier (snowflake.https.proxy.host / snowflake.https.proxy.port)
  8081  worker JVM tier (-Dhttps.proxyHost, set on the worker by the overlay)
  8082  a second connector's proxy, for two connectors with different proxies

The connector picks one proxy (snowflake.https.proxy.*, else the worker's JVM proxy
system properties) and gives the same one to JDBC and the streaming SDK.
Each test asserts which listener carried the JDBC and SDK calls, so a
connector that silently falls back to the other tier fails the test.
"""

import json
import logging
import time

import pytest
import requests

from lib.https_proxy_control import (
    CONNECTOR_TIER_PORT,
    JVM_TIER_PORT,
    PROXY_HOST,
    SECOND_CONNECTOR_TIER_PORT,
)

logger = logging.getLogger(__name__)

RECORD_COUNT = 100
UNREACHABLE_PROXY_PORT = 8099
# JDK http.nonProxyHosts format, with wildcards. None of these match Snowflake.
JDK_STYLE_NON_PROXY_HOSTS = "localhost|127.*|*.internal.example.com"

# Calls the connector must make to ingest rows with JWT auth.
EXPECTED_OPERATIONS = (
    "jdbc_login",
    "sdk_get_subdomain_name",
    "sdk_open_channel",
    "sdk_insert_rows",
)
SNOWFLAKE_OPERATIONS = EXPECTED_OPERATIONS + (
    "jdbc_query",
    "sdk_token_downscoping",
    "sdk_get_pipe_info",
)

pytestmark = [
    pytest.mark.https_proxy,
    pytest.mark.parametrize("connector_version", ["v4"], indirect=True),
]


@pytest.fixture(autouse=True)
def _wait_for_the_previous_connector_to_go_quiet(https_proxy):
    """Runs after the test's own teardown, which deletes its connector and table."""
    yield
    https_proxy.wait_until_quiet()


def _connector_config(**proxy_config: str) -> dict:
    config = {
        "connector.class": "com.snowflake.kafka.connector.SnowflakeStreamingSinkConnector",
        "topics": "SNOWFLAKE_TEST_TOPIC",
        "tasks.max": "1",
        "snowflake.url.name": "SNOWFLAKE_HOST",
        "snowflake.user.name": "SNOWFLAKE_USER",
        "snowflake.private.key": "SNOWFLAKE_PRIVATE_KEY",
        "snowflake.database.name": "SNOWFLAKE_DATABASE",
        "snowflake.schema.name": "SNOWFLAKE_SCHEMA",
        "snowflake.role.name": "SNOWFLAKE_ROLE",
        "snowflake.enable.schematization": "false",
        "snowflake.streaming.validate.compatibility.with.classic": "false",
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
        "errors.tolerance": "none",
        "errors.log.enable": "true",
    }
    config.update(proxy_config)
    return config


def _ingest_through_proxy(
    unsalted_name,
    proxy_config,
    driver,
    create_table,
    create_custom_connector,
    wait_for_rows,
):
    table = create_table(
        unsalted_name, columns='(record_metadata variant, "seq" varchar)'
    )
    driver.createTopics(table.name, partitionNum=1, replicationNum=1)
    connector = create_custom_connector(
        unsalted_name, _connector_config(**proxy_config)
    )
    driver.wait_for_connector_running(connector.name, timeout=120)

    values = [json.dumps({"seq": i}).encode() for i in range(RECORD_COUNT)]
    driver.sendBytesData(table.name, values)
    wait_for_rows(table.name, RECORD_COUNT, connector_name=connector.name)


def _assert_carried_all_traffic(counts: dict, port: int, status: dict):
    missing = [op for op in EXPECTED_OPERATIONS if counts.get(op, 0) == 0]
    assert not missing, f"proxy port {port} never saw {missing}; proxy status: {status}"


def _assert_no_traffic_on_any_port(https_proxy, status: dict):
    for port in (CONNECTOR_TIER_PORT, JVM_TIER_PORT, SECOND_CONNECTOR_TIER_PORT):
        _assert_no_snowflake_traffic(https_proxy.counts(port), port, status)


def _assert_no_snowflake_traffic(counts: dict, port: int, status: dict):
    seen = {op: counts[op] for op in SNOWFLAKE_OPERATIONS if counts.get(op, 0)}
    assert not seen, (
        f"proxy port {port} unexpectedly saw {seen}; proxy status: {status}"
    )


def test_connector_proxy_carries_jdbc_and_sdk_traffic(
    driver, https_proxy, create_table, create_custom_connector, wait_for_rows
):
    """snowflake.https.proxy.* routes JDBC and SDK calls through the connector tier, not the worker JVM tier."""
    https_proxy.reset()
    _ingest_through_proxy(
        "https_proxy_connector_tier",
        {
            "snowflake.https.proxy.host": PROXY_HOST,
            "snowflake.https.proxy.port": str(CONNECTOR_TIER_PORT),
        },
        driver,
        create_table,
        create_custom_connector,
        wait_for_rows,
    )

    status = https_proxy.get_status()
    logger.info("Proxy status: %s", status)
    _assert_carried_all_traffic(
        https_proxy.counts(CONNECTOR_TIER_PORT), CONNECTOR_TIER_PORT, status
    )
    _assert_no_snowflake_traffic(
        https_proxy.counts(JVM_TIER_PORT), JVM_TIER_PORT, status
    )


def test_worker_jvm_proxy_used_without_connector_proxy(
    driver, https_proxy, create_table, create_custom_connector, wait_for_rows
):
    """Without snowflake.https.proxy.*, the worker's -Dhttps.proxyHost carries JDBC and SDK calls.

    JDBC reads the system properties itself. The SDK has no other route out (the
    worker sets no HTTPS_PROXY), so rows only land if the connector forwarded
    the JVM proxy to the SDK.
    """
    https_proxy.reset()
    _ingest_through_proxy(
        "https_proxy_jvm_tier",
        {},
        driver,
        create_table,
        create_custom_connector,
        wait_for_rows,
    )

    status = https_proxy.get_status()
    logger.info("Proxy status: %s", status)
    _assert_carried_all_traffic(
        https_proxy.counts(JVM_TIER_PORT), JVM_TIER_PORT, status
    )
    _assert_no_snowflake_traffic(
        https_proxy.counts(CONNECTOR_TIER_PORT), CONNECTOR_TIER_PORT, status
    )


def test_jdk_style_non_proxy_hosts_reach_sdk_unchanged(
    driver, https_proxy, create_table, create_custom_connector, wait_for_rows
):
    """snowflake.proxy.bypasshosts in JDK format (| separated, * wildcards) is accepted by JDBC and the SDK.

    The connector passes the list to the SDK as no_proxy without converting it.
    None of the entries match Snowflake, so all calls still go through the
    connector tier.
    """
    https_proxy.reset()
    _ingest_through_proxy(
        "https_proxy_jdk_non_proxy_hosts",
        {
            "snowflake.https.proxy.host": PROXY_HOST,
            "snowflake.https.proxy.port": str(CONNECTOR_TIER_PORT),
            "snowflake.proxy.bypasshosts": JDK_STYLE_NON_PROXY_HOSTS,
        },
        driver,
        create_table,
        create_custom_connector,
        wait_for_rows,
    )

    status = https_proxy.get_status()
    logger.info("Proxy status: %s", status)
    _assert_carried_all_traffic(
        https_proxy.counts(CONNECTOR_TIER_PORT), CONNECTOR_TIER_PORT, status
    )
    _assert_no_snowflake_traffic(
        https_proxy.counts(JVM_TIER_PORT), JVM_TIER_PORT, status
    )


def test_unreachable_connector_proxy_fails_without_fallback(
    driver, https_proxy, create_table, create_custom_connector
):
    """A wrong snowflake.https.proxy.port fails the connector instead of falling back to the worker JVM proxy."""
    unsalted_name = "https_proxy_unreachable"
    https_proxy.reset()
    table = create_table(
        unsalted_name, columns='(record_metadata variant, "seq" varchar)'
    )
    driver.createTopics(table.name, partitionNum=1, replicationNum=1)
    config = _connector_config(
        **{
            "snowflake.https.proxy.host": PROXY_HOST,
            "snowflake.https.proxy.port": str(UNREACHABLE_PROXY_PORT),
        }
    )

    # Connect runs the connector's validate(), which opens a JDBC connection.
    # Through an unreachable proxy that should reject the config. If a future
    # change lets creation through, the connector or its task must fail instead.
    try:
        connector = create_custom_connector(unsalted_name, config)
    except RuntimeError:
        logger.info("Connector creation rejected, as expected")
    else:
        driver.sendBytesData(table.name, [json.dumps({"seq": 0}).encode()])
        deadline = time.monotonic() + 180
        while True:
            status = driver.get_connector_status(connector.name) or {}
            if status.get("connector", {}).get(
                "state"
            ) == "FAILED" or driver.get_failed_tasks(connector.name):
                break
            assert time.monotonic() < deadline, f"Connector did not fail: {status}"
            time.sleep(5)
        assert not driver.select_number_of_records(table.name), (
            "rows landed through an unreachable proxy"
        )

    status = https_proxy.get_status()
    logger.info("Proxy status: %s", status)
    _assert_no_snowflake_traffic(
        https_proxy.counts(JVM_TIER_PORT), JVM_TIER_PORT, status
    )
    _assert_no_snowflake_traffic(
        https_proxy.counts(CONNECTOR_TIER_PORT), CONNECTOR_TIER_PORT, status
    )


def test_old_jvm_proxy_keys_are_rejected(
    driver, https_proxy, create_table, create_custom_connector
):
    """The jvm.proxy.* names were renamed to snowflake.https.proxy.*.

    Kafka Connect ignores keys it does not know, so a connector left on the old
    names would silently run without its proxy. It must be rejected instead,
    and validation must stop before it opens any connection.
    """
    unsalted_name = "https_proxy_old_keys"
    https_proxy.reset()
    table = create_table(
        unsalted_name, columns='(record_metadata variant, "seq" varchar)'
    )
    driver.createTopics(table.name, partitionNum=1, replicationNum=1)
    config = _connector_config(
        **{
            "jvm.proxy.host": PROXY_HOST,
            "jvm.proxy.port": str(CONNECTOR_TIER_PORT),
        }
    )

    with pytest.raises(RuntimeError):
        create_custom_connector(unsalted_name, config)

    status = https_proxy.get_status()
    logger.info("Proxy status: %s", status)
    _assert_no_snowflake_traffic(
        https_proxy.counts(JVM_TIER_PORT), JVM_TIER_PORT, status
    )
    _assert_no_snowflake_traffic(
        https_proxy.counts(CONNECTOR_TIER_PORT), CONNECTOR_TIER_PORT, status
    )


def test_two_connectors_in_one_worker_keep_their_own_proxies(
    driver, https_proxy, create_table, create_custom_connector, wait_for_rows
):
    """Two connectors in the same Connect worker, each with its own proxy.

    This is the case the per-connector proxy exists for. Before it, starting a
    connector wrote its proxy into JVM-wide system properties, so the last connector
    to start took over the other one's proxy. Both connectors are running before any
    data is sent, so a shared proxy would show up as one connector's traffic on the
    other's port. The proxy records the pipe names in SDK request paths per port,
    which tells the two connectors' streaming traffic apart.
    """
    https_proxy.reset()
    sides = []
    for unsalted_name, port in (
        ("https_proxy_two_connectors_a", CONNECTOR_TIER_PORT),
        ("https_proxy_two_connectors_b", SECOND_CONNECTOR_TIER_PORT),
    ):
        table = create_table(
            unsalted_name, columns='(record_metadata variant, "seq" varchar)'
        )
        driver.createTopics(table.name, partitionNum=1, replicationNum=1)
        sides.append((unsalted_name, port, table))

    connectors = [
        create_custom_connector(
            unsalted_name,
            _connector_config(
                **{
                    "snowflake.https.proxy.host": PROXY_HOST,
                    "snowflake.https.proxy.port": str(port),
                }
            ),
        )
        for unsalted_name, port, _ in sides
    ]
    for connector in connectors:
        driver.wait_for_connector_running(connector.name, timeout=120)

    values = [json.dumps({"seq": i}).encode() for i in range(RECORD_COUNT)]
    for _, _, table in sides:
        driver.sendBytesData(table.name, values)
    for connector, (_, _, table) in zip(connectors, sides):
        wait_for_rows(table.name, RECORD_COUNT, connector_name=connector.name)

    status = https_proxy.get_status()
    logger.info("Proxy status: %s", status)
    for _, port, table in sides:
        _assert_carried_all_traffic(https_proxy.counts(port), port, status)
        pipes = https_proxy.pipes(port)
        other_tables = [t.name.lower() for _, _, t in sides if t is not table]
        assert pipes, f"proxy port {port} saw no streaming pipe; proxy status: {status}"
        assert all(table.name.lower() in pipe for pipe in pipes), (
            f"proxy port {port} carried a pipe that is not {table.name}'s: {pipes}"
        )
        assert not any(o in pipe for pipe in pipes for o in other_tables), (
            f"proxy port {port} carried the other connector's pipe: {pipes}"
        )
    _assert_no_snowflake_traffic(
        https_proxy.counts(JVM_TIER_PORT), JVM_TIER_PORT, status
    )


def _validate_connector_config(driver, config: dict) -> dict:
    """Runs Kafka Connect's config validation and returns the errors per config key."""
    replacements = {
        "SNOWFLAKE_HOST": driver.credentials.make_url(),
        "SNOWFLAKE_DATABASE": driver.credentials.database,
        "SNOWFLAKE_SCHEMA": driver.credentials.schema,
        "SNOWFLAKE_USER": driver.credentials.user,
        "SNOWFLAKE_ROLE": driver.credentials.role,
        "SNOWFLAKE_PRIVATE_KEY": driver.credentials.private_key,
        "SNOWFLAKE_TEST_TOPIC": "https_proxy_partial_config",
    }
    resolved = {k: replacements.get(v, v) for k, v in config.items()}
    resolved["name"] = "https_proxy_partial_config"
    connector_class = resolved["connector.class"]
    url = (
        f"http://{driver.kafkaConnectAddress}/connector-plugins/"
        f"{connector_class}/config/validate"
    )
    response = requests.put(url, json=resolved, timeout=120)
    assert response.status_code == 200, (
        f"validate returned {response.status_code}: {response.text}"
    )
    return {
        entry["value"]["name"]: entry["value"]["errors"]
        for entry in response.json()["configs"]
        if entry["value"]["errors"]
    }


@pytest.mark.parametrize(
    "proxy_config, keys_with_errors",
    [
        pytest.param(
            {"snowflake.https.proxy.host": PROXY_HOST},
            {"snowflake.https.proxy.host", "snowflake.https.proxy.port"},
            id="host_without_port",
        ),
        pytest.param(
            {"snowflake.https.proxy.port": str(CONNECTOR_TIER_PORT)},
            {"snowflake.https.proxy.host", "snowflake.https.proxy.port"},
            id="port_without_host",
        ),
        pytest.param(
            {
                "snowflake.https.proxy.host": PROXY_HOST,
                "snowflake.https.proxy.port": " ",
            },
            {"snowflake.https.proxy.host", "snowflake.https.proxy.port"},
            id="blank_port",
        ),
        pytest.param(
            {
                "snowflake.https.proxy.host": PROXY_HOST,
                "snowflake.https.proxy.port": "not-a-port",
            },
            {"snowflake.https.proxy.port"},
            id="non_numeric_port",
        ),
        pytest.param(
            {
                "snowflake.https.proxy.host": PROXY_HOST,
                "snowflake.https.proxy.port": str(CONNECTOR_TIER_PORT),
                "snowflake.https.proxy.username": "proxy-user",
            },
            {"snowflake.https.proxy.username", "snowflake.https.proxy.password"},
            id="username_without_password",
        ),
    ],
)
def test_partial_proxy_config_fails_validation(
    driver, https_proxy, connector_version, proxy_config, keys_with_errors
):
    """An incomplete or invalid proxy config is rejected when the connector is validated.

    It must not be accepted and then run with the proxy silently ignored. The errors
    are reported against the offending keys, and validation stops before connecting,
    so no Snowflake call reaches any proxy listener.
    """
    https_proxy.reset()
    errors = _validate_connector_config(driver, _connector_config(**proxy_config))
    logger.info("Validation errors: %s", errors)

    assert set(errors) == keys_with_errors, (
        f"expected errors on exactly {sorted(keys_with_errors)}, got {errors}"
    )

    status = https_proxy.get_status()
    _assert_no_traffic_on_any_port(https_proxy, status)
