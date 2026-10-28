"""
E2E tests for routing connector traffic through a forward proxy.

Requires --with-https-proxy (Apache platform). The Connect worker can only
reach Snowflake through the https-proxy container, which counts calls per
listener port:

  8080  connector tier (jvm.proxy.host / jvm.proxy.port)
  8081  worker JVM tier (-Dhttps.proxyHost, set on the worker by the overlay)

Each test asserts which listener carried the JDBC and streaming SDK calls,
so a connector that silently falls back to the other tier fails the test.
"""

import json
import logging
import time

import pytest

from lib.https_proxy_control import CONNECTOR_TIER_PORT, JVM_TIER_PORT, PROXY_HOST

logger = logging.getLogger(__name__)

RECORD_COUNT = 100
UNREACHABLE_PROXY_PORT = 8099

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


def _assert_no_snowflake_traffic(counts: dict, port: int, status: dict):
    seen = {op: counts[op] for op in SNOWFLAKE_OPERATIONS if counts.get(op, 0)}
    assert not seen, (
        f"proxy port {port} unexpectedly saw {seen}; proxy status: {status}"
    )


def test_connector_proxy_carries_jdbc_and_sdk_traffic(
    driver, https_proxy, create_table, create_custom_connector, wait_for_rows
):
    """jvm.proxy.* routes JDBC and SDK calls through the connector tier, not the worker JVM tier."""
    https_proxy.reset()
    _ingest_through_proxy(
        "https_proxy_connector_tier",
        {
            "jvm.proxy.host": PROXY_HOST,
            "jvm.proxy.port": str(CONNECTOR_TIER_PORT),
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
    """Without jvm.proxy.*, JDBC and the SDK fall back to the worker's -Dhttps.proxyHost."""
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


def test_unreachable_connector_proxy_fails_without_fallback(
    driver, https_proxy, create_table, create_custom_connector
):
    """A wrong jvm.proxy.port fails the connector instead of falling back to the worker JVM proxy."""
    unsalted_name = "https_proxy_unreachable"
    https_proxy.reset()
    table = create_table(
        unsalted_name, columns='(record_metadata variant, "seq" varchar)'
    )
    driver.createTopics(table.name, partitionNum=1, replicationNum=1)
    config = _connector_config(
        **{
            "jvm.proxy.host": PROXY_HOST,
            "jvm.proxy.port": str(UNREACHABLE_PROXY_PORT),
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
