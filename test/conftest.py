import logging
import os
import random
import string
import time
from pathlib import Path

import pytest

from lib.config import Profile
from lib.driver import KafkaDriver

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Custom CLI options
# ---------------------------------------------------------------------------


def pytest_addoption(parser):
    group = parser.getgroup("kafka-e2e", "Kafka connector end-to-end test options")
    group.addoption(
        "--kafka-address",
        required=True,
        help="Kafka bootstrap server address (e.g. kafka:9092)",
    )
    group.addoption(
        "--schema-registry-address",
        required=True,
        help="Schema registry URL (e.g. http://schema-registry:8081)",
    )
    group.addoption(
        "--kafka-connect-address",
        required=True,
        help="Kafka Connect REST address (e.g. kafka-connect:8083)",
    )
    group.addoption(
        "--platform",
        choices=["confluent", "apache"],
        required=True,
        help="Kafka platform: 'confluent' (with Schema Registry) or 'apache'",
    )
    group.addoption(
        "--platform-version",
        required=True,
        help="Kafka / Confluent platform version under test (e.g. 7.8.0)",
    )
    group.addoption(
        "--name-salt",
        default=None,
        help="Unique salt appended to connector and topic names (auto-generated if omitted)",
    )
    group.addoption(
        "--enable-ssl",
        action="store_true",
        default=False,
        help="Enable SSL for Kafka connections",
    )


def pytest_collection_modifyitems(config, items):
    if config.getoption("--platform") == "confluent":
        return
    skip = pytest.mark.skip(reason="requires Confluent platform (schema registry)")
    for item in items:
        if "confluent_only" in item.keywords:
            item.add_marker(skip)


# ---------------------------------------------------------------------------
# Session-scoped fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def credentials():
    credential_path = Path(os.environ["SNOWFLAKE_CREDENTIAL_FILE"])
    assert credential_path.is_file(), (
        f"SNOWFLAKE_CREDENTIAL_FILE={credential_path} does not exist"
    )
    return Profile.load(credential_path)


@pytest.fixture(scope="session")
def name_salt(request):
    salt = request.config.getoption("--name-salt")
    if salt is None:
        chars = string.ascii_letters + string.digits
        salt = "_" + "".join(random.choices(chars, k=7))
    logger.info("Using name salt: %s", salt)
    return salt


@pytest.fixture(scope="session")
def driver(request, credentials):
    return KafkaDriver(
        kafkaAddress=request.config.getoption("--kafka-address"),
        schemaRegistryAddress=request.config.getoption("--schema-registry-address"),
        kafkaConnectAddress=request.config.getoption("--kafka-connect-address"),
        credentials=credentials,
        testVersion=request.config.getoption("--platform-version"),
        enableSSL=request.config.getoption("--enable-ssl"),
    )


# ---------------------------------------------------------------------------
# Per-test fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def create_connector(driver, name_salt):
    """Factory fixture: call with a config filename to register a connector.

    All connectors created during the test are torn down automatically.
    """
    created = []

    def _create(config_filename: str):
        driver.createConnector(config_filename, name_salt)
        created.append(config_filename)

    yield _create

    for config_filename in reversed(created):
        driver.closeConnector(config_filename, name_salt)


@pytest.fixture()
def snowflake_table(driver, name_salt):
    """Factory fixture: call with a base name and a DDL statement to create a table.

    The table (and associated stage/pipe) is cleaned up after the test.
    """
    created = []

    def _create(base_name: str, ddl: str):
        topic = base_name + name_salt
        driver.snowflake_conn.cursor().execute(ddl)
        created.append(topic)
        return topic

    yield _create

    for topic in reversed(created):
        driver.cleanTableStagePipe(topic)
        driver.deleteTopic(topic)


@pytest.fixture(scope="session")
def wait_for_rows(driver):
    """Returns a polling helper that waits until a Snowflake table reaches the expected row count."""

    def _wait(table_name: str, expected: int, *, timeout: int = 600, interval: int = 10):
        deadline = time.monotonic() + timeout
        while True:
            count = driver.select_number_of_records(table_name)
            if count == expected:
                return count
            if time.monotonic() >= deadline:
                raise AssertionError(
                    f"Timed out waiting for {expected} rows in {table_name} "
                    f"(got {count} after {timeout}s)"
                )
            logger.info(
                "Waiting for %d rows in %s (currently %d), retrying in %ds...",
                expected, table_name, count, interval,
            )
            time.sleep(interval)

    return _wait
