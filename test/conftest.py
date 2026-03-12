import logging
import os
import random
import string
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List

import pytest
import snowflake.connector
from _pytest.reports import TestReport

from lib.config import Profile, SnowflakeConnectorConfig
from lib.config_migration import v4_config_to_v3
from lib.driver import KafkaDriver

logger = logging.getLogger(__name__)

_PROTO_DIR = Path(__file__).parent / "test_data"


# ---------------------------------------------------------------------------
# Custom CLI options
# ---------------------------------------------------------------------------


def pytest_addoption(parser):
    """Register CLI options.

    Every option falls back to an environment variable so that tests can
    be launched inside a container where the compose file already sets
    the values -- no long CLI arg lists needed.
    """
    group = parser.getgroup("kafka-e2e", "Kafka connector end-to-end test options")
    group.addoption(
        "--kafka-address",
        default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS"),
        help="Kafka bootstrap server address (env: KAFKA_BOOTSTRAP_SERVERS)",
    )
    group.addoption(
        "--schema-registry-address",
        default=os.environ.get("SCHEMA_REGISTRY_URL", ""),
        help="Schema registry URL (env: SCHEMA_REGISTRY_URL)",
    )
    group.addoption(
        "--kafka-connect-address",
        default=os.environ.get("KAFKA_CONNECT_ADDRESS"),
        help="Kafka Connect REST address (env: KAFKA_CONNECT_ADDRESS)",
    )
    group.addoption(
        "--platform",
        choices=["confluent", "apache"],
        default=os.environ.get("KAFKA_PLATFORM"),
        help="Kafka platform: 'confluent' or 'apache' (env: KAFKA_PLATFORM)",
    )
    group.addoption(
        "--platform-version",
        default=os.environ.get("KAFKA_PLATFORM_VERSION"),
        help="Kafka / Confluent platform version (env: KAFKA_PLATFORM_VERSION)",
    )
    group.addoption(
        "--name-salt",
        default=os.environ.get("TEST_NAME_SALT"),
        help="Unique salt appended to connector and topic names (env: TEST_NAME_SALT, auto-generated if omitted)",
    )
    # currently unused, all tests run on all clouds
    group.addoption(
        "--cloud",
        choices=["AWS", "GCP", "AZURE"],
        default=os.environ.get("SF_CLOUD_PLATFORM"),
        help="Snowflake cloud platform: AWS, GCP, or AZURE (env: SF_CLOUD_PLATFORM)",
    )
    group.addoption(
        "--enable-ssl",
        action="store_true",
        default=os.environ.get("ENABLE_SSL", "").lower() in ("true", "1", "yes"),
        help="Enable SSL for Kafka connections (env: ENABLE_SSL)",
    )


_REQUIRED_OPTIONS = {
    "--kafka-address": "KAFKA_BOOTSTRAP_SERVERS",
    "--kafka-connect-address": "KAFKA_CONNECT_ADDRESS",
    "--platform": "KAFKA_PLATFORM",
    "--platform-version": "KAFKA_PLATFORM_VERSION",
}


def pytest_configure(config):
    # Validate required options (set via CLI or env var)
    missing = []
    for opt, env in _REQUIRED_OPTIONS.items():
        if config.getoption(opt) is None:
            missing.append(f"  {opt}  (or env {env})")
    if missing:
        raise pytest.UsageError(
            "Missing required configuration:\n" + "\n".join(missing)
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
def sensor_pb2():
    """Compile sensor.proto and return the generated module."""
    subprocess.run(
        ["protoc", "--python_out=.", "sensor.proto"],
        cwd=_PROTO_DIR,
        check=True,
    )
    import test_data.sensor_pb2

    return test_data.sensor_pb2


@pytest.fixture(scope="session")
def credentials_unsalted():
    """Load the credentials from the environment variable SNOWFLAKE_CREDENTIAL_FILE."""
    credential_path = Path(os.environ["SNOWFLAKE_CREDENTIAL_FILE"])
    assert credential_path.is_file(), (
        f"SNOWFLAKE_CREDENTIAL_FILE={credential_path} does not exist"
    )
    return Profile.load(credential_path)


@pytest.fixture(scope="session")
def session_name_salt(request):
    """Common name salt for all tests in this session."""
    salt = request.config.getoption("--name-salt")
    if salt is None:
        chars = string.ascii_letters + string.digits
        salt = "_" + "".join(random.choices(chars, k=7))
    logger.info(f"Using session name salt: {salt}")
    return salt


@pytest.fixture(scope="session")
def test_schema(credentials_unsalted, session_name_salt):
    """Create an isolated schema for this test session and drop it on teardown.

    The schema name is `<original_schema><session_name_salt>`.
    """
    original_schema = credentials_unsalted.schema
    salted_schema = f"{original_schema}{session_name_salt}"
    fqn = f"{credentials_unsalted.database}.{salted_schema}"

    conn_config = SnowflakeConnectorConfig.from_profile(credentials_unsalted)
    try:
        logger.info(f"Creating test schema: {fqn}")
        conn = snowflake.connector.connect(**conn_config.to_dict())
        conn.cursor().execute(f"CREATE SCHEMA IF NOT EXISTS {fqn}")
        yield salted_schema
    finally:
        logger.info(f"Dropping test schema: {fqn}")
        conn = snowflake.connector.connect(**conn_config.to_dict())
        conn.cursor().execute(f"DROP SCHEMA IF EXISTS {fqn} CASCADE")
        conn.close()


@pytest.fixture(scope="session")
def credentials(credentials_unsalted, test_schema):
    """Load the credentials from the environment variable SNOWFLAKE_CREDENTIAL_FILE and replaces the schema with its salted version.

    Mutating
    `credentials.schema` before the driver is built ensures that every
    Snowflake object (tables, pipes, channels) created by both the test
    harness and the Kafka connector lands in the throwaway schema.
    """
    credentials_unsalted.schema = test_schema
    return credentials_unsalted


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


@pytest.fixture(params=["v4", "v3"])
def connector_version(request):
    """The Snowflake Kafka Connector version under test.

    Every test that (transitively) depends on this fixture is automatically run twice:
    once for v4 and once for v3.
    """
    return request.param


@pytest.fixture
def name_salt(session_name_salt, connector_version):
    """Diversify names between test runs and connector versions."""
    if connector_version == "v3":
        return f"{session_name_salt}_v3"
    return session_name_salt


@pytest.fixture()
def create_connector(driver, name_salt, connector_version):
    """Factory fixture: call to register a connector for the current version.

    All connectors created during the test are torn down automatically.

    Args:
        v4_config_file: Config template for the v4 connector.
        v3_config_file: Optional separate config template for v3. When omitted,
            v4_config_file is auto-migrated via v4_config_to_v3.
    """
    created = []

    def _create(v4_config_filename: str):
        match connector_version:
            case "v3":
                logger.info(f"Will transform {v4_config_filename} to KC v3 config")
                config = driver.createConnector(
                    v4_config_filename,
                    name_salt,
                    config_transform=v4_config_to_v3,
                )
            case "v4":
                config = driver.createConnector(v4_config_filename, name_salt)
        created.append(v4_config_filename)
        return config

    try:
        yield _create
    finally:
        for config_filename in reversed(created):
            driver.closeConnector(config_filename, name_salt)


@pytest.fixture
def create_topic(driver: KafkaDriver, name_salt):
    """Factory fixture: call with a topic name to create a topic.

    The Kafka topic is cleaned up after the test.  The corresponding
    Snowflake table is left for the session-scoped `test_schema`
    teardown (`DROP SCHEMA ... CASCADE`) to remove.
    """
    created: List[str] = []

    def _create_one(topic, num_partitions, replication_factor):
        salted = f"{topic}{name_salt}"
        driver.createTopics(salted, num_partitions, replication_factor)
        driver.create_table(salted)
        return salted

    def _create(topics: List[str], *, num_partitions=1, replication_factor=1):
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [
                executor.submit(_create_one, t, num_partitions, replication_factor)
                for t in topics
            ]
            for future in as_completed(futures):
                created.append(future.result())
        return [f"{t}{name_salt}" for t in topics]

    try:
        yield _create
    finally:
        with ThreadPoolExecutor(max_workers=10) as executor:
            for _ in executor.map(driver.deleteTopic, created):
                pass


@pytest.fixture()
def snowflake_table(driver, name_salt):
    """Factory fixture: call with a base name and a DDL statement to create a table.

    The Kafka topic is cleaned up after the test.  The Snowflake table
    (and associated stage/pipe) is left for the session-scoped
    `test_schema` teardown (`DROP SCHEMA ... CASCADE`) to remove.
    """
    created = []

    def _create(base_name: str, ddl: str):
        topic = base_name + name_salt
        driver.snowflake_conn.cursor().execute(ddl)
        created.append(topic)
        return topic

    try:
        yield _create
    finally:
        for topic in created:
            driver.deleteTopic(topic)


@pytest.fixture(scope="session")
def wait_for_rows(driver):
    """Returns a polling helper that waits until a Snowflake table reaches the expected row count."""

    def _wait(
        table_name: str, expected: int, *, timeout: int = 600, interval: int = 10
    ):
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
                expected,
                table_name,
                count,
                interval,
            )
            time.sleep(interval)

    return _wait


# ---------------------------------------------------------------------------
# GitHub Actions step summary (failures only)
# ---------------------------------------------------------------------------

_github_summary_failures: List[TestReport] = []


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item):
    """Collect failed test reports for GITHUB_STEP_SUMMARY."""
    outcome = yield
    report = outcome.get_result()
    if report.when == "call" and report.failed and report.longrepr:
        _github_summary_failures.append(report)


def _python_error_annotation(report: TestReport) -> None:
    """Emit a ::error workflow command to stderr for GitHub annotations."""
    filename, line, domain = report.location
    parts = [f"file=test/{filename}", f"title={domain}"]
    if line is not None:
        parts.append(f"line={line + 1}")
    opts = ",".join(parts)
    message = report.longrepr.reprcrash.message
    print(f"::error {opts}::{message}", file=sys.stderr)


def pytest_sessionfinish(session, exitstatus):
    """Append failure summary to GITHUB_STEP_SUMMARY when set (e.g. in GitHub Actions)."""
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not summary_path or not _github_summary_failures or exitstatus == 0:
        return
    for report in _github_summary_failures:
        _python_error_annotation(report)
    try:
        with open(summary_path, "a", encoding="utf-8") as summary_file:
            summary_file.write("\n## Python test failures\n\n")
            for report in _github_summary_failures:
                summary_file.write(f"### {report.nodeid}\n\n")
                summary_file.write("```\n")
                summary_file.write(report.longreprtext)
                summary_file.write("\n```\n\n")
    except OSError:
        logger.debug("Could not write to GITHUB_STEP_SUMMARY", exc_info=True)
