import logging
import os
import sys
import time
from typing import Dict, List

import pytest
from _pytest.reports import TestReport

from lib.config_migration import v4_config_to_v3
from lib.driver import KafkaDriver
from lib.fixtures.session import (  # noqa: F401 — re-exported for pytest discovery
    sensor_pb2,
    credentials_unsalted,
    session_name_salt,
    test_schema,
    credentials,
    driver,
)
from lib.fixtures.connector import (  # noqa: F401
    create_topics,
    create_connector,
    create_custom_connector,
)
from lib.fixtures.table import (  # noqa: F401
    create_table,
    snowflake_table,
    create_iceberg_table,
    iceberg_external_volume,
)
from lib.fixtures.function import connector_version, name_salt  # noqa: F401

logger = logging.getLogger(__name__)


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
    # An empty salt silently resolves to the unsalted schema name, which is
    # shared with Java integration tests.  Dropping it would break those runs.
    name_salt_value = config.getoption("--name-salt")
    if name_salt_value is not None and name_salt_value == "":
        raise pytest.UsageError(
            "--name-salt / TEST_NAME_SALT must not be empty "
            "(omit it entirely to auto-generate, or provide a non-empty value)"
        )

    config.addinivalue_line(
        "markers", "schema_evolution: schema evolution e2e tests (FR6)"
    )
    config.addinivalue_line(
        "markers", "compatibility: v3/v4 dual-version compatibility tests"
    )
    config.addinivalue_line(
        "markers",
        "correctness: connector correctness tests (schema mapping, DLQ, multi-topic)",
    )
    config.addinivalue_line(
        "markers", "confluent_only: requires Confluent platform (schema registry)"
    )
    config.addinivalue_line("markers", "pressure: load / stress tests")
    config.addinivalue_line(
        "markers",
        "iceberg: iceberg table tests (requires ICEBERG_EXTERNAL_VOLUME)",
    )

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


@pytest.fixture()
def create_connector_from_file(
    driver: KafkaDriver,  # noqa: F811
    name_salt: str,  # noqa: F811
    connector_version: str,  # noqa: F811
):
    """DEPRECATED

    Factory fixture: call to register a connector for the current version.

    All connectors created during the test are torn down automatically.

    Args:
        v4_config_file: Config template for the v4 connector.
        v3_config_file: Optional separate config template for v3. When omitted,
            v4_config_file is auto-migrated via v4_config_to_v3.
    """
    created = []

    def _create(
        v4_config_filename: str, *, config_overrides: Dict[str, str] = None
    ) -> dict:
        def try_convert_and_apply_overrides(config: Dict[str, str]) -> Dict[str, str]:
            match connector_version:
                case "v3":
                    logger.info(f"Will transform {v4_config_filename} to KC v3 config")
                    config = v4_config_to_v3(config)
                case "v4":
                    pass
            if config_overrides:
                config.update(config_overrides)
            return config

        rest_request = driver.createConnector(
            name_salt=name_salt,
            rest_request_template_filename=v4_config_filename,
            config_transform=try_convert_and_apply_overrides,
        )
        created.append(rest_request["name"])
        return rest_request

    try:
        yield _create
    finally:
        for connector_name in reversed(created):
            driver.closeConnector(connector_name)


@pytest.fixture(scope="session")
def wait_for_rows(driver: KafkaDriver):  # noqa: F811 — pytest fixture injection, not a true redefinition
    """Returns a polling helper that waits until a Snowflake table reaches the expected row count.

    Supports an optional ``connector_name`` parameter: when provided, each
    poll iteration also checks the Kafka Connect task status via the REST API.
    If any task is in FAILED state the helper raises immediately instead of
    waiting for the full timeout -- a failed task will never produce more rows.

    Default timeout/interval can be overridden globally via environment
    variables ``E2E_WAIT_TIMEOUT`` and ``E2E_WAIT_INTERVAL``.
    """
    default_timeout = int(os.environ.get("E2E_WAIT_TIMEOUT", "300"))
    default_interval = int(os.environ.get("E2E_WAIT_INTERVAL", "5"))

    def _wait(
        table_name: str,
        expected: int,
        *,
        timeout: int | None = None,
        interval: int | None = None,
        at_least: bool = False,
        connector_name: str | None = None,
        max_consecutive_failures: int = 6,
    ):
        timeout = timeout or default_timeout
        interval = interval or default_interval
        deadline = time.monotonic() + timeout
        consecutive_failures = 0
        while True:
            count = driver.select_number_of_records(table_name)
            if count is not None:
                if count == expected:
                    return count
                if at_least and count > expected:
                    return count
                if not at_least and count > expected:
                    raise AssertionError(
                        f"Found more than {expected} rows in {table_name} (got {count})"
                    )
            if time.monotonic() >= deadline:
                raise AssertionError(
                    f"Timed out waiting for {expected} rows in {table_name} "
                    f"(got {count} after {timeout}s)"
                )
            if connector_name is not None:
                if failed := driver.get_failed_tasks(connector_name):
                    consecutive_failures += 1
                    if consecutive_failures >= max_consecutive_failures:
                        traces = "\n".join(
                            f"  task {t['id']}: {t.get('trace', 'no trace')}"
                            for t in failed
                        )
                        raise AssertionError(
                            f"Connector {connector_name} has FAILED tasks while "
                            f"waiting for {expected} rows in {table_name} "
                            f"(got {count}):\n{traces}"
                        )
                    logger.warning(
                        f"Connector {connector_name} has failed tasks "
                        f"({consecutive_failures}/{max_consecutive_failures}), "
                        f"waiting for recovery..."
                    )
                else:
                    consecutive_failures = 0
            logger.info(
                f"Waiting for {'at least ' if at_least else ''}{expected} rows "
                f"in {table_name} (currently {count}), retrying in {interval}s..."
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
    # longrepr can be a ReprExceptionInfo (has .reprcrash.message) or a plain
    # string (e.g. for xpass-strict failures).
    longrepr = report.longrepr
    if hasattr(longrepr, "reprcrash") and longrepr.reprcrash is not None:
        message = longrepr.reprcrash.message
    else:
        message = str(longrepr).split("\n", 1)[0]
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
