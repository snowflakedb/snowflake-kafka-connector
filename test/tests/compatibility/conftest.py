"""Shared fixtures for data-type ingestion tests.

Provides two infrastructure patterns:
  1. Single-table batch connector (``results`` fixture) — module-scoped, creates
     one table + one topic + one connector per ingestion mode.  All test cases
     are defined as data in test_type_compatibility.py, sent in one batch,
     queried once, then asserted.  Used by test_type_compatibility.py.
  2. Per-test connector (``typed_table`` + ``standalone_ingest``) — function-
     scoped, one connector per test. Used by test_unsupported_types.py for types
     that crash streaming channels.
"""

import datetime
import json
import logging
import math
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal
from uuid import uuid4

import pytest
from confluent_kafka import Consumer as KafkaConsumer, KafkaError

from lib.config_migration import v4_config_to_v3
from lib.driver import quote_name

logger = logging.getLogger(__name__)

TEMPLATE_DIR = Path("rest_request_template")
BASE_TEMPLATE = "datatype_ingestion.json"


# ---------------------------------------------------------------------------
# Sentinel for unset expected_value
# ---------------------------------------------------------------------------


class _Unset:
    def __repr__(self):
        return "UNSET"


UNSET = _Unset()


# ---------------------------------------------------------------------------
# Test case definition
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Case:
    """A single test vector: send ``value`` to column ``col``, expect outcome.

    The optional ``group`` tag controls which test function owns this case.
    Tests filter on group to avoid name-based set lookups.
    """

    name: str
    col: str
    value: Any
    expect: Literal["ingested", "error"]
    expected_value: Any = UNSET
    approx: float | None = None
    group: str | None = None


def cases_where(*, col=None, expect=None, group=None, exclude_groups=None):
    """Filter CASES (from test_type_compatibility) by column, outcome, and/or group."""
    from .test_type_compatibility import CASES

    result = CASES
    if col is not None:
        result = [c for c in result if c.col == col]
    if expect is not None:
        result = [c for c in result if c.expect == expect]
    if group is not None:
        result = [c for c in result if c.group == group]
    if exclude_groups is not None:
        result = [c for c in result if c.group not in exclude_groups]
    return result


# ---------------------------------------------------------------------------
# Results dataclass
# ---------------------------------------------------------------------------


# Map DDL base types to comparison categories
_COMPARE_CATEGORIES = {
    "FLOAT": "float",
    "VARIANT": "json",
    "OBJECT": "json",
    "ARRAY": "json",
    "TIMESTAMP_LTZ": "timestamp_ltz",
    "TIMESTAMP_TZ": "timestamp_tz",
}


def _ddl_category(col: str, columns: dict) -> str:
    """Derive comparison category from a column's DDL type."""
    ddl = columns.get(col, "")
    base = ddl.split("(")[0].strip().upper()
    return _COMPARE_CATEGORIES.get(base, "exact")


@dataclass(frozen=True)
class Results:
    """Outcome of sending all CASES through one connector instance."""

    rows: dict
    dlq_ids: frozenset
    mode: str
    total_sent: int
    columns: dict  # {col_name: ddl_type} — used for comparison dispatch

    @property
    def total_ingested(self):
        return len(self.rows)

    @property
    def total_dlq(self):
        return len(self.dlq_ids)

    @property
    def total_missing(self):
        return self.total_sent - self.total_ingested - self.total_dlq

    def assert_ingested(self, case):
        """Assert that ``case`` landed in the table with the correct value."""
        assert case.name in self.rows, (
            f"[{case.name}] expected in table but not found "
            f"(mode={self.mode}, in_dlq={case.name in self.dlq_ids})"
        )
        actual = self.rows[case.name].get(case.col)

        if not isinstance(case.expected_value, _Unset):
            expected = case.expected_value
        else:
            expected = case.value

        if expected is None:
            assert actual is None, f"[{case.name}] expected NULL, got {actual!r}"
            return

        if case.approx is not None:
            assert float(actual) == pytest.approx(float(expected), abs=case.approx), (
                f"[{case.name}] approx mismatch: {actual!r} != {expected!r} ± {case.approx}"
            )
            return

        category = _ddl_category(case.col, self.columns)
        match category:
            case "float":
                self._compare_float(case.name, actual, expected)
            case "json":
                self._compare_json(case.name, actual, expected)
            case "timestamp_ltz":
                assert isinstance(actual, datetime.datetime), (
                    f"[{case.name}] expected datetime, got {type(actual).__name__}: {actual!r}"
                )
                assert actual.replace(tzinfo=None) == expected, (
                    f"[{case.name}] LTZ mismatch (tz-stripped): {actual!r} != {expected!r}"
                )
            case "timestamp_tz":
                assert isinstance(actual, datetime.datetime), (
                    f"[{case.name}] expected datetime, got {type(actual).__name__}: {actual!r}"
                )
                assert actual.tzinfo is not None, (
                    f"[{case.name}] expected tz-aware datetime, got naive: {actual!r}"
                )
            case _:
                assert actual == expected, (
                    f"[{case.name}] value mismatch: {actual!r} != {expected!r}"
                )

    def assert_error(self, case):
        """Assert that ``case`` did NOT land in the table (and hit DLQ if applicable)."""
        assert case.name not in self.rows, (
            f"[{case.name}] expected NOT in table but found: "
            f"{self.rows[case.name].get(case.col)!r} (mode={self.mode})"
        )
        # v4-ht has no DLQ — errors silently drop records server-side
        if self.mode != "v4-ht":
            assert case.name in self.dlq_ids, (
                f"[{case.name}] expected in DLQ but not found (mode={self.mode})"
            )

    @staticmethod
    def _compare_float(name, actual, expected):
        if isinstance(expected, str):
            exp_f = float(expected)
            if math.isnan(exp_f):
                assert math.isnan(float(actual)), (
                    f"[{name}] expected NaN, got {actual!r}"
                )
            else:
                assert float(actual) == exp_f, (
                    f"[{name}] expected {exp_f}, got {actual!r}"
                )
        else:
            assert actual == pytest.approx(expected, rel=1e-6), (
                f"[{name}] float mismatch: {actual!r} != {expected!r}"
            )

    @staticmethod
    def _compare_json(name, actual, expected):
        def _try_parse(val):
            if isinstance(val, str):
                try:
                    return json.loads(val)
                except (json.JSONDecodeError, TypeError):
                    return val
            return val

        parsed = _try_parse(actual)
        exp = _try_parse(expected) if isinstance(expected, str) else expected
        # SSv1 sometimes double-encodes JSON strings — try one more parse
        if isinstance(parsed, str) and not isinstance(exp, str):
            parsed = _try_parse(parsed)
        assert parsed == exp, (
            f"[{name}] JSON mismatch: {parsed!r} != {exp!r} (raw: {actual!r})"
        )


# ---------------------------------------------------------------------------
# Fixtures — shared
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module", params=["v3", "v4-compat", "v4-ht"])
def ingestion_mode(request):
    return request.param


@pytest.fixture(scope="module")
def mode_salt(session_name_salt, ingestion_mode):
    suffix = {"v3": "_v3", "v4-compat": "", "v4-ht": "_ht"}[ingestion_mode]
    return f"{session_name_salt}{suffix}"


# ---------------------------------------------------------------------------
# Connector config builder
# ---------------------------------------------------------------------------


def _build_mode_config(ingestion_mode, *, dlq_topic=None):
    """Load the base template config and apply mode-specific overrides.

    When ``dlq_topic`` is provided, errors.tolerance is set to "all" and DLQ
    routing is configured.  When omitted, errors.tolerance stays "none" so
    the connector task aborts immediately on validation errors — this gives
    fast failure for tests that expect errors (e.g. unsupported types).
    """
    base = json.loads((TEMPLATE_DIR / BASE_TEMPLATE).read_text())
    config = dict(base["config"])

    # Schematization for all modes — required for JSON key → column mapping
    config["snowflake.enable.schematization"] = "true"

    if dlq_topic:
        config["errors.tolerance"] = "all"
        config["errors.deadletterqueue.topic.name"] = dlq_topic
        config["errors.deadletterqueue.topic.replication.factor"] = "1"
        config["errors.deadletterqueue.context.headers.enable"] = "true"

    match ingestion_mode:
        case "v3":
            config = v4_config_to_v3(config)
        case "v4-compat":
            config["snowflake.validation"] = "client_side"
        case "v4-ht":
            config["snowflake.validation"] = "server_side"

    return config


# ---------------------------------------------------------------------------
# Fixture — single-table batch connector (test_type_compatibility.py)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def results(driver, mode_salt, ingestion_mode):
    """Single-table batch connector for type compatibility tests.

    Creates one table with all typed columns, sends every CASES entry in a
    single batch, waits for ingested rows, queries them, reads the DLQ, and
    yields a frozen Results object for assertion.
    """
    from .test_type_compatibility import COLUMNS, CASES

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    table_name = f"dt_compat{mode_salt}"
    dlq_topic = f"dlq_dt_compat{mode_salt}"

    # Consistent timezone for timestamp tests
    driver.snowflake_conn.cursor().execute("ALTER SESSION SET TIMEZONE = 'UTC'")

    # Create single table from COLUMNS spec.
    # Use quoted table name so it matches the case-sensitive name the v4
    # connector will use (the connector quotes identifiers internally).
    quoted_table = quote_name(table_name)
    col_defs = ", ".join(f"{name} {ddl}" for name, ddl in COLUMNS.items())
    driver.snowflake_conn.cursor().execute(
        f"CREATE OR REPLACE TABLE {quoted_table} ({col_defs})"
    )
    # v4 connector requires the table property for schema evolution (not the
    # connector config).  Without this, any structural mismatch routes to DLQ.
    driver.snowflake_conn.cursor().execute(
        f"ALTER TABLE {quoted_table} SET ENABLE_SCHEMA_EVOLUTION = TRUE"
    )

    # Create topics
    driver.createTopics(table_name, partitionNum=1, replicationNum=1)
    driver.createTopics(dlq_topic, partitionNum=1, replicationNum=1)

    # Register connector via driver.createConnector (handles retries and cleanup)
    config = _build_mode_config(ingestion_mode, dlq_topic=dlq_topic)
    rest_request = driver.createConnector(
        name_salt=mode_salt,
        unsalted_name="dt_compat",
        config_template=config,
    )
    connector_name = rest_request["name"]
    driver.startConnectorWaitTime()

    # Build and send all records in one batch
    records = []
    keys = []
    for i, case in enumerate(CASES):
        record = {"ID": case.name, "TEST_CASE": case.name}
        record[case.col] = case.value
        records.append(json.dumps(record).encode())
        keys.append(json.dumps({"number": str(i)}).encode())

    driver.sendBytesData(table_name, records, keys)

    # Wait until row count stabilizes.  We cannot wait for an exact count
    # because v4-compat and v4-ht reject some "ingested" cases due to known
    # divergences (binary, boolean coercion, int-epoch, etc.).  Instead,
    # poll until the count stops changing for STABLE_SECS.
    STABLE_SECS = 15
    deadline = time.monotonic() + 120
    last_count = 0
    stable_since = None

    while time.monotonic() < deadline:
        count = driver.select_number_of_records(table_name) or 0
        if count != last_count:
            last_count = count
            stable_since = time.monotonic()
        elif stable_since and count > 0:
            if (time.monotonic() - stable_since) >= STABLE_SECS:
                logger.info(
                    "Row count stabilized at %d for %ds, proceeding",
                    count,
                    STABLE_SECS,
                )
                break
        if failed := driver.get_failed_tasks(connector_name):
            logger.warning(
                "Connector task failed: %s", failed[0].get("trace", "")[:200]
            )
            break
        time.sleep(5)

    # Query all rows
    cursor = driver.snowflake_conn.cursor()
    cursor.execute(f'SELECT * FROM {quoted_table} ORDER BY RECORD_METADATA:"offset"::int')
    col_names = [desc[0] for desc in cursor.description]
    raw_rows = cursor.fetchall()

    row_lookup = {}
    for row in raw_rows:
        row_dict = dict(zip(col_names, row))
        row_id = row_dict.get("ID")
        if row_id:
            row_lookup[row_id] = row_dict

    # Read DLQ — parse message body JSON to extract case ID
    dlq_ids = set()
    consumer = KafkaConsumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"dlq-reader-{uuid4().hex[:8]}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": "false",
        }
    )
    consumer.subscribe([dlq_topic])

    deadline = time.monotonic() + 20
    empty_polls = 0
    while time.monotonic() < deadline:
        remaining = max(0.5, deadline - time.monotonic())
        msg = consumer.poll(remaining)
        if msg is None:
            empty_polls += 1
            # After partition assignment, 3 consecutive empty polls → done
            if empty_polls >= 3 and dlq_ids:
                break
            continue
        empty_polls = 0
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                logger.warning("DLQ consumer error: %s", msg.error())
            continue
        try:
            body = json.loads(msg.value())
            if "ID" in body:
                dlq_ids.add(body["ID"])
            else:
                logger.warning("DLQ message missing ID field: %s", msg.value()[:200])
        except (json.JSONDecodeError, TypeError):
            logger.warning("Could not parse DLQ message body: %s", msg.value()[:200])
    consumer.close()

    logger.info(
        "Results for mode=%s: %d rows, %d DLQ, %d sent",
        ingestion_mode,
        len(row_lookup),
        len(dlq_ids),
        len(CASES),
    )

    result = Results(
        rows=row_lookup,
        dlq_ids=frozenset(dlq_ids),
        mode=ingestion_mode,
        total_sent=len(CASES),
        columns=COLUMNS,
    )

    try:
        yield result
    finally:
        driver.closeConnector(connector_name)
        try:
            driver.deleteTopic(table_name)
        except Exception:
            pass
        try:
            driver.deleteTopic(dlq_topic)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Fixtures — per-test connector (test_unsupported_types.py)
#
# These stay separate because unsupported types (GEOGRAPHY, GEOMETRY, VECTOR,
# structured OBJECT/ARRAY) crash streaming channels and cannot share a batch
# with well-behaved types.
# ---------------------------------------------------------------------------


@pytest.fixture
def typed_table(driver, mode_salt):
    """Factory: create a Snowflake table + Kafka topic for a single test."""
    created = []

    def _create(test_id, col_ddl):
        topic = f"{test_id}{mode_salt}"
        quoted = quote_name(topic)
        driver.snowflake_conn.cursor().execute(
            f"CREATE OR REPLACE TABLE {quoted} "
            f"(VALUE_COL {col_ddl}, RECORD_METADATA VARIANT)"
        )
        driver.snowflake_conn.cursor().execute(
            f"ALTER TABLE {quoted} SET ENABLE_SCHEMA_EVOLUTION = TRUE"
        )
        driver.createTopics(topic, partitionNum=1, replicationNum=1)
        created.append(topic)
        return topic

    try:
        yield _create
    finally:
        for t in created:
            try:
                driver.deleteTopic(t)
            except Exception:
                pass


@pytest.fixture
def ingest_one_type_abort(driver, mode_salt, ingestion_mode, typed_table):
    """Per-test connector (abort mode) for a single column type.

    Creates a table with one typed column, registers a connector with
    errors.tolerance=none, sends values, and returns an IngestResult.
    The connector task fails immediately on validation errors — no DLQ.
    """

    created_connectors = []

    def _run(test_id, col_ddl, values, *, timeout=60):
        topic = typed_table(test_id, col_ddl)

        # Abort mode (errors.tolerance=none) — connector task fails immediately
        # on validation errors, giving fast feedback for unsupported types.
        config = _build_mode_config(ingestion_mode)
        rest_request = driver.createConnector(
            name_salt=mode_salt,
            unsalted_name=test_id,
            config_template=config,
        )
        connector_name = rest_request["name"]
        created_connectors.append(connector_name)
        driver.startConnectorWaitTime()

        records = [json.dumps({"VALUE_COL": v}).encode() for v in values]
        keys = [json.dumps({"number": str(i)}).encode() for i in range(len(values))]
        driver.sendBytesData(topic, records, keys)

        deadline = time.monotonic() + timeout
        error = None
        while time.monotonic() < deadline:
            if failed := driver.get_failed_tasks(connector_name):
                error = failed[0].get("trace", "no trace")
                logger.info("Connector error for %s: %.500s", test_id, error)
                break
            tbl = driver.select_number_of_records(topic) or 0
            if tbl >= len(values):
                break
            time.sleep(2)

        rows = (
            driver.snowflake_conn.cursor()
            .execute(
                f'SELECT VALUE_COL FROM {quote_name(topic)} ORDER BY RECORD_METADATA:"offset"::int'
            )
            .fetchall()
        )

        return IngestResult(
            values=[r[0] for r in rows],
            connector_error=error,
        )

    try:
        yield _run
    finally:
        for name in reversed(created_connectors):
            driver.closeConnector(name)


@dataclass
class IngestResult:
    """Legacy result type for standalone_ingest (test_unsupported_types.py)."""

    values: list
    dlq_count: int = 0
    dlq_errors: list = field(default_factory=list)
    connector_error: str | None = None
