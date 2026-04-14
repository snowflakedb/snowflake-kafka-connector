"""Avro type compatibility tests across v4-compat and v4-ht ingestion modes.

Verifies that Avro-typed values (int, long, float, double, string, boolean,
bytes, date logical, timestamp-millis logical, array, map) are correctly
ingested into pre-created Snowflake typed columns via the AvroConverter pipeline.

Also tests Avro-specific cross-type mismatches (bytes->VARCHAR, float NaN->NUMBER,
etc.) that cannot be exercised through JSON.

v3 is excluded: Schema Registry classloader conflict prevents v3 from running
Avro tests (see E2E_TEST_PLAN.md Section 3.1.2).
"""

import datetime
import json
import logging
import math
import time

import pytest
from confluent_kafka import avro

from lib.config_migration import V4_CONFIG_TEMPLATE
from lib.driver import quote_name

from .conftest import Case, Results

logger = logging.getLogger(__name__)

pytestmark = [pytest.mark.confluent_only, pytest.mark.compatibility]

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

OK = "ingested"
ERR = "error"

# Avro schema: single record with nullable unions for each typed column,
# plus XTYPE_* columns whose Avro types intentionally mismatch the Snowflake
# column types (for cross-type error testing).
VALUE_SCHEMA = avro.loads(
    json.dumps(
        {
            "type": "record",
            "name": "TypeTestRecord",
            "namespace": "com.snowflake.kafka.test",
            "fields": [
                {"name": "ID", "type": "string"},
                {"name": "TEST_CASE", "type": "string"},
                # Positive: Avro type matches Snowflake column type
                {"name": "COL_INT", "type": ["null", "int"], "default": None},
                {"name": "COL_BIGINT", "type": ["null", "long"], "default": None},
                {"name": "COL_FLOAT", "type": ["null", "float"], "default": None},
                {"name": "COL_DOUBLE", "type": ["null", "double"], "default": None},
                {"name": "COL_VARCHAR", "type": ["null", "string"], "default": None},
                {"name": "COL_BOOLEAN", "type": ["null", "boolean"], "default": None},
                {"name": "COL_BINARY", "type": ["null", "bytes"], "default": None},
                {
                    "name": "COL_DATE",
                    "type": [
                        "null",
                        {"type": "int", "logicalType": "date"},
                    ],
                    "default": None,
                },
                {
                    "name": "COL_TS_NTZ",
                    "type": [
                        "null",
                        {"type": "long", "logicalType": "timestamp-millis"},
                    ],
                    "default": None,
                },
                {
                    "name": "COL_ARRAY",
                    "type": [
                        "null",
                        {"type": "array", "items": "string"},
                    ],
                    "default": None,
                },
                {
                    "name": "COL_VARIANT",
                    "type": [
                        "null",
                        {"type": "map", "values": "string"},
                    ],
                    "default": None,
                },
                # Cross-type: Avro type intentionally mismatches Snowflake column
                {
                    "name": "XTYPE_BYTES_TO_VARCHAR",
                    "type": ["null", "bytes"],
                    "default": None,
                },
                {
                    "name": "XTYPE_BYTES_TO_NUM",
                    "type": ["null", "bytes"],
                    "default": None,
                },
                {
                    "name": "XTYPE_FLOAT_NAN_TO_NUM",
                    "type": ["null", "float"],
                    "default": None,
                },
                {
                    "name": "XTYPE_FLOAT_INF_TO_NUM",
                    "type": ["null", "float"],
                    "default": None,
                },
                {
                    "name": "XTYPE_MAP_TO_BOOL",
                    "type": [
                        "null",
                        {"type": "map", "values": "string"},
                    ],
                    "default": None,
                },
                {
                    "name": "XTYPE_ARR_TO_BOOL",
                    "type": [
                        "null",
                        {"type": "array", "items": "string"},
                    ],
                    "default": None,
                },
            ],
        }
    )
)

# Snowflake table DDL.  Positive columns match Avro types; XTYPE_* columns
# have intentionally mismatched types.
COLUMNS = {
    "ID": "VARCHAR NOT NULL",
    "TEST_CASE": "VARCHAR",
    # Positive
    "COL_INT": "NUMBER",
    "COL_BIGINT": "NUMBER",
    "COL_FLOAT": "FLOAT",
    "COL_DOUBLE": "FLOAT",
    "COL_VARCHAR": "VARCHAR",
    "COL_BOOLEAN": "BOOLEAN",
    "COL_BINARY": "BINARY",
    "COL_DATE": "DATE",
    "COL_TS_NTZ": "TIMESTAMP_NTZ",
    "COL_ARRAY": "ARRAY",
    "COL_VARIANT": "VARIANT",
    # Cross-type mismatch targets
    "XTYPE_BYTES_TO_VARCHAR": "VARCHAR",
    "XTYPE_BYTES_TO_NUM": "NUMBER",
    "XTYPE_FLOAT_NAN_TO_NUM": "NUMBER",
    "XTYPE_FLOAT_INF_TO_NUM": "NUMBER",
    "XTYPE_MAP_TO_BOOL": "BOOLEAN",
    "XTYPE_ARR_TO_BOOL": "BOOLEAN",
    "RECORD_METADATA": "VARIANT",
}

# Days from epoch for known dates
_DATE_2024_01_15 = (datetime.date(2024, 1, 15) - datetime.date(1970, 1, 1)).days
_DATE_EPOCH = 0

# Millis from epoch for known timestamps (UTC)
_TS_2024_01_15_10_00 = int(
    datetime.datetime(2024, 1, 15, 10, 0, 0, tzinfo=datetime.timezone.utc).timestamp()
    * 1000
)
_TS_EPOCH = 0


# ---------------------------------------------------------------------------
# Test cases
# ---------------------------------------------------------------------------

CASES = [
    # ---- NUMBER (Avro int, 32-bit) ----
    Case("int_pos", "COL_INT", 42, OK, expected_value=42),
    Case("int_neg", "COL_INT", -100, OK, expected_value=-100),
    Case("int_zero", "COL_INT", 0, OK, expected_value=0),
    Case("int_max", "COL_INT", 2147483647, OK, expected_value=2147483647),
    # ---- NUMBER (Avro long, 64-bit) ----
    Case("long_pos", "COL_BIGINT", 9999999999, OK, expected_value=9999999999),
    Case("long_neg", "COL_BIGINT", -9999999999, OK, expected_value=-9999999999),
    Case("long_zero", "COL_BIGINT", 0, OK, expected_value=0),
    # ---- FLOAT (Avro float, 32-bit) ----
    Case("float_pos", "COL_FLOAT", 3.14, OK, approx=0.01),
    Case("float_neg", "COL_FLOAT", -2.72, OK, approx=0.01),
    Case("float_nan", "COL_FLOAT", float("nan"), OK, group="float_special"),
    Case("float_inf", "COL_FLOAT", float("inf"), OK, group="float_special"),
    Case("float_neginf", "COL_FLOAT", float("-inf"), OK, group="float_special"),
    # ---- FLOAT (Avro double, 64-bit) ----
    Case("dbl_pos", "COL_DOUBLE", 3.14159265358979, OK, approx=1e-6),
    Case("dbl_neg", "COL_DOUBLE", -2.71828182845905, OK, approx=1e-6),
    Case("dbl_nan", "COL_DOUBLE", float("nan"), OK, group="float_special"),
    Case("dbl_inf", "COL_DOUBLE", float("inf"), OK, group="float_special"),
    Case("dbl_neginf", "COL_DOUBLE", float("-inf"), OK, group="float_special"),
    # ---- VARCHAR (Avro string) ----
    Case("str_normal", "COL_VARCHAR", "hello world", OK),
    Case("str_empty", "COL_VARCHAR", "", OK),
    Case("str_unicode", "COL_VARCHAR", "\u3053\u3093\u306b\u3061\u306f", OK),
    # ---- BOOLEAN (Avro boolean) ----
    Case("bool_true", "COL_BOOLEAN", True, OK),
    Case("bool_false", "COL_BOOLEAN", False, OK),
    # ---- BINARY (Avro bytes) ----
    Case("bin_normal", "COL_BINARY", b"\x01\x02\x03\x04", OK),
    Case("bin_empty", "COL_BINARY", b"", OK),
    # ---- DATE (Avro date logical type: days from epoch) ----
    Case(
        "date_normal",
        "COL_DATE",
        _DATE_2024_01_15,
        OK,
        expected_value=datetime.date(2024, 1, 15),
    ),
    Case(
        "date_epoch",
        "COL_DATE",
        _DATE_EPOCH,
        OK,
        expected_value=datetime.date(1970, 1, 1),
    ),
    # ---- TIMESTAMP_NTZ (Avro timestamp-millis: millis from epoch UTC) ----
    Case(
        "ts_normal",
        "COL_TS_NTZ",
        _TS_2024_01_15_10_00,
        OK,
        expected_value=datetime.datetime(2024, 1, 15, 10, 0, 0),
    ),
    Case(
        "ts_epoch",
        "COL_TS_NTZ",
        _TS_EPOCH,
        OK,
        expected_value=datetime.datetime(1970, 1, 1, 0, 0, 0),
    ),
    # ---- ARRAY (Avro array of strings) ----
    Case("arr_normal", "COL_ARRAY", ["hello", "world"], OK),
    Case("arr_empty", "COL_ARRAY", [], OK),
    # ---- VARIANT (Avro map string->string) ----
    Case("map_normal", "COL_VARIANT", {"key1": "value1", "key2": "value2"}, OK),
    Case("map_empty", "COL_VARIANT", {}, OK),
    # ---- NULL values ----
    Case("null_int", "COL_INT", None, OK, group="null"),
    Case("null_varchar", "COL_VARCHAR", None, OK, group="null"),
    Case("null_boolean", "COL_BOOLEAN", None, OK, group="null"),
    Case("null_binary", "COL_BINARY", None, OK, group="null"),
    Case("null_date", "COL_DATE", None, OK, group="null"),
    # ---- Cross-type mismatch (Avro-specific, not covered by JSON tests) ----
    # bytes→VARCHAR: v4-compat rejects (RowValidator TEXT validation rejects byte[]),
    # v4-ht coerces to base64 (SDK accepts byte[] for VARCHAR).
    Case(
        "xtype_bytes_varchar",
        "XTYPE_BYTES_TO_VARCHAR",
        b"\x01\x02",
        ERR,
        group="xtype_bytes_varchar",
    ),
    Case("xtype_bytes_num", "XTYPE_BYTES_TO_NUM", b"\x01\x02", ERR, group="xtype"),
    Case("xtype_nan_num", "XTYPE_FLOAT_NAN_TO_NUM", float("nan"), ERR, group="xtype"),
    Case("xtype_inf_num", "XTYPE_FLOAT_INF_TO_NUM", float("inf"), ERR, group="xtype"),
    Case("xtype_map_bool", "XTYPE_MAP_TO_BOOL", {"k": "v"}, ERR, group="xtype"),
    Case("xtype_arr_bool", "XTYPE_ARR_TO_BOOL", ["a"], ERR, group="xtype"),
]

# Groups with dedicated test functions (excluded from per-column tests).
_SPECIAL_GROUPS = {
    "float_special",
    "null",
    "xtype",
    "xtype_bytes_varchar",
}


def _cases_where(*, col=None, expect=None, group=None, exclude_groups=None):
    """Filter CASES by column, outcome, and/or group."""
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
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module", params=["v4-compat", "v4-ht"])
def avro_mode(request):
    return request.param


@pytest.fixture(scope="module")
def avro_mode_salt(session_name_salt, avro_mode):
    suffix = {"v4-compat": "_avro", "v4-ht": "_avro_ht"}[avro_mode]
    return f"{session_name_salt}{suffix}"


@pytest.fixture(scope="module")
def avro_results(driver, avro_mode_salt, avro_mode):
    """Single-table batch connector for Avro type compatibility tests.

    Creates one table with all typed columns, sends every CASES entry in a
    single Avro batch, waits for ingested rows, queries them, and yields a
    frozen Results object for assertion.
    """
    table_name = f"dt_avro{avro_mode_salt}"
    sf_table = table_name
    quoted_table = quote_name(sf_table)

    # Consistent timezone for timestamp tests
    driver.snowflake_conn.cursor().execute("ALTER SESSION SET TIMEZONE = 'UTC'")

    # Create table from COLUMNS spec
    col_defs = ", ".join(f"{name} {ddl}" for name, ddl in COLUMNS.items())
    error_logging = " ERROR_LOGGING = TRUE" if avro_mode == "v4-ht" else ""
    driver.snowflake_conn.cursor().execute(
        f"CREATE OR REPLACE TABLE {quoted_table} ({col_defs}){error_logging}"
    )
    driver.snowflake_conn.cursor().execute(
        f"ALTER TABLE {quoted_table} SET ENABLE_SCHEMA_EVOLUTION = TRUE"
    )

    # Create topic
    driver.createTopics(table_name, partitionNum=1, replicationNum=1)

    # Build connector config inline
    config = {
        **V4_CONFIG_TEMPLATE,
        "topics": "SNOWFLAKE_TEST_TOPIC",
        "tasks.max": "1",
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "io.confluent.connect.avro.AvroConverter",
        "value.converter.schema.registry.url": "CONFLUENT_SCHEMA_REGISTRY",
        "snowflake.enable.schematization": "true",
        "errors.tolerance": "all",
        "errors.log.enable": "true",
    }
    match avro_mode:
        case "v4-compat":
            config["snowflake.validation"] = "client_side"
        case "v4-ht":
            config["snowflake.validation"] = "server_side"

    rest_request = driver.createConnector(
        name_salt=avro_mode_salt,
        unsalted_name="dt_avro",
        config_template=config,
    )
    connector_name = rest_request["name"]
    driver.startConnectorWaitTime()

    # Build and send all records as Avro
    records = []
    for case in CASES:
        record = {
            "ID": case.name,
            "TEST_CASE": case.name,
            # All nullable fields default to None
            "COL_INT": None,
            "COL_BIGINT": None,
            "COL_FLOAT": None,
            "COL_DOUBLE": None,
            "COL_VARCHAR": None,
            "COL_BOOLEAN": None,
            "COL_BINARY": None,
            "COL_DATE": None,
            "COL_TS_NTZ": None,
            "COL_ARRAY": None,
            "COL_VARIANT": None,
            "XTYPE_BYTES_TO_VARCHAR": None,
            "XTYPE_BYTES_TO_NUM": None,
            "XTYPE_FLOAT_NAN_TO_NUM": None,
            "XTYPE_FLOAT_INF_TO_NUM": None,
            "XTYPE_MAP_TO_BOOL": None,
            "XTYPE_ARR_TO_BOOL": None,
        }
        record[case.col] = case.value
        records.append(record)

    driver.sendAvroSRData(table_name, records, VALUE_SCHEMA)

    # Wait until row count stabilizes (same approach as JSON test).
    # Cannot predict exact count: error cases won't land in table.
    STABLE_SECS = 15
    deadline = time.monotonic() + 120
    last_count = 0
    stable_since = None

    while time.monotonic() < deadline:
        count = driver.select_number_of_records(sf_table) or 0
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
    else:
        if last_count == 0:
            logger.warning(
                "Stabilization timed out with 0 rows -- connector may not be ingesting"
            )

    # Query all rows
    cursor = driver.snowflake_conn.cursor()
    cursor.execute(
        f'SELECT * FROM {quoted_table} ORDER BY RECORD_METADATA:"offset"::int'
    )
    col_names = [desc[0] for desc in cursor.description]
    raw_rows = cursor.fetchall()

    row_lookup = {}
    for row in raw_rows:
        row_dict = dict(zip(col_names, row))
        row_id = row_dict.get("ID")
        if row_id:
            row_lookup[row_id] = row_dict

    # Query error table for v4-ht mode
    error_table_rows = []
    if avro_mode == "v4-ht":
        try:
            et_cursor = driver.snowflake_conn.cursor()
            et_cursor.execute(f"SELECT * FROM ERROR_TABLE({quoted_table})")
            et_col_names = [desc[0] for desc in et_cursor.description]
            for row in et_cursor.fetchall():
                error_table_rows.append(dict(zip(et_col_names, row)))
            et_cursor.close()
        except Exception as e:
            logger.warning("Could not query error table: %s", e)

    logger.info(
        "Avro results for mode=%s: %d rows, %d error_table, %d sent",
        avro_mode,
        len(row_lookup),
        len(error_table_rows),
        len(CASES),
    )

    result = Results(
        rows=row_lookup,
        dlq_ids=frozenset(),  # DLQ messages are Avro-encoded, can't parse case IDs
        mode=avro_mode,
        total_sent=len(CASES),
        columns=COLUMNS,
        error_table_rows=tuple(error_table_rows),
    )

    try:
        yield result
    finally:
        driver.closeConnector(connector_name)
        try:
            driver.deleteTopic(table_name)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _assert_all(results, cases):
    """Assert all cases in a list, dispatching to assert_ingested or assert_error."""
    for case in cases:
        if case.expect == OK:
            results.assert_ingested(case)
        else:
            _assert_error_no_dlq(results, case)


def _assert_error_no_dlq(results, case):
    """Assert case did NOT land in table. No DLQ check (Avro-encoded DLQ)."""
    assert case.name not in results.rows, (
        f"[{case.name}] expected NOT in table but found: "
        f"{results.rows[case.name].get(case.col)!r} (mode={results.mode})"
    )


# ---------------------------------------------------------------------------
# Tests — positive
# ---------------------------------------------------------------------------


def test_int(avro_results):
    """NUMBER from Avro int: 32-bit signed integers."""
    _assert_all(
        avro_results, _cases_where(col="COL_INT", exclude_groups=_SPECIAL_GROUPS)
    )


def test_long(avro_results):
    """NUMBER from Avro long: 64-bit signed integers."""
    _assert_all(
        avro_results, _cases_where(col="COL_BIGINT", exclude_groups=_SPECIAL_GROUPS)
    )


def test_float(avro_results):
    """FLOAT from Avro float: 32-bit values (excluding NaN/Inf specials)."""
    _assert_all(
        avro_results, _cases_where(col="COL_FLOAT", exclude_groups=_SPECIAL_GROUPS)
    )


def test_double(avro_results):
    """FLOAT from Avro double: 64-bit values (excluding NaN/Inf specials)."""
    _assert_all(
        avro_results, _cases_where(col="COL_DOUBLE", exclude_groups=_SPECIAL_GROUPS)
    )


def test_float_special(avro_results):
    """FLOAT NaN/Inf from native Avro float and double values.

    Unlike JSON where NaN/Inf are string representations, Avro sends native
    IEEE 754 NaN/Inf float values through the pipeline.

    Custom comparison: Results._compare_float handles string NaN ("NaN") but
    not float NaN (which is what Avro produces). We check presence + value
    directly instead of using assert_ingested.
    """
    for case in _cases_where(group="float_special"):
        assert case.name in avro_results.rows, (
            f"[{case.name}] expected in table but not found (mode={avro_results.mode})"
        )
        actual = avro_results.rows[case.name].get(case.col)
        sent = case.value
        if math.isnan(sent):
            assert actual is not None and math.isnan(float(actual)), (
                f"[{case.name}] expected NaN, got {actual!r}"
            )
        elif math.isinf(sent):
            actual_f = float(actual)
            assert math.isinf(actual_f) and (actual_f > 0) == (sent > 0), (
                f"[{case.name}] expected {'Inf' if sent > 0 else '-Inf'}, got {actual!r}"
            )


def test_string(avro_results):
    """VARCHAR from Avro string."""
    _assert_all(
        avro_results, _cases_where(col="COL_VARCHAR", exclude_groups=_SPECIAL_GROUPS)
    )


def test_boolean(avro_results):
    """BOOLEAN from Avro boolean: native true/false."""
    _assert_all(
        avro_results, _cases_where(col="COL_BOOLEAN", exclude_groups=_SPECIAL_GROUPS)
    )


def test_binary(avro_results):
    """BINARY from Avro bytes: raw byte arrays (not hex strings like JSON)."""
    _assert_all(
        avro_results, _cases_where(col="COL_BINARY", exclude_groups=_SPECIAL_GROUPS)
    )


def test_date(avro_results):
    """DATE from Avro date logical type (days from epoch)."""
    _assert_all(
        avro_results, _cases_where(col="COL_DATE", exclude_groups=_SPECIAL_GROUPS)
    )


def test_timestamp_ntz(avro_results):
    """TIMESTAMP_NTZ from Avro timestamp-millis logical type."""
    _assert_all(
        avro_results, _cases_where(col="COL_TS_NTZ", exclude_groups=_SPECIAL_GROUPS)
    )


def test_array(avro_results):
    """ARRAY from Avro array of strings."""
    _assert_all(
        avro_results, _cases_where(col="COL_ARRAY", exclude_groups=_SPECIAL_GROUPS)
    )


def test_variant(avro_results):
    """VARIANT from Avro map (string->string)."""
    _assert_all(
        avro_results, _cases_where(col="COL_VARIANT", exclude_groups=_SPECIAL_GROUPS)
    )


# ---------------------------------------------------------------------------
# Tests — null
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "col",
    ["COL_INT", "COL_VARCHAR", "COL_BOOLEAN", "COL_BINARY", "COL_DATE"],
)
def test_null(avro_results, col):
    """NULL values via Avro nullable unions."""
    case = next(c for c in CASES if c.col == col and c.group == "null")
    avro_results.assert_ingested(case)
    actual = avro_results.rows[case.name].get(col)
    assert actual is None, f"[{case.name}] expected NULL, got {actual!r}"


# ---------------------------------------------------------------------------
# Tests — cross-type mismatch (Avro-specific)
# ---------------------------------------------------------------------------


def test_cross_type_bytes_to_varchar(avro_results):
    """Avro bytes → VARCHAR: v4-compat rejects, v4-ht coerces to base64.

    RowValidator's TEXT validation rejects byte[], so v4-compat errors.
    The SSv2 SDK (v4-ht) accepts byte[] for VARCHAR and coerces to base64.
    """
    case = next(c for c in CASES if c.name == "xtype_bytes_varchar")
    if avro_results.mode == "v4-compat":
        _assert_error_no_dlq(avro_results, case)
    else:
        assert case.name in avro_results.rows, (
            f"[{case.name}] expected in table (v4-ht coercion) but not found"
        )
        actual = avro_results.rows[case.name].get(case.col)
        assert actual == "AQI=", f"[{case.name}] expected base64 'AQI=', got {actual!r}"


def test_cross_type_mismatch(avro_results):
    """Avro-specific cross-type errors not covered by JSON tests.

    These cases send Avro-typed values (bytes, native float NaN/Inf, map, array)
    to incompatible Snowflake column types. JSON tests can't produce these Java
    types (byte[], native float NaN, typed Avro map/array).
    """
    for case in _cases_where(group="xtype"):
        _assert_error_no_dlq(avro_results, case)
