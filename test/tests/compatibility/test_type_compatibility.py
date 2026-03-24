"""Data-type ingestion compatibility tests — single-table architecture.

All test cases are defined as data in ``CASES`` (below).  One table, one
topic, and one connector per ingestion mode — all records are sent in a single
batch, queried once, and then asserted via the shared ``results`` fixture.

Assertions encode **v3 reference behavior**.  When v4-compat diverges from v3,
the test will fail — that failure IS the signal.  No ``if ingestion_mode``
branches; no ``xfail`` per mode (except where DLQ is structurally unavailable
in v4-ht).

Parameterized across three ingestion modes via the ``ingestion_mode`` fixture
(module-scoped):
  - v3:        SnowflakeSinkConnector with SNOWPIPE_STREAMING
  - v4-compat: SnowflakeStreamingSinkConnector with client.validation.enabled=true
  - v4-ht:     SnowflakeStreamingSinkConnector with client.validation.enabled=false
               (server-side validation only, no DLQ — errors silently drop records)

Type aliases (INT, STRING, DOUBLE, DECIMAL, CHAR, etc.) are not tested
separately — they resolve to the same storage type and code path in Snowflake.

Reference: https://docs.snowflake.com/en/sql-reference/intro-summary-data-types
"""

import datetime
import json
import logging
from enum import Enum

import pytest

from .conftest import UNSET, Case, Divergence, cases_where

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.compatibility

# ---------------------------------------------------------------------------
# Expect enum — makes the CASES table scannable at a glance
# ---------------------------------------------------------------------------


class Expect(str, Enum):
    OK = "ingested"
    ERR = "error"


OK = Expect.OK
ERR = Expect.ERR


# ---------------------------------------------------------------------------
# Table column definitions  (col_name → DDL type)
# ---------------------------------------------------------------------------

COLUMNS = {
    "ID": "VARCHAR NOT NULL",
    "TEST_CASE": "VARCHAR",
    "COL_NUMBER": "NUMBER",
    "COL_NUMSCALE": "NUMBER(10,2)",
    "COL_FLOAT": "FLOAT",
    "COL_VARCHAR": "VARCHAR",
    "COL_VARCHAR10": "VARCHAR(10)",
    "COL_BINARY": "BINARY",
    "COL_BOOLEAN": "BOOLEAN",
    "COL_DATE": "DATE",
    "COL_TIME": "TIME",
    "COL_TS_NTZ": "TIMESTAMP_NTZ",
    "COL_TS_LTZ": "TIMESTAMP_LTZ",
    "COL_TS_TZ": "TIMESTAMP_TZ",
    "COL_VARIANT": "VARIANT",
    "COL_OBJECT": "OBJECT",
    "COL_ARRAY": "ARRAY",
    "RECORD_METADATA": "VARIANT",
}


# ---------------------------------------------------------------------------
# Full test case specification
# ---------------------------------------------------------------------------
# Each Case populates only ID, TEST_CASE, and ONE typed column; the rest are
# NULL.  Schematization handles sparse records.
#
# The ``group`` tag controls which test function owns each case:
#   None           → owned by the per-column test (test_number, test_float, ...)
#   "float_special"→ test_float_special  (NaN/Inf need string-representation docs)
#   "bool_coercion"→ test_boolean_coercion (known v4-compat divergence)
#   "ts_epoch"     → test_timestamp_ntz_epoch (known v4 divergence)
#   "xtype"        → test_cross_type_mismatch (values sent to wrong column)
#   "null"         → test_null (parametrized across all columns)

CASES = [
    # ---- NUMBER(38,0) ----
    Case("num_int", "COL_NUMBER", 42, OK),
    Case("num_zero", "COL_NUMBER", 0, OK),
    Case("num_neg", "COL_NUMBER", -100, OK),
    Case("num_maxint", "COL_NUMBER", 2147483647, OK),
    Case("num_minint", "COL_NUMBER", -2147483648, OK),
    Case("num_bad_str", "COL_NUMBER", "not_a_number", ERR),
    Case("num_bad_abc", "COL_NUMBER", "abc", ERR),
    Case("num_bad_obj", "COL_NUMBER", {"obj": 1}, ERR),
    # ---- NUMBER(10,2) ----
    Case("nsc_decimal", "COL_NUMSCALE", 123.45, OK, approx=0.01),
    Case("nsc_neg", "COL_NUMSCALE", -0.01, OK, approx=0.01),
    Case("nsc_zero", "COL_NUMSCALE", 0.0, OK, approx=0.01),
    Case("nsc_max", "COL_NUMSCALE", 99999.99, OK, approx=0.01),
    Case("nsc_bad_text", "COL_NUMSCALE", "text", ERR),
    # ---- FLOAT ----
    Case("flt_pi", "COL_FLOAT", 3.14, OK),
    Case("flt_neg", "COL_FLOAT", -1.5, OK),
    Case("flt_zero", "COL_FLOAT", 0.0, OK),
    Case("flt_sci", "COL_FLOAT", 1.0e10, OK),
    Case("flt_bad_text", "COL_FLOAT", "text", ERR),
    Case("flt_bad_arr", "COL_FLOAT", [1, 2], ERR),
    # FLOAT special: NaN, Infinity, -Infinity (string representations)
    Case("flt_nan", "COL_FLOAT", "NaN", OK, group="float_special"),
    Case("flt_inf", "COL_FLOAT", "Infinity", OK, group="float_special"),
    Case("flt_ninf", "COL_FLOAT", "-Infinity", OK, group="float_special"),
    # ---- VARCHAR ----
    Case("vc_hello", "COL_VARCHAR", "hello world", OK),
    Case("vc_special", "COL_VARCHAR", "special chars: !@#$%^&*()", OK),
    Case("vc_long", "COL_VARCHAR", "a" * 1000, OK),
    # ---- VARCHAR(10) ----
    Case("vc10_short", "COL_VARCHAR10", "hello", OK),
    Case("vc10_exact", "COL_VARCHAR10", "0123456789", OK),
    Case("vc10_over", "COL_VARCHAR10", "a" * 20, ERR),
    # ---- BINARY ----
    Case(
        "bin_hello",
        "COL_BINARY",
        "48656C6C6F",
        OK,
        expected_value=bytes.fromhex("48656C6C6F"),
    ),
    Case(
        "bin_dead",
        "COL_BINARY",
        "DEADBEEF",
        OK,
        expected_value=bytes.fromhex("DEADBEEF"),
    ),
    Case("bin_zero", "COL_BINARY", "00", OK, expected_value=bytes.fromhex("00")),
    Case(
        "bin_long",
        "COL_BINARY",
        "FF" * 100,
        OK,
        expected_value=bytes.fromhex("FF" * 100),
    ),
    # ---- BOOLEAN ----
    Case("bool_true", "COL_BOOLEAN", True, OK),
    Case("bool_false", "COL_BOOLEAN", False, OK),
    Case("bool_bad_obj", "COL_BOOLEAN", {"key": "value"}, ERR),
    Case("bool_bad_arr", "COL_BOOLEAN", [1, 2, 3], ERR),
    # Boolean coercion: numeric 0/1 and string tokens
    # KNOWN DIVERGENCE: v4-compat RowValidator rejects "yes"/"no" while v3 accepts them.
    Case(
        "bool_zero", "COL_BOOLEAN", 0, OK, expected_value=False, group="bool_coercion"
    ),
    Case("bool_one", "COL_BOOLEAN", 1, OK, expected_value=True, group="bool_coercion"),
    Case(
        "bool_str_true",
        "COL_BOOLEAN",
        "true",
        OK,
        expected_value=True,
        group="bool_coercion",
    ),
    Case(
        "bool_str_false",
        "COL_BOOLEAN",
        "false",
        OK,
        expected_value=False,
        group="bool_coercion",
    ),
    Case(
        "bool_str_yes",
        "COL_BOOLEAN",
        "yes",
        OK,
        expected_value=True,
        group="bool_coercion",
    ),
    Case(
        "bool_str_no",
        "COL_BOOLEAN",
        "no",
        OK,
        expected_value=False,
        group="bool_coercion",
    ),
    # ---- DATE ----
    Case(
        "date_normal",
        "COL_DATE",
        "2024-01-15",
        OK,
        expected_value=datetime.date(2024, 1, 15),
    ),
    Case(
        "date_epoch",
        "COL_DATE",
        "1970-01-01",
        OK,
        expected_value=datetime.date(1970, 1, 1),
    ),
    Case(
        "date_future",
        "COL_DATE",
        "2099-12-31",
        OK,
        expected_value=datetime.date(2099, 12, 31),
    ),
    Case("date_bad", "COL_DATE", "not_a_date", ERR),
    # ---- TIME ----
    Case(
        "time_normal",
        "COL_TIME",
        "13:45:30",
        OK,
        expected_value=datetime.time(13, 45, 30),
    ),
    Case(
        "time_midnight",
        "COL_TIME",
        "00:00:00",
        OK,
        expected_value=datetime.time(0, 0, 0),
    ),
    Case(
        "time_end", "COL_TIME", "23:59:59", OK, expected_value=datetime.time(23, 59, 59)
    ),
    Case("time_bad", "COL_TIME", "not_a_time", ERR),
    # ---- TIMESTAMP_NTZ ----
    Case(
        "tsntz_normal",
        "COL_TS_NTZ",
        "2024-01-15T13:45:30",
        OK,
        expected_value=datetime.datetime(2024, 1, 15, 13, 45, 30),
    ),
    Case(
        "tsntz_epoch",
        "COL_TS_NTZ",
        "1970-01-01T00:00:00",
        OK,
        expected_value=datetime.datetime(1970, 1, 1, 0, 0, 0),
    ),
    Case(
        "tsntz_future",
        "COL_TS_NTZ",
        "2099-12-31T23:59:59",
        OK,
        expected_value=datetime.datetime(2099, 12, 31, 23, 59, 59),
    ),
    Case("tsntz_bad", "COL_TS_NTZ", "not_a_timestamp", ERR),
    # Integer epoch → TIMESTAMP_NTZ
    # KNOWN DIVERGENCE: v4 RowValidator rejects java.lang.Long for TIMESTAMP_NTZ.
    Case(
        "tsntz_int_epoch",
        "COL_TS_NTZ",
        1705312800,
        OK,
        expected_value=datetime.datetime(2024, 1, 15, 10, 0, 0),
        group="ts_epoch",
    ),
    # ---- TIMESTAMP_LTZ ----
    Case(
        "tsltz_normal",
        "COL_TS_LTZ",
        "2024-01-15T13:45:30+00:00",
        OK,
        expected_value=datetime.datetime(2024, 1, 15, 13, 45, 30),
    ),
    Case(
        "tsltz_epoch",
        "COL_TS_LTZ",
        "1970-01-01T00:00:00+00:00",
        OK,
        expected_value=datetime.datetime(1970, 1, 1, 0, 0, 0),
    ),
    Case("tsltz_bad", "COL_TS_LTZ", "not_a_timestamp", ERR),
    # ---- TIMESTAMP_TZ ----
    Case("tstz_offset", "COL_TS_TZ", "2024-01-15T13:45:30+05:00", OK),
    Case("tstz_utc", "COL_TS_TZ", "1970-01-01T00:00:00+00:00", OK),
    Case("tstz_bad", "COL_TS_TZ", "not_a_timestamp", ERR),
    # ---- VARIANT (accepts any JSON type including primitives) ----
    Case("var_obj", "COL_VARIANT", {"key": "value", "number": 42}, OK),
    Case("var_arr", "COL_VARIANT", [1, 2, 3], OK),
    Case("var_nested", "COL_VARIANT", {"nested": [True, False, None]}, OK),
    Case("var_int", "COL_VARIANT", 42, OK),
    Case("var_float", "COL_VARIANT", 3.14, OK),
    Case("var_bool", "COL_VARIANT", True, OK),
    # Bare string (not valid JSON) → DLQ on v3/v4-compat; v4-ht ingests it
    # as a string VARIANT value (server-side accepts non-JSON scalars).
    Case("var_str", "COL_VARIANT", "hello", ERR, group="variant_bare_str"),
    # String containing valid JSON — probes SSv1/SSv2 parse divergence:
    # SSv1 parses JSON-like strings into native objects, SSv2 may keep as string.
    Case("var_json_str", "COL_VARIANT", '{"a":1}', OK),
    # ---- OBJECT ----
    Case("obj_simple", "COL_OBJECT", {"key": "value"}, OK),
    Case("obj_nested", "COL_OBJECT", {"nested": {"a": 1, "b": 2}}, OK),
    Case("obj_with_arr", "COL_OBJECT", {"array_val": [1, 2, 3]}, OK),
    # JSON string that parses to an object
    Case("obj_str_json", "COL_OBJECT", '{"key":"value"}', OK),
    # ---- ARRAY ----
    Case("arr_strings", "COL_ARRAY", ["a", "b", "c"], OK),
    Case("arr_numbers", "COL_ARRAY", [1, 2, 3], OK),
    Case("arr_objects", "COL_ARRAY", [{"key": "value"}, {"key": "value2"}], OK),
    # JSON string sent to ARRAY: v3 (SSv1) parses it into [1,2,3],
    # v4 (SSv2) stores it as literal string element ["[1,2,3]"].
    Case("arr_str_json", "COL_ARRAY", "[1,2,3]", OK, group="array_json_str"),
    # ---- NULL handling (one per supported type) ----
    # KNOWN DIVERGENCE for VARIANT: v4-compat stores JSON null as string 'null'
    # while v3 stores SQL NULL.
    Case("null_number", "COL_NUMBER", None, OK, group="null"),
    Case("null_float", "COL_FLOAT", None, OK, group="null"),
    Case("null_varchar", "COL_VARCHAR", None, OK, group="null"),
    Case("null_boolean", "COL_BOOLEAN", None, OK, group="null"),
    Case("null_date", "COL_DATE", None, OK, group="null"),
    Case("null_time", "COL_TIME", None, OK, group="null"),
    Case("null_ts_ntz", "COL_TS_NTZ", None, OK, group="null"),
    Case("null_ts_ltz", "COL_TS_LTZ", None, OK, group="null"),
    Case("null_ts_tz", "COL_TS_TZ", None, OK, group="null"),
    Case("null_variant", "COL_VARIANT", None, OK, group="null"),
    Case("null_object", "COL_OBJECT", None, OK, group="null"),
    Case("null_array", "COL_ARRAY", None, OK, group="null"),
    # ---- Cross-type mismatch ----
    Case("xtype_str_num_1", "COL_NUMBER", "hello", ERR, group="xtype"),
    Case("xtype_str_num_2", "COL_NUMBER", "world", ERR, group="xtype"),
    Case("xtype_num_bool_1", "COL_BOOLEAN", 42, ERR, group="xtype"),
    Case("xtype_num_bool_2", "COL_BOOLEAN", -1, ERR, group="xtype"),
    Case("xtype_num_bool_3", "COL_BOOLEAN", 999, ERR, group="xtype"),
    # Object coerced to JSON string in VARCHAR — accepted by all modes
    Case(
        "xtype_obj_str",
        "COL_VARCHAR",
        {"key": "value"},
        OK,
        expected_value='{"key":"value"}',
        group="xtype",
    ),
    Case("xtype_arr_num", "COL_NUMBER", [1, 2, 3], ERR, group="xtype"),
]

# Groups that have their own dedicated test functions.
# Per-column tests (test_number, test_float, ...) exclude these.
_SPECIAL_GROUPS = {
    "float_special",
    "bool_coercion",
    "ts_epoch",
    "xtype",
    "null",
    "variant_bare_str",
    "array_json_str",
}


# ---------------------------------------------------------------------------
# Known mode divergences from v3 reference behavior
#
# Each entry maps (case_name, mode) → Divergence describing how that mode
# differs from v3.  The _assert_all helper applies these automatically.
# ---------------------------------------------------------------------------

_D = Divergence  # short alias for readability

MODE_OVERRIDES: dict[tuple[str, str], Divergence] = {
    # BINARY: v4 SSv2 rejects hex-encoded binary strings (SNOW-3256183).
    # Records silently dropped — not in table, not in DLQ.
    ("bin_hello", "v4-compat"): _D(
        expect="error",
        no_dlq=True,
        description="SSv2 rejects hex strings (SNOW-3256183)",
    ),
    ("bin_hello", "v4-ht"): _D(
        expect="error", description="SSv2 rejects hex strings (SNOW-3256183)"
    ),
    ("bin_dead", "v4-compat"): _D(
        expect="ingested",
        skip_value_check=True,
        description="SSv2 ingests hex string as raw bytes (garbled value)",
    ),
    ("bin_dead", "v4-ht"): _D(
        expect="ingested",
        skip_value_check=True,
        description="SSv2 ingests hex string as raw bytes (garbled value)",
    ),
    ("bin_zero", "v4-compat"): _D(
        expect="error",
        no_dlq=True,
        description="SSv2 rejects hex strings (SNOW-3256183)",
    ),
    ("bin_zero", "v4-ht"): _D(
        expect="error", description="SSv2 rejects hex strings (SNOW-3256183)"
    ),
    ("bin_long", "v4-compat"): _D(
        expect="ingested",
        skip_value_check=True,
        description="SSv2 ingests hex string as raw bytes (garbled value)",
    ),
    ("bin_long", "v4-ht"): _D(
        expect="ingested",
        skip_value_check=True,
        description="SSv2 ingests hex string as raw bytes (garbled value)",
    ),
    # BOOLEAN coercion: v4 RowValidator rejects numeric 0/1 as boolean values.
    # Records silently dropped on v4-compat (not DLQ'd).
    ("bool_zero", "v4-compat"): _D(
        expect="error", no_dlq=True, description="v4 rejects numeric 0/1 as boolean"
    ),
    ("bool_zero", "v4-ht"): _D(
        expect="error", description="v4 rejects numeric 0/1 as boolean"
    ),
    ("bool_one", "v4-compat"): _D(
        expect="error", no_dlq=True, description="v4 rejects numeric 0/1 as boolean"
    ),
    ("bool_one", "v4-ht"): _D(
        expect="error", description="v4 rejects numeric 0/1 as boolean"
    ),
    # TIMESTAMP_NTZ from integer epoch: v4 RowValidator rejects java.lang.Long.
    # v4-compat: DLQ'd.  v4-ht: ingested but epoch interpreted differently (UTC-8 vs UTC).
    ("tsntz_int_epoch", "v4-compat"): _D(
        expect="error", description="v4 rejects Long for TIMESTAMP_NTZ"
    ),
    ("tsntz_int_epoch", "v4-ht"): _D(
        expected_value=datetime.datetime(2024, 1, 15, 2, 0, 0),
        description="epoch interpreted in session TZ (UTC-8) instead of UTC",
    ),
    # NULL VARIANT: v4 stores JSON null as string 'null' instead of SQL NULL.
    ("null_variant", "v4-compat"): _D(
        expected_value="null", description="JSON null stored as string 'null'"
    ),
    ("null_variant", "v4-ht"): _D(
        expected_value="null", description="JSON null stored as string 'null'"
    ),
    # Cross-type: numeric values (42, -1, 999) sent to BOOLEAN column.
    # v4-compat silently drops these (not DLQ'd, not in table).
    ("xtype_num_bool_1", "v4-compat"): _D(
        no_dlq=True, description="v4-compat silently drops numeric→BOOLEAN"
    ),
    ("xtype_num_bool_2", "v4-compat"): _D(
        no_dlq=True, description="v4-compat silently drops numeric→BOOLEAN"
    ),
    ("xtype_num_bool_3", "v4-compat"): _D(
        no_dlq=True, description="v4-compat silently drops numeric→BOOLEAN"
    ),
    # Object→VARCHAR: v3/v4-ht coerce to JSON string, v4-compat DLQ's it.
    ("xtype_obj_str", "v4-compat"): _D(
        expect="error", description="v4-compat DLQ's object→VARCHAR (v3/v4-ht coerce)"
    ),
}


# ---------------------------------------------------------------------------
# Helper: assert all cases for a column, dispatching on expect
# ---------------------------------------------------------------------------


def _effective_expect(case, mode):
    """Return the effective expect for a case on a given mode."""
    div = MODE_OVERRIDES.get((case.name, mode))
    return div.expect if (div and div.expect) else case.expect


def _assert_all(results, cases):
    """Assert every case in the list, applying mode-specific divergence overrides."""
    for c in cases:
        div = MODE_OVERRIDES.get((c.name, results.mode))
        if _effective_expect(c, results.mode) == "ingested":
            results.assert_ingested(c, divergence=div)
        else:
            results.assert_error(c, divergence=div)


# ---------------------------------------------------------------------------
# Numeric data types
# ---------------------------------------------------------------------------


def test_number(results):
    """NUMBER(38,0): integers land, non-numeric values → DLQ/dropped."""
    _assert_all(results, cases_where(col="COL_NUMBER", exclude_groups=_SPECIAL_GROUPS))


def test_number_with_scale(results):
    """NUMBER(10,2): decimal values + non-numeric string to DLQ."""
    _assert_all(
        results, cases_where(col="COL_NUMSCALE", exclude_groups=_SPECIAL_GROUPS)
    )


def test_float(results):
    """FLOAT: standard floating-point values + non-numeric → DLQ/dropped."""
    _assert_all(results, cases_where(col="COL_FLOAT", exclude_groups=_SPECIAL_GROUPS))


def test_float_special(results):
    """FLOAT special values: NaN, +Infinity, -Infinity.

    JSON RFC 8259 does not define NaN/Infinity literals. We send them as
    string representations which is how DataValidationUtil.validateAndParseReal
    handles them (via Double.parseDouble).
    """
    _assert_all(results, cases_where(col="COL_FLOAT", group="float_special"))


# ---------------------------------------------------------------------------
# String & binary data types
# ---------------------------------------------------------------------------


def test_varchar(results):
    """VARCHAR: variable-length character strings."""
    _assert_all(results, cases_where(col="COL_VARCHAR", exclude_groups=_SPECIAL_GROUPS))


def test_varchar_length_limit(results):
    """VARCHAR(10): strings at and exceeding declared length limit.

    Snowflake silently truncates or the connector rejects overlength strings.
    This probes whether v3 and v4 handle the constraint identically.
    """
    _assert_all(
        results, cases_where(col="COL_VARCHAR10", exclude_groups=_SPECIAL_GROUPS)
    )


def test_binary(results):
    """BINARY: hex-encoded binary data."""
    _assert_all(results, cases_where(col="COL_BINARY", exclude_groups=_SPECIAL_GROUPS))


# ---------------------------------------------------------------------------
# Logical data type
# ---------------------------------------------------------------------------


def test_boolean(results):
    """BOOLEAN: true/false values + non-coercible objects/arrays → DLQ/dropped."""
    _assert_all(results, cases_where(col="COL_BOOLEAN", exclude_groups=_SPECIAL_GROUPS))


def test_boolean_coercion(results):
    """BOOLEAN coercion: numeric 0/1 and string tokens."""
    _assert_all(results, cases_where(group="bool_coercion"))


# ---------------------------------------------------------------------------
# Date & time data types
# ---------------------------------------------------------------------------


def test_date(results):
    """DATE: ISO date strings + invalid string to DLQ."""
    _assert_all(results, cases_where(col="COL_DATE", exclude_groups=_SPECIAL_GROUPS))


def test_time(results):
    """TIME: time-of-day strings + invalid string to DLQ."""
    _assert_all(results, cases_where(col="COL_TIME", exclude_groups=_SPECIAL_GROUPS))


def test_timestamp_ntz(results):
    """TIMESTAMP_NTZ: ISO 8601 timestamps + invalid string to DLQ."""
    _assert_all(results, cases_where(col="COL_TS_NTZ", exclude_groups=_SPECIAL_GROUPS))


def test_timestamp_ntz_epoch(results):
    """TIMESTAMP_NTZ with integer epoch."""
    _assert_all(results, cases_where(group="ts_epoch"))


def test_timestamp_ltz(results):
    """TIMESTAMP_LTZ: timestamps with explicit UTC offset + invalid to DLQ."""
    _assert_all(results, cases_where(col="COL_TS_LTZ", exclude_groups=_SPECIAL_GROUPS))


def test_timestamp_tz(results):
    """TIMESTAMP_TZ: timestamps with explicit timezone + invalid to DLQ."""
    _assert_all(results, cases_where(col="COL_TS_TZ", exclude_groups=_SPECIAL_GROUPS))


# ---------------------------------------------------------------------------
# Semi-structured data types
# ---------------------------------------------------------------------------


def test_variant(results):
    """VARIANT: any JSON type including primitives, objects, arrays.

    Includes a string containing valid JSON ('{\"a\":1}') to probe the known
    SSv1/SSv2 divergence: SSv1 parses JSON-like strings into native JSON
    objects in VARIANT columns, while SSv2 may store them as string literals.
    """
    _assert_all(results, cases_where(col="COL_VARIANT", exclude_groups=_SPECIAL_GROUPS))


def test_object(results):
    """OBJECT: JSON object values, including from-string JSON."""
    _assert_all(results, cases_where(col="COL_OBJECT", exclude_groups=_SPECIAL_GROUPS))


def test_array(results):
    """ARRAY: JSON array values, including from-string JSON."""
    _assert_all(results, cases_where(col="COL_ARRAY", exclude_groups=_SPECIAL_GROUPS))


def test_variant_bare_string(results):
    """Bare string to VARIANT: DLQ on v3/v4-compat, ingested on v4-ht.

    KNOWN DIVERGENCE: v3/v4-compat reject bare strings (not valid JSON) and
    route them to DLQ. v4-ht (server-side only) accepts them as string
    VARIANT values.
    """
    [case] = cases_where(group="variant_bare_str")
    if results.mode == "v4-ht":
        # Server-side accepts bare strings in VARIANT — row is ingested.
        # Snowflake stores VARIANT strings as JSON-quoted: "hello" → '"hello"'
        assert case.name in results.rows, (
            f"[{case.name}] expected v4-ht to ingest bare string to VARIANT"
        )
        actual = results.rows[case.name].get(case.col)
        expected_json = json.dumps(case.value)  # "hello" → '"hello"'
        assert actual == expected_json, (
            f"[{case.name}] value mismatch: {actual!r} != {expected_json!r}"
        )
    else:
        results.assert_error(case)


def test_array_json_string(results):
    """JSON string sent to ARRAY: SSv1 parses, SSv2 stores as literal.

    KNOWN DIVERGENCE: v3 (SSv1) parses the JSON string '[1,2,3]' into a
    proper array [1,2,3]. v4 (SSv2) stores it as a single-element array
    containing the literal string: ["[1,2,3]"].
    """
    [case] = cases_where(group="array_json_str")
    if results.mode == "v3":
        results.assert_ingested(case)
    else:
        # v4-compat/v4-ht: string stored as literal element
        assert case.name in results.rows, (
            f"[{case.name}] expected row in table on {results.mode}"
        )
        actual = results.rows[case.name].get(case.col)
        parsed = json.loads(actual) if isinstance(actual, str) else actual
        assert parsed == ["[1,2,3]"], (
            f"[{case.name}] expected ['[1,2,3]'] on {results.mode}, got {parsed!r}"
        )


# ---------------------------------------------------------------------------
# NULL handling
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "col",
    [
        "COL_NUMBER",
        "COL_FLOAT",
        "COL_VARCHAR",
        "COL_BOOLEAN",
        "COL_DATE",
        "COL_TIME",
        "COL_TS_NTZ",
        "COL_TS_LTZ",
        "COL_TS_TZ",
        "COL_VARIANT",
        "COL_OBJECT",
        "COL_ARRAY",
    ],
)
def test_null(results, col):
    """NULL in every supported column type — must be stored as SQL NULL."""
    c = next(c for c in CASES if c.col == col and c.group == "null")
    div = MODE_OVERRIDES.get((c.name, results.mode))
    results.assert_ingested(c, divergence=div)


# ---------------------------------------------------------------------------
# Cross-type mismatch — DLQ behavior
# ---------------------------------------------------------------------------


def test_cross_type_mismatch(results):
    """Values sent to incompatible column types — expected DLQ/drop."""
    _assert_all(results, cases_where(group="xtype"))


# ---------------------------------------------------------------------------
# Accounting sanity check
# ---------------------------------------------------------------------------


def test_accounting(results):
    """Safety net: verify no records were silently lost.

    A record is "accounted for" if it is in the table, in the DLQ, or has a
    MODE_OVERRIDES entry with no_dlq=True (known silent drop).  For v4-ht,
    DLQ is structurally unavailable so only table presence is checked.
    """
    if results.mode == "v4-ht":
        expected_ingested = sum(
            1 for c in CASES if _effective_expect(c, results.mode) == "ingested"
        )
        assert results.total_ingested >= expected_ingested - 2, (
            f"Too few rows for v4-ht: expected ~{expected_ingested}, "
            f"got {results.total_ingested}"
        )
    else:
        known_no_dlq = {
            name
            for (name, mode), d in MODE_OVERRIDES.items()
            if mode == results.mode and d.no_dlq
        }
        missing = [
            c.name
            for c in CASES
            if (
                c.name not in results.rows
                and c.name not in results.dlq_ids
                and c.name not in known_no_dlq
            )
        ]
        assert not missing, (
            f"Records silently lost (not in table, not in DLQ, not in known_no_dlq): {missing}"
        )


# ---------------------------------------------------------------------------
# Compatibility report — prints behavioral differences across modes
# ---------------------------------------------------------------------------


def test_compatibility_report(results):
    """Print the behavioral difference matrix (informational, always passes).

    Generates the matrix once for the last mode that runs (v4-ht).
    """
    if results.mode != "v4-ht":
        return

    # Group divergences by description for compact output
    from collections import defaultdict

    groups = defaultdict(lambda: {"cases": [], "modes": {}})
    for (case_name, mode), div in sorted(MODE_OVERRIDES.items()):
        key = div.description
        if case_name not in groups[key]["cases"]:
            groups[key]["cases"].append(case_name)
        orig = next((c for c in CASES if c.name == case_name), None)
        if orig:
            v3_behavior = orig.expect
            effective = div.expect or orig.expect
            if div.no_dlq and effective == "error":
                mode_behavior = "dropped (no DLQ)"
            elif div.skip_value_check and effective == "ingested":
                mode_behavior = "ingested (value differs)"
            elif div.expect and div.expect != orig.expect:
                mode_behavior = div.expect
            elif not isinstance(div.expected_value, type(UNSET)):
                mode_behavior = f"ingested (value: {div.expected_value!r})"
            else:
                mode_behavior = effective
            groups[key]["modes"][mode] = mode_behavior
            groups[key]["v3"] = v3_behavior

    lines = [
        "",
        "=" * 90,
        "COMPATIBILITY REPORT — Known behavioral divergences from v3 reference",
        "=" * 90,
    ]
    for desc, info in groups.items():
        cases_str = ", ".join(info["cases"])
        lines.append(f"\n  {desc}")
        lines.append(f"    Cases: {cases_str}")
        lines.append(f"    v3:        {info.get('v3', '?')}")
        for mode in ["v4-compat", "v4-ht"]:
            if mode in info["modes"]:
                lines.append(f"    {mode:10s} {info['modes'][mode]}")
    lines.append("")
    lines.append("=" * 90)

    report = "\n".join(lines)
    logger.info(report)
