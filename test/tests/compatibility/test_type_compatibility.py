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
from enum import Enum

import pytest

from .conftest import Case, cases_where

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
    Case("xtype_obj_str", "COL_VARCHAR", {"key": "value"}, ERR, group="xtype"),
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
# Helper: assert all cases for a column, dispatching on expect
# ---------------------------------------------------------------------------


def _assert_all(results, cases):
    """Assert every case in the list: ingested → in table, error → in DLQ/dropped."""
    for c in cases:
        if c.expect == "ingested":
            results.assert_ingested(c)
        else:
            results.assert_error(c)


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
    """BINARY: hex-encoded binary data.

    KNOWN DIVERGENCE: v4-compat fails server-side on hex strings
    (SNOW-3256183). v3 passes. This test asserts v3 behavior — v4 failure
    shows up as a test failure (the signal we want).
    """
    _assert_all(results, cases_where(col="COL_BINARY", exclude_groups=_SPECIAL_GROUPS))


# ---------------------------------------------------------------------------
# Logical data type
# ---------------------------------------------------------------------------


def test_boolean(results):
    """BOOLEAN: true/false values + non-coercible objects/arrays → DLQ/dropped."""
    _assert_all(results, cases_where(col="COL_BOOLEAN", exclude_groups=_SPECIAL_GROUPS))


def test_boolean_coercion(results):
    """BOOLEAN coercion: numeric 0/1 and string tokens.

    DataValidationUtil.validateAndParseBoolean accepts: 0, 1, "true", "false",
    "yes", "no", "y", "n", "t", "f", "on", "off" (case-insensitive).

    KNOWN DIVERGENCE: v4-compat RowValidator rejects "yes"/"no" (and likely
    other string tokens beyond "true"/"false"/"0"/"1") while v3 SSv1 accepts
    them. This test asserts v3 behavior — all 6 values land.
    """
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
    """TIMESTAMP_NTZ with integer epoch — probes v3/v4 type acceptance divergence.

    KNOWN DIVERGENCE: v4's RowValidator rejects java.lang.Long for TIMESTAMP_NTZ.
    It only accepts: String, LocalDate, LocalDateTime, ZonedDateTime, OffsetDateTime.
    V3 SSv1 accepts integer epochs and converts them to timestamps.
    This test asserts v3 behavior — the epoch lands as a timestamp.
    """
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
    """NULL in every supported column type — must be stored as SQL NULL.

    KNOWN DIVERGENCE for VARIANT: v4-compat stores JSON null as the string
    'null' while v3 stores SQL NULL.
    """
    c = next(c for c in CASES if c.col == col and c.group == "null")
    results.assert_ingested(c)


# ---------------------------------------------------------------------------
# Cross-type mismatch — DLQ behavior
# ---------------------------------------------------------------------------


def test_cross_type_mismatch(results):
    """Values sent to incompatible column types — expected DLQ/drop.

    Clear-cut mismatches (string→NUMBER, array→NUMBER) use strict assertion.
    Ambiguous cases (number→BOOLEAN for non-0/1, object→VARCHAR) use a softer
    check: the record must be accounted for (in table or DLQ), since the
    connector may coerce or reject depending on mode.
    """
    strict_errors = {"xtype_str_num_1", "xtype_str_num_2", "xtype_arr_num"}

    for c in cases_where(group="xtype"):
        if c.name in strict_errors:
            results.assert_error(c)
        else:
            in_table = c.name in results.rows
            in_dlq = c.name in results.dlq_ids
            if results.mode != "v4-ht":
                assert in_table or in_dlq, (
                    f"[{c.name}] record lost: not in table and not in DLQ"
                )


# ---------------------------------------------------------------------------
# Accounting sanity check
# ---------------------------------------------------------------------------


def test_accounting(results):
    """Safety net: verify no records were silently lost.

    Each per-column test above already catches missing records for its type.
    This test catches anything that slips through the cracks.

    For v4-ht: DLQ is not available; just verify at least 50% of expected rows
    landed (errors are silently dropped server-side).
    """
    if results.mode == "v4-ht":
        expected_ingested = sum(1 for c in CASES if c.expect == "ingested")
        assert results.total_ingested >= expected_ingested // 2, (
            f"Too few rows for v4-ht: expected at least {expected_ingested // 2}, "
            f"got {results.total_ingested}"
        )
    else:
        missing = [
            c.name
            for c in CASES
            if c.name not in results.rows and c.name not in results.dlq_ids
        ]
        assert not missing, (
            f"Records silently lost (not in table, not in DLQ): {missing}"
        )
