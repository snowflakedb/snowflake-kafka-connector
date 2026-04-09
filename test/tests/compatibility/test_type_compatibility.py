"""Data-type ingestion compatibility tests — single-table architecture.

All test cases are defined as data in ``CASES`` (below).  One table, one
topic, and one connector per ingestion mode — all records are sent in a single
batch, queried once, and then asserted via the shared ``results`` fixture.

Assertions encode **v3 reference behavior**.  Tests that encounter known v4
divergences handle them inline and log a ``DIVERGENCE`` warning — grep for
that prefix to find all behavioral differences across modes.

Parameterized across three ingestion modes via the ``ingestion_mode`` fixture
(module-scoped):
  - v3:        SnowflakeSinkConnector with SNOWPIPE_STREAMING
  - v4-compat: SnowflakeStreamingSinkConnector with snowflake.validation=client_side
  - v4-ht:     SnowflakeStreamingSinkConnector with snowflake.validation=server_side
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

from .conftest import UNSET, Case, cases_where

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
    Case("bool_bad_str", "COL_BOOLEAN", "random_string", ERR),
    # Boolean coercion: numeric 0/1 and string tokens.
    # v4-compat fix: RowValidator normalizes any valid input to Boolean.
    # v4-ht: RowValidator bypassed; Integer 0/1 reach SSv2 SDK directly and are dropped.
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
    Case(
        "bool_str_on",
        "COL_BOOLEAN",
        "on",
        OK,
        expected_value=True,
        group="bool_coercion",
    ),
    Case(
        "bool_str_off",
        "COL_BOOLEAN",
        "off",
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
    # String containing valid JSON — probes SSv1/SSv2 parse divergence.
    # v3/v4-compat: SSv1/RowValidator parses string into native object {"a":1}.
    # v4-ht: SSv2 SDK stores the string as a JSON-quoted literal '"{\\"a\\":1}"'.
    Case(
        "var_json_str",
        "COL_VARIANT",
        '{"a":1}',
        OK,
        expected_value={"a": 1},
        group="variant_json_str",
    ),
    # JSON scalar strings to VARIANT — exercises the String→native re-parse path
    # for primitives (number, boolean, null).  All are valid JSON.
    Case(
        "var_json_num",
        "COL_VARIANT",
        "42",
        OK,
        expected_value=42,
        group="variant_json_str",
    ),
    Case(
        "var_json_bool",
        "COL_VARIANT",
        "true",
        OK,
        expected_value=True,
        group="variant_json_str",
    ),
    Case(
        "var_json_arr",
        "COL_VARIANT",
        "[1,2]",
        OK,
        expected_value=[1, 2],
        group="variant_json_str",
    ),
    # ---- OBJECT ----
    Case("obj_simple", "COL_OBJECT", {"key": "value"}, OK),
    Case("obj_nested", "COL_OBJECT", {"nested": {"a": 1, "b": 2}}, OK),
    Case("obj_with_arr", "COL_OBJECT", {"array_val": [1, 2, 3]}, OK),
    # JSON string that parses to an object
    Case("obj_str_json", "COL_OBJECT", '{"key":"value"}', OK),
    # Invalid JSON string → OBJECT: rejected in all modes
    Case("obj_bad_str", "COL_OBJECT", "not_json", ERR),
    # Valid JSON but not an object (array) → OBJECT: rejected in all modes
    Case("obj_str_arr", "COL_OBJECT", "[1,2,3]", ERR),
    # ---- ARRAY ----
    Case("arr_strings", "COL_ARRAY", ["a", "b", "c"], OK),
    Case("arr_numbers", "COL_ARRAY", [1, 2, 3], OK),
    Case("arr_objects", "COL_ARRAY", [{"key": "value"}, {"key": "value2"}], OK),
    # Invalid JSON string → ARRAY: v3/v4-compat reject (DLQ); v4-ht wraps as ["not_json"].
    Case("arr_bad_str", "COL_ARRAY", "not_json", ERR, group="array_json_str"),
    # JSON string sent to ARRAY: v3 (SSv1) parses it into [1,2,3],
    # v4 (SSv2) stores it as literal string element ["[1,2,3]"].
    Case("arr_str_json", "COL_ARRAY", "[1,2,3]", OK, group="array_json_str"),
    # Non-array JSON string: validateAndParseArray wraps into single-element array.
    Case(
        "arr_str_scalar",
        "COL_ARRAY",
        "42",
        OK,
        expected_value=[42],
        group="array_json_str",
    ),
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
    # List coerced to JSON string in VARCHAR — same as Map (xtype_obj_str)
    Case(
        "xtype_list_str",
        "COL_VARCHAR",
        [1, 2, 3],
        OK,
        expected_value="[1,2,3]",
        group="xtype",
    ),
    # Map serialized to JSON exceeds VARCHAR(10) limit → rejected
    Case("xtype_map_vc10", "COL_VARCHAR10", {"key": "value"}, ERR, group="xtype"),
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
    "variant_json_str",
    "array_json_str",
}


# ---------------------------------------------------------------------------
# Divergence logging — grep for "DIVERGENCE" to find all behavioral diffs
# ---------------------------------------------------------------------------

_DIVERGENCE_PREFIX = "DIVERGENCE"


def _log_divergence(mode, case_name, description):
    """Log a known behavioral divergence from v3 reference.

    All divergences use the same prefix so they can be found with:
        grep DIVERGENCE <test-output>
    """
    logger.warning("%s [%s] %s: %s", _DIVERGENCE_PREFIX, mode, case_name, description)


# ---------------------------------------------------------------------------
# Helper: assert all cases for a column, dispatching on expect
# ---------------------------------------------------------------------------


def _assert_all(results, cases):
    """Assert every case in the list using v3 reference expectations."""
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

    v3 and v4-compat both correctly decode hex strings to bytes.  v4-compat
    is fixed by SNOW-3256183: client-side RowValidator converts hex → byte[]
    before handing the row to the Ingest SDK, matching SSv1 behavior.

    KNOWN DIVERGENCE for v4-ht : server-side validation passes hex strings
    directly to the SSv2 SDK, which interprets them as base64 when
    ENABLE_SSV2_DEFAULT_BINARY_FORMAT_BASE64 is set, producing garbled bytes.
    """
    cases = cases_where(col="COL_BINARY", exclude_groups=_SPECIAL_GROUPS)
    if results.mode in ("v3", "v4-compat"):
        _assert_all(results, cases)
        return

    # v4-ht: log divergence details for diagnostics, then xfail.
    for c in cases:
        if c.name in results.rows:
            actual = results.rows[c.name].get(c.col)
            if c.expected_value is not UNSET and actual != c.expected_value:
                _log_divergence(
                    results.mode,
                    c.name,
                    f"ingested with wrong value: {actual!r} (expected {c.expected_value!r})",
                )
            elif c.expected_value is UNSET:
                _log_divergence(
                    results.mode,
                    c.name,
                    f"ingested (v3 also ingests, value={actual!r})",
                )
        else:
            in_dlq = c.name in results.dlq_ids
            _log_divergence(
                results.mode,
                c.name,
                f"rejected (v3 ingests); in_dlq={in_dlq}",
            )

    try:
        _assert_all(results, cases)
    except AssertionError as e:
        pytest.xfail(f"v4-ht SSv2 binary handling diverges from v3: {e}")


# ---------------------------------------------------------------------------
# Logical data type
# ---------------------------------------------------------------------------


def test_boolean(results):
    """BOOLEAN: true/false values + non-coercible objects/arrays → DLQ/dropped."""
    _assert_all(results, cases_where(col="COL_BOOLEAN", exclude_groups=_SPECIAL_GROUPS))


def test_boolean_coercion(results):
    """BOOLEAN coercion: numeric 0/1 and string tokens.

    v3 and v4-compat both coerce Integer 0->False, 1->True.
    v4-compat fix: RowValidator now normalizes any valid input to Boolean before
    passing to the SSv2 SDK (which only accepts Boolean, not Integer/String).

    KNOWN DIVERGENCE for v4-ht: server-side validation bypasses RowValidator,
    so Integer 0/1 reach the SSv2 SDK directly and are silently dropped.
    String tokens ("true"/"false"/"yes"/"no") work on all modes.
    """
    cases = cases_where(group="bool_coercion")
    numeric_cases = {c.name for c in cases if isinstance(c.value, int)}

    # String boolean tokens work identically on all modes — always hard assert.
    for c in cases:
        if c.name not in numeric_cases:
            if c.expect == "ingested":
                results.assert_ingested(c)
            else:
                results.assert_error(c)

    if results.mode in ("v3", "v4-compat"):
        # Both v3 (SSv1 coercion) and v4-compat (RowValidator normalization) ingest 0/1 correctly.
        for c in cases:
            if c.name in numeric_cases:
                results.assert_ingested(c)
        return

    # v4-ht: RowValidator is bypassed; SSv2 SDK silently drops Integer inputs for BOOLEAN.
    for c in cases:
        if c.name in numeric_cases:
            in_dlq = c.name in results.dlq_ids
            _log_divergence(
                results.mode,
                c.name,
                f"v4-ht drops numeric {c.value} for BOOLEAN (SSv2 SDK rejects Integer); in_dlq={in_dlq}",
            )

    try:
        for c in cases:
            if c.name in numeric_cases:
                results.assert_ingested(c)
    except AssertionError as e:
        pytest.xfail(f"v4-ht drops numeric booleans (SSv2 SDK rejects Integer): {e}")


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
    """TIMESTAMP_NTZ with integer epoch.

    v3: SSv1 SDK converts epoch to UTC client-side via parseInstantGuessScale.
    v4-compat: RowValidator normalizes Integer epoch to ISO string (same as v3).
    KNOWN DIVERGENCE: v4-ht bypasses RowValidator; SSv2 SDK passes raw Integer to
    the Snowflake backend which interprets it using the channel's default timezone
    (America/Los_Angeles) instead of UTC, producing a -8h shifted timestamp.
    """
    [case] = cases_where(group="ts_epoch")
    if results.mode in ("v3", "v4-compat"):
        results.assert_ingested(case)
        return

    # v4-ht: log and xfail on expected timezone shift.
    if case.name in results.rows:
        actual = results.rows[case.name].get(case.col)
        expected = (
            case.expected_value
            if not isinstance(case.expected_value, type(UNSET))
            else case.value
        )
        if actual != expected:
            _log_divergence(
                results.mode,
                case.name,
                f"epoch timestamp shifted: got {actual!r}, v3 expects {expected!r}",
            )
    else:
        in_dlq = case.name in results.dlq_ids
        _log_divergence(
            results.mode,
            case.name,
            f"v4-ht rejects Long for TIMESTAMP_NTZ; in_dlq={in_dlq}",
        )

    try:
        results.assert_ingested(case)
    except AssertionError as e:
        pytest.xfail(f"v4-ht: SSv2 backend uses channel TZ for integer epoch: {e}")


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
        _log_divergence(
            results.mode, case.name, "bare string ingested as VARIANT (v3 DLQ's it)"
        )
    else:
        results.assert_error(case)


def test_variant_json_string(results):
    """JSON string sent to VARIANT: v3/v4-compat parse to native object, v4-ht stores as string.

    Covers JSON object strings ('{"a":1}'), scalar strings ('42', 'true'),
    and JSON array strings ('[1,2]') sent as String values to a VARIANT column.

    v3 and v4-compat: RowValidator normalizes the String to a native Java object
    (Map, List, Integer, Boolean) so the SSv2 SDK stores it correctly.

    KNOWN DIVERGENCE for v4-ht: server-side validation bypasses RowValidator; the SSv2 SDK
    receives the raw String and stores it as a JSON-quoted string literal.
    """
    cases = cases_where(group="variant_json_str")
    if results.mode in ("v3", "v4-compat"):
        for c in cases:
            results.assert_ingested(c)
        return

    # v4-ht: row is ingested but stored as a JSON-quoted string, not as a native object.
    divergences = []
    for c in cases:
        assert c.name in results.rows, (
            f"[{c.name}] expected row in table on {results.mode}"
        )
        try:
            results.assert_ingested(c)
        except AssertionError:
            actual = results.rows[c.name].get(c.col)
            _log_divergence(
                results.mode,
                c.name,
                f"JSON string stored as quoted literal {actual!r} (v3 stores as {c.expected_value!r})",
            )
            divergences.append(c.name)

    if divergences:
        pytest.xfail(
            f"v4-ht stores JSON strings as quoted literals in VARIANT: {divergences}"
        )


def test_array_json_string(results):
    """String values sent to ARRAY: v3/v4-compat parse or reject, v4-ht wraps as literal element.

    Covers:
      - JSON array strings ('[1,2,3]') — v3/v4-compat parse to proper array
      - Non-array JSON scalars ('42') — v3/v4-compat wrap as single-element array
      - Invalid JSON strings ('not_json') — v3/v4-compat reject (DLQ)

    v3 and v4-compat: RowValidator normalizes String to a List so the SSv2 SDK
    stores it as a proper array.  Non-array scalars are wrapped into a
    single-element array (e.g. '42' → [42]).  Invalid JSON is rejected.

    KNOWN DIVERGENCE for v4-ht: server-side validation bypasses RowValidator; the SSv2 SDK
    wraps ANY String as a single-element array, including invalid JSON and valid JSON alike.
    """
    cases = cases_where(group="array_json_str")
    if results.mode in ("v3", "v4-compat"):
        for c in cases:
            if c.expect == "ingested":
                results.assert_ingested(c)
            else:
                results.assert_error(c)
        return

    # v4-ht: SSv2 wraps all strings as single-element arrays (no rejection)
    divergences = []
    for c in cases:
        if c.expect == "error":
            # v3/v4-compat reject this, but v4-ht ingests it as ["<value>"]
            if c.name in results.rows:
                actual = results.rows[c.name].get(c.col)
                parsed = json.loads(actual) if isinstance(actual, str) else actual
                _log_divergence(
                    results.mode,
                    c.name,
                    f"v4-ht ingested (v3 rejects): stored as {parsed!r}",
                )
                divergences.append(c.name)
            else:
                # Also rejected on v4-ht — no divergence
                pass
        else:
            assert c.name in results.rows, (
                f"[{c.name}] expected row in table on {results.mode}"
            )
            try:
                results.assert_ingested(c)
            except AssertionError:
                actual = results.rows[c.name].get(c.col)
                parsed = json.loads(actual) if isinstance(actual, str) else actual
                _log_divergence(
                    results.mode,
                    c.name,
                    f"JSON string stored as literal array element {parsed!r} (v3 stores {c.expected_value or c.value!r})",
                )
                divergences.append(c.name)

    if divergences:
        pytest.xfail(f"v4-ht array string handling diverges from v3: {divergences}")


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

    KNOWN DIVERGENCE: v4 stores JSON null in VARIANT as string 'null'
    instead of SQL NULL.
    """
    c = next(c for c in CASES if c.col == col and c.group == "null")
    assert c.name in results.rows, (
        f"[{c.name}] expected in table but not found (mode={results.mode})"
    )
    actual = results.rows[c.name].get(c.col)

    # KNOWN DIVERGENCE: v4 stores VARIANT null as string 'null'
    if actual is not None and results.mode != "v3" and col == "COL_VARIANT":
        _log_divergence(results.mode, c.name, f"expected SQL NULL, got {actual!r}")
        pytest.xfail(f"v4 stores VARIANT null as {actual!r} instead of SQL NULL")

    assert actual is None, (
        f"[{c.name}] expected NULL, got {actual!r} (mode={results.mode})"
    )


# ---------------------------------------------------------------------------
# Cross-type mismatch — DLQ behavior
# ---------------------------------------------------------------------------


def test_cross_type_mismatch(results):
    """Values sent to incompatible column types — expected DLQ/drop."""
    if results.mode == "v3":
        _assert_all(results, cases_where(group="xtype"))
        return

    # v4: track divergences from v3 reference behavior.
    divergences = []
    for c in cases_where(group="xtype"):
        in_table = c.name in results.rows
        in_dlq = c.name in results.dlq_ids

        if c.expect == "ingested":
            if in_table:
                results.assert_ingested(c)
            else:
                _log_divergence(
                    results.mode,
                    c.name,
                    f"rejected (v3 ingests via coercion); in_dlq={in_dlq}",
                )
                divergences.append(c.name)
        else:
            if in_table:
                actual = results.rows[c.name].get(c.col)
                _log_divergence(
                    results.mode,
                    c.name,
                    f"ingested (v3 rejects): value={actual!r}",
                )
                divergences.append(c.name)
            elif results.mode != "v4-ht" and not in_dlq:
                _log_divergence(
                    results.mode,
                    c.name,
                    "rejected without DLQ (silently dropped)",
                )
                divergences.append(c.name)
            else:
                results.assert_error(c)

    if divergences:
        pytest.xfail(f"v4 cross-type handling diverges from v3 on: {divergences}")


# ---------------------------------------------------------------------------
# Error table accounting (v4-ht only)
# ---------------------------------------------------------------------------


def test_error_table_accounting(results):
    """v4-ht: verify error table captured rejected records."""
    if results.mode != "v4-ht":
        pytest.skip("Error table only applicable to v4-ht mode")

    expected_errors = sum(
        1 for c in CASES if c.expect == "error" and c.name not in results.rows
    )

    assert len(results.error_table_rows) >= expected_errors, (
        f"Expected at least {expected_errors} error table rows for v4-ht but found "
        f"{len(results.error_table_rows)} — errors may be silently dropped"
    )

    for row in results.error_table_rows:
        assert row.get("ERROR_CODE") is not None, (
            f"Error table row missing ERROR_CODE: {row}"
        )
