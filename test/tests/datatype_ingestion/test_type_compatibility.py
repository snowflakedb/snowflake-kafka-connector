"""Data-type ingestion compatibility tests.

Verify that each canonical Snowflake data type can be ingested through the
Kafka connector in both v3 and v4-compatibility modes.  Each test sends a mix
of valid and invalid (when applicable) JSON values through a shared batch
connector, then asserts:

  * valid rows landed in the target Snowflake table
  * invalid rows were routed to the Dead Letter Queue (DLQ)

Assertions encode **v3 reference behavior**.  When v4-compat diverges from v3,
the test will fail — that failure IS the signal.  No ``if ingestion_mode``
branches; no ``xfail`` per mode.

Parameterized across two ingestion modes via the ``ingestion_mode`` fixture
(module-scoped):
  - v3:        SnowflakeSinkConnector with SNOWPIPE_STREAMING
  - v4-compat: SnowflakeStreamingSinkConnector with client.validation.enabled=true

Type aliases (INT, STRING, DOUBLE, DECIMAL, CHAR, etc.) are not tested
separately — they resolve to the same storage type and code path in Snowflake.

Reference: https://docs.snowflake.com/en/sql-reference/intro-summary-data-types
"""

import datetime
import json
import math

import pytest


# ---------------------------------------------------------------------------
# Numeric data types
# ---------------------------------------------------------------------------


def test_dt_number(ingest):
    """NUMBER(38,0): integers + non-numeric strings to DLQ."""
    result = ingest(
        "dt_number",
        [42, 0, -100, 2147483647, -2147483648, "not_a_number", "abc", {"obj": 1}],
    )
    assert result.values == [42, 0, -100, 2147483647, -2147483648]
    assert result.dlq_count == 3


def test_dt_number_with_scale(ingest):
    """NUMBER(10,2): decimal values + non-numeric string to DLQ."""
    result = ingest("dt_numscale", [123.45, -0.01, 0.0, 99999.99, "text"])
    for a, e in zip(result.values, [123.45, -0.01, 0.0, 99999.99]):
        assert float(a) == pytest.approx(e, abs=0.01)
    assert result.dlq_count == 1


def test_dt_float(ingest):
    """FLOAT: floating-point values + non-numeric values to DLQ."""
    result = ingest("dt_float", [3.14, -1.5, 0.0, 1.0e10, "text", [1, 2]])
    for a, e in zip(result.values, [3.14, -1.5, 0.0, 1.0e10]):
        assert a == pytest.approx(e, rel=1e-6)
    assert result.dlq_count == 2


def test_dt_float_special(ingest):
    """FLOAT special values: NaN, +Infinity, -Infinity.

    JSON RFC 8259 does not define NaN/Infinity literals. We send them as
    string representations ("NaN", "Infinity", "-Infinity") which is how
    DataValidationUtil.validateAndParseReal handles them (via Double.parseDouble).
    SSv1 and SSv2 may handle string-to-float coercion for these differently.
    """
    result = ingest("dt_fltspec", ["NaN", "Infinity", "-Infinity"])
    assert len(result.values) == 3, f"Expected 3 rows, got {len(result.values)}"
    assert math.isnan(result.values[0]), f"Expected NaN, got {result.values[0]}"
    assert result.values[1] == float("inf"), f"Expected inf, got {result.values[1]}"
    assert result.values[2] == float("-inf"), f"Expected -inf, got {result.values[2]}"
    assert result.dlq_count == 0


# ---------------------------------------------------------------------------
# String & binary data types
# ---------------------------------------------------------------------------


def test_dt_varchar(ingest):
    """VARCHAR: variable-length character strings."""
    result = ingest(
        "dt_varchar", ["hello world", "special chars: !@#$%^&*()", "a" * 1000]
    )
    assert result.values == ["hello world", "special chars: !@#$%^&*()", "a" * 1000]
    assert result.dlq_count == 0


def test_dt_varchar_length_limit(ingest):
    """VARCHAR(10): strings exceeding declared length limit.

    Snowflake silently truncates or the connector rejects overlength strings.
    This probes whether v3 and v4 handle the constraint identically.
    """
    short = "hello"  # 5 chars — fits
    exact = "0123456789"  # 10 chars — fits
    over = "a" * 20  # 20 chars — exceeds limit
    result = ingest("dt_varchar10", [short, exact, over])
    # v3 reference: Snowflake may truncate or the connector may DLQ the overlength value.
    # Assert at least the short and exact values landed.
    assert len(result.values) >= 2, (
        f"Expected at least 2 rows, got {len(result.values)}; "
        f"dlq={result.dlq_count}, error={result.connector_error}"
    )
    assert result.values[0] == short
    assert result.values[1] == exact


def test_dt_binary(ingest):
    """BINARY: hex-encoded binary data.

    KNOWN DIVERGENCE: v4-compat fails server-side on hex strings
    (SNOW-3256183). v3 passes. This test asserts v3 behavior — v4 failure
    shows up as a test failure (the signal we want).
    """
    hex_values = ["48656C6C6F", "DEADBEEF", "00", "FF" * 100]
    result = ingest("dt_binary", hex_values)
    assert len(result.values) == 4, (
        f"Expected 4 rows, got {len(result.values)}; "
        f"dlq={result.dlq_count}, error={result.connector_error}"
    )
    assert result.values == [bytes.fromhex(v) for v in hex_values]
    assert result.dlq_count == 0


# ---------------------------------------------------------------------------
# Logical data type
# ---------------------------------------------------------------------------


def test_dt_boolean(ingest):
    """BOOLEAN: true/false + non-coercible objects/arrays to DLQ."""
    result = ingest("dt_boolean", [True, False, True, {"key": "value"}, [1, 2, 3]])
    assert result.values == [True, False, True]
    assert result.dlq_count == 2


def test_dt_boolean_coercion(ingest):
    """BOOLEAN coercion: numeric 0/1 and string tokens.

    DataValidationUtil.validateAndParseBoolean accepts: 0, 1, "true", "false",
    "yes", "no", "y", "n", "t", "f", "on", "off" (case-insensitive).

    KNOWN DIVERGENCE: v4-compat RowValidator rejects "yes"/"no" (and likely
    other string tokens beyond "true"/"false"/"0"/"1") while v3 SSv1 accepts
    them. This test asserts v3 behavior — all 6 values land.
    """
    result = ingest("dt_boolcoerce", [0, 1, "true", "false", "yes", "no"])
    assert result.values == [False, True, True, False, True, False], (
        f"Boolean coercion divergence: got {result.values}, "
        f"dlq={result.dlq_count}, errors={result.dlq_errors}"
    )
    assert result.dlq_count == 0


# ---------------------------------------------------------------------------
# Date & time data types
# ---------------------------------------------------------------------------


def test_dt_date(ingest):
    """DATE: ISO date strings + invalid string to DLQ."""
    result = ingest("dt_date", ["2024-01-15", "1970-01-01", "2099-12-31", "not_a_date"])
    assert result.values == [
        datetime.date(2024, 1, 15),
        datetime.date(1970, 1, 1),
        datetime.date(2099, 12, 31),
    ]
    assert result.dlq_count == 1


def test_dt_time(ingest):
    """TIME: time-of-day strings + invalid string to DLQ."""
    result = ingest("dt_time", ["13:45:30", "00:00:00", "23:59:59", "not_a_time"])
    assert result.values == [
        datetime.time(13, 45, 30),
        datetime.time(0, 0, 0),
        datetime.time(23, 59, 59),
    ]
    assert result.dlq_count == 1


def test_dt_timestamp_ntz(ingest):
    """TIMESTAMP_NTZ: ISO 8601 timestamps + invalid string to DLQ."""
    result = ingest(
        "dt_ts_ntz",
        [
            "2024-01-15T13:45:30",
            "1970-01-01T00:00:00",
            "2099-12-31T23:59:59",
            "not_a_timestamp",
        ],
    )
    assert result.values == [
        datetime.datetime(2024, 1, 15, 13, 45, 30),
        datetime.datetime(1970, 1, 1, 0, 0, 0),
        datetime.datetime(2099, 12, 31, 23, 59, 59),
    ]
    assert result.dlq_count == 1


def test_dt_timestamp_ntz_epoch(ingest):
    """TIMESTAMP_NTZ with integer epoch — probes v3/v4 type acceptance divergence.

    KNOWN DIVERGENCE: v4's RowValidator rejects java.lang.Long for TIMESTAMP_NTZ.
    It only accepts: String, LocalDate, LocalDateTime, ZonedDateTime, OffsetDateTime.
    V3 SSv1 accepts integer epochs and converts them to timestamps.
    This test asserts v3 behavior — the epoch lands as a timestamp.
    """
    # 1705312800 = 2024-01-15T10:00:00 UTC
    result = ingest("dt_ts_ntzep", [1705312800])
    expected = datetime.datetime(2024, 1, 15, 10, 0, 0)
    assert len(result.values) == 1, (
        f"Expected 1 row, got {len(result.values)}; "
        f"dlq={result.dlq_count}, error={result.connector_error}"
    )
    assert result.values[0] == expected, (
        f"Timezone divergence: expected {expected} (UTC), got {result.values[0]}. "
        f"If off by ~8h, RowValidator is using America/Los_Angeles default."
    )
    assert result.dlq_count == 0


def test_dt_timestamp_ltz(ingest):
    """TIMESTAMP_LTZ: timestamps with explicit UTC offset + invalid to DLQ."""
    result = ingest(
        "dt_ts_ltz",
        [
            "2024-01-15T13:45:30+00:00",
            "1970-01-01T00:00:00+00:00",
            "not_a_timestamp",
        ],
    )
    expected_ntz = [
        datetime.datetime(2024, 1, 15, 13, 45, 30),
        datetime.datetime(1970, 1, 1, 0, 0, 0),
    ]
    assert len(result.values) == 2
    for a, e in zip(result.values, expected_ntz):
        assert isinstance(a, datetime.datetime)
        assert a.replace(tzinfo=None) == e
    assert result.dlq_count == 1


def test_dt_timestamp_tz(ingest):
    """TIMESTAMP_TZ: timestamps with explicit timezone + invalid to DLQ."""
    result = ingest(
        "dt_ts_tz",
        [
            "2024-01-15T13:45:30+05:00",
            "1970-01-01T00:00:00+00:00",
            "not_a_timestamp",
        ],
    )
    assert len(result.values) == 2
    for a in result.values:
        assert isinstance(a, datetime.datetime)
        assert a.tzinfo is not None
    assert result.dlq_count == 1


# ---------------------------------------------------------------------------
# Semi-structured data types
# ---------------------------------------------------------------------------


def test_dt_variant(ingest):
    """VARIANT: JSON objects, arrays, and JSON-encoded string probe.

    Includes a string containing valid JSON ('{"a":1}') to probe the known
    SSv1/SSv2 divergence: SSv1 parses JSON-like strings into native JSON
    objects in VARIANT columns, while SSv2 may store them as string literals.
    """
    good = [
        {"key": "value", "number": 42},
        [1, 2, 3],
        {"nested": [True, False, None]},
        '{"a":1}',
    ]
    result = ingest("dt_variant", good)
    assert len(result.values) == 4, (
        f"Expected 4 rows, got {len(result.values)}; "
        f"dlq={result.dlq_count}, errors={result.dlq_errors}"
    )
    for i, (a, e) in enumerate(zip(result.values, good)):
        parsed = json.loads(a) if isinstance(a, str) else a
        if isinstance(e, str):
            expected_obj = json.loads(e)
            if isinstance(parsed, str):
                parsed = json.loads(parsed)
            assert parsed == expected_obj, (
                f"VARIANT JSON-string divergence at index {i}: "
                f"sent {e!r}, got {a!r} (parsed as {parsed!r})"
            )
        else:
            assert parsed == e
    assert result.dlq_count == 0


def test_dt_object(ingest):
    """OBJECT: JSON object values."""
    good = [
        {"key": "value"},
        {"nested": {"a": 1, "b": 2}},
        {"array_val": [1, 2, 3]},
    ]
    result = ingest("dt_object", good)
    assert len(result.values) == 3
    for a, e in zip(result.values, good):
        assert json.loads(a) == e
    assert result.dlq_count == 0


def test_dt_array(ingest):
    """ARRAY: JSON array values."""
    good = [
        ["a", "b", "c"],
        [1, 2, 3],
        [{"key": "value"}, {"key": "value2"}],
    ]
    result = ingest("dt_array", good)
    assert len(result.values) == 3
    for a, e in zip(result.values, good):
        assert json.loads(a) == e
    assert result.dlq_count == 0


# ---------------------------------------------------------------------------
# NULL handling
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "type_name,col_ddl",
    [
        ("number", "NUMBER"),
        ("float", "FLOAT"),
        ("varchar", "VARCHAR"),
        ("boolean", "BOOLEAN"),
        ("date", "DATE"),
        ("time", "TIME"),
        ("ts_ntz", "TIMESTAMP_NTZ"),
        ("ts_ltz", "TIMESTAMP_LTZ"),
        ("ts_tz", "TIMESTAMP_TZ"),
        ("variant", "VARIANT"),
        ("object", "OBJECT"),
        ("array", "ARRAY"),
    ],
)
def test_dt_null(ingest, type_name, col_ddl):
    """NULL in every supported column type — must be stored as SQL NULL.

    KNOWN DIVERGENCE for VARIANT: v4-compat stores JSON null as the string
    'null' while v3 stores SQL NULL.
    """
    result = ingest(f"dt_null_{type_name}", [None])
    assert len(result.values) == 1, (
        f"Expected 1 row for NULL {col_ddl}, got {len(result.values)}; "
        f"dlq={result.dlq_count}, error={result.connector_error}"
    )
    assert result.values == [None], (
        f"Expected SQL NULL for {col_ddl}, got {result.values}. "
        f"If 'null' string, this is the known VARIANT/semi-structured divergence."
    )
    assert result.dlq_count == 0


# ---------------------------------------------------------------------------
# Cross-type mismatch — DLQ behavior
# ---------------------------------------------------------------------------


def test_dt_xtype_string_to_number(ingest):
    """Send a non-numeric string to a NUMBER column — must go to DLQ."""
    result = ingest("dt_xtype_str_num", ["hello", "world"])
    assert result.values == [], f"Expected no rows (all DLQ), got {result.values}"
    assert result.dlq_count == 2
    for err in result.dlq_errors:
        assert err, "DLQ error message should not be empty"


def test_dt_xtype_number_to_boolean(ingest):
    """Send a large number (not 0/1) to a BOOLEAN column — probes coercion boundary."""
    result = ingest("dt_xtype_num_bool", [42, -1, 999])
    # v3 reference: numbers other than 0/1 may be rejected or coerced.
    # The key signal is whether the behavior matches across modes.
    total = len(result.values) + result.dlq_count
    assert total == 3, (
        f"Lost records: {len(result.values)} rows + {result.dlq_count} DLQ = {total}, expected 3"
    )


def test_dt_xtype_object_to_varchar(ingest):
    """Send a JSON object to a VARCHAR column — probes stringification behavior."""
    obj = {"key": "value"}
    result = ingest("dt_xtype_obj_str", [obj])
    # Both modes should either stringify or DLQ — the key is parity.
    total = len(result.values) + result.dlq_count
    assert total == 1, (
        f"Lost records: {len(result.values)} rows + {result.dlq_count} DLQ = {total}, expected 1"
    )


def test_dt_xtype_array_to_number(ingest):
    """Send a JSON array to a NUMBER column — must go to DLQ."""
    result = ingest("dt_xtype_arr_num", [[1, 2, 3]])
    assert result.values == [], f"Expected no rows (all DLQ), got {result.values}"
    assert result.dlq_count == 1
