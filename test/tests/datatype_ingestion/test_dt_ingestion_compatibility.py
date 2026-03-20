"""Data-type ingestion compatibility tests.

Verify that each canonical Snowflake data type can be ingested through the
Kafka connector in both v3 and v4-compatibility modes.  Each test sends a mix
of valid and invalid (when applicable) JSON values, then asserts:

  * valid rows landed in the target Snowflake table
  * invalid rows were routed to the Dead Letter Queue (DLQ)

Parameterized across two ingestion modes via the ``ingestion_mode`` fixture:
  - v3:        SnowflakeSinkConnector with SNOWPIPE_STREAMING
  - v4-compat: SnowflakeStreamingSinkConnector with client.validation.enabled=true

Type aliases (INT, STRING, DOUBLE, DECIMAL, CHAR, etc.) are not tested
separately — they resolve to the same storage type and code path in Snowflake.

Reference: https://docs.snowflake.com/en/sql-reference/intro-summary-data-types
"""

import datetime
import json

import pytest


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def _run_test(
    driver,
    mode_name_salt,
    create_typed_connector,
    wait_for_rows,
    typed_table,
    *,
    test_id,
    col_ddl,
    good_values,
    bad_values=(),
):
    """Send good + bad records to a typed column.

    * Waits until all *good_values* land in the Snowflake table.
    * When *bad_values* is non-empty, configures a DLQ and asserts that every
      bad record is routed there.
    * Returns the actual column values from the table (in offset order).
    """
    topic = typed_table(test_id, col_ddl)

    extra = {}
    dlq_topic = None
    if bad_values:
        dlq_topic = f"dlq_{test_id}{mode_name_salt}"
        extra = {
            "errors.tolerance": "all",
            "errors.deadletterqueue.topic.name": dlq_topic,
            "errors.deadletterqueue.topic.replication.factor": "1",
        }

    config = create_typed_connector(test_id, extra_config=extra or None)
    connector_name = config["name"]
    driver.startConnectorWaitTime()

    all_values = list(good_values) + list(bad_values)
    records = [json.dumps({"VALUE_COL": v}).encode() for v in all_values]
    keys = [json.dumps({"number": str(i)}).encode() for i in range(len(records))]
    driver.sendBytesData(topic, records, keys)

    wait_for_rows(topic, len(good_values), connector_name=connector_name)

    rows = (
        driver.snowflake_conn.cursor()
        .execute(
            f'SELECT VALUE_COL FROM {topic} ORDER BY RECORD_METADATA:"offset"::int'
        )
        .fetchall()
    )
    actual = [row[0] for row in rows]

    if bad_values and dlq_topic:
        dlq_count = driver.consume_messages(dlq_topic, 0, len(bad_values) - 1)
        assert dlq_count == len(bad_values), (
            f"Expected {len(bad_values)} DLQ records, got {dlq_count}"
        )

    return actual


# ---------------------------------------------------------------------------
# Numeric data types
# ---------------------------------------------------------------------------


def test_dt_number(
    driver, mode_name_salt, create_typed_connector, wait_for_rows, typed_table
):
    """NUMBER(38,0): integers + non-numeric strings to DLQ."""
    good = [42, 0, -100, 2147483647, -2147483648]
    bad = ["not_a_number", "abc", {"obj": 1}]
    actual = _run_test(
        driver,
        mode_name_salt,
        create_typed_connector,
        wait_for_rows,
        typed_table,
        test_id="dt_number",
        col_ddl="NUMBER",
        good_values=good,
        bad_values=bad,
    )
    assert actual == good


def test_dt_number_with_scale(
    driver, mode_name_salt, create_typed_connector, wait_for_rows, typed_table
):
    """NUMBER(10,2): decimal values + non-numeric string to DLQ."""
    good = [123.45, -0.01, 0.0, 99999.99]
    bad = ["text"]
    actual = _run_test(
        driver,
        mode_name_salt,
        create_typed_connector,
        wait_for_rows,
        typed_table,
        test_id="dt_numscale",
        col_ddl="NUMBER(10,2)",
        good_values=good,
        bad_values=bad,
    )
    for a, e in zip(actual, good):
        assert float(a) == pytest.approx(e, abs=0.01)


def test_dt_float(
    driver, mode_name_salt, create_typed_connector, wait_for_rows, typed_table
):
    """FLOAT: floating-point values + non-numeric values to DLQ."""
    good = [3.14, -1.5, 0.0, 1.0e10]
    bad = ["text", [1, 2]]
    actual = _run_test(
        driver,
        mode_name_salt,
        create_typed_connector,
        wait_for_rows,
        typed_table,
        test_id="dt_float",
        col_ddl="FLOAT",
        good_values=good,
        bad_values=bad,
    )
    for a, e in zip(actual, good):
        assert a == pytest.approx(e, rel=1e-6)


# ---------------------------------------------------------------------------
# String & binary data types
# ---------------------------------------------------------------------------


def test_dt_varchar(
    driver, mode_name_salt, create_typed_connector, wait_for_rows, typed_table
):
    """VARCHAR: variable-length character strings (no meaningful negatives)."""
    good = ["hello world", "special chars: !@#$%^&*()", "a" * 1000]
    actual = _run_test(
        driver,
        mode_name_salt,
        create_typed_connector,
        wait_for_rows,
        typed_table,
        test_id="dt_varchar",
        col_ddl="VARCHAR",
        good_values=good,
    )
    assert actual == good


def test_dt_binary(
    driver,
    mode_name_salt,
    create_typed_connector,
    wait_for_rows,
    typed_table,
    ingestion_mode,
):
    """BINARY: hex-encoded binary data.

    v4-compat fails server-side: "Failed to cast variant value" for some
    hex strings.  v3 passes — the v4 Streaming SDK handles string-to-BINARY
    conversion differently.
    """
    if ingestion_mode == "v4-compat":
        pytest.skip("v4 Streaming SDK fails to cast hex strings to BINARY server-side")
    hex_values = ["48656C6C6F", "DEADBEEF", "00", "FF" * 100]
    actual = _run_test(
        driver,
        mode_name_salt,
        create_typed_connector,
        wait_for_rows,
        typed_table,
        test_id="dt_binary",
        col_ddl="BINARY",
        good_values=hex_values,
    )
    assert actual == [bytes.fromhex(v) for v in hex_values]


# ---------------------------------------------------------------------------
# Logical data type
# ---------------------------------------------------------------------------


def test_dt_boolean(
    driver, mode_name_salt, create_typed_connector, wait_for_rows, typed_table
):
    """BOOLEAN: true/false + non-coercible objects/arrays to DLQ."""
    good = [True, False, True]
    bad = [{"key": "value"}, [1, 2, 3]]
    actual = _run_test(
        driver,
        mode_name_salt,
        create_typed_connector,
        wait_for_rows,
        typed_table,
        test_id="dt_boolean",
        col_ddl="BOOLEAN",
        good_values=good,
        bad_values=bad,
    )
    assert actual == good


# ---------------------------------------------------------------------------
# Date & time data types
# ---------------------------------------------------------------------------


def test_dt_date(
    driver, mode_name_salt, create_typed_connector, wait_for_rows, typed_table
):
    """DATE: ISO date strings + invalid string to DLQ."""
    good = ["2024-01-15", "1970-01-01", "2099-12-31"]
    bad = ["not_a_date"]
    actual = _run_test(
        driver,
        mode_name_salt,
        create_typed_connector,
        wait_for_rows,
        typed_table,
        test_id="dt_date",
        col_ddl="DATE",
        good_values=good,
        bad_values=bad,
    )
    expected = [
        datetime.date(2024, 1, 15),
        datetime.date(1970, 1, 1),
        datetime.date(2099, 12, 31),
    ]
    assert actual == expected


def test_dt_time(
    driver, mode_name_salt, create_typed_connector, wait_for_rows, typed_table
):
    """TIME: time-of-day strings + invalid string to DLQ."""
    good = ["13:45:30", "00:00:00", "23:59:59"]
    bad = ["not_a_time"]
    actual = _run_test(
        driver,
        mode_name_salt,
        create_typed_connector,
        wait_for_rows,
        typed_table,
        test_id="dt_time",
        col_ddl="TIME",
        good_values=good,
        bad_values=bad,
    )
    expected = [
        datetime.time(13, 45, 30),
        datetime.time(0, 0, 0),
        datetime.time(23, 59, 59),
    ]
    assert actual == expected


def test_dt_timestamp_ntz(
    driver, mode_name_salt, create_typed_connector, wait_for_rows, typed_table
):
    """TIMESTAMP_NTZ: ISO 8601 timestamps + invalid string to DLQ."""
    good = ["2024-01-15T13:45:30", "1970-01-01T00:00:00", "2099-12-31T23:59:59"]
    bad = ["not_a_timestamp"]
    actual = _run_test(
        driver,
        mode_name_salt,
        create_typed_connector,
        wait_for_rows,
        typed_table,
        test_id="dt_ts_ntz",
        col_ddl="TIMESTAMP_NTZ",
        good_values=good,
        bad_values=bad,
    )
    expected = [
        datetime.datetime(2024, 1, 15, 13, 45, 30),
        datetime.datetime(1970, 1, 1, 0, 0, 0),
        datetime.datetime(2099, 12, 31, 23, 59, 59),
    ]
    assert actual == expected


def test_dt_timestamp_ltz(
    driver, mode_name_salt, create_typed_connector, wait_for_rows, typed_table
):
    """TIMESTAMP_LTZ: timestamps with explicit UTC offset + invalid to DLQ."""
    driver.snowflake_conn.cursor().execute("ALTER SESSION SET TIMEZONE = 'UTC'")
    good = ["2024-01-15T13:45:30+00:00", "1970-01-01T00:00:00+00:00"]
    bad = ["not_a_timestamp"]
    actual = _run_test(
        driver,
        mode_name_salt,
        create_typed_connector,
        wait_for_rows,
        typed_table,
        test_id="dt_ts_ltz",
        col_ddl="TIMESTAMP_LTZ",
        good_values=good,
        bad_values=bad,
    )
    expected_ntz = [
        datetime.datetime(2024, 1, 15, 13, 45, 30),
        datetime.datetime(1970, 1, 1, 0, 0, 0),
    ]
    for a, e in zip(actual, expected_ntz):
        assert isinstance(a, datetime.datetime)
        assert a.replace(tzinfo=None) == e


def test_dt_timestamp_tz(
    driver, mode_name_salt, create_typed_connector, wait_for_rows, typed_table
):
    """TIMESTAMP_TZ: timestamps with explicit timezone + invalid to DLQ."""
    good = ["2024-01-15T13:45:30+05:00", "1970-01-01T00:00:00+00:00"]
    bad = ["not_a_timestamp"]
    actual = _run_test(
        driver,
        mode_name_salt,
        create_typed_connector,
        wait_for_rows,
        typed_table,
        test_id="dt_ts_tz",
        col_ddl="TIMESTAMP_TZ",
        good_values=good,
        bad_values=bad,
    )
    for a in actual:
        assert isinstance(a, datetime.datetime)
        assert a.tzinfo is not None


# ---------------------------------------------------------------------------
# Semi-structured data types
# ---------------------------------------------------------------------------


def test_dt_variant(
    driver, mode_name_salt, create_typed_connector, wait_for_rows, typed_table
):
    """VARIANT: JSON objects and arrays (no meaningful negatives)."""
    good = [
        {"key": "value", "number": 42},
        [1, 2, 3],
        {"nested": [True, False, None]},
    ]
    actual = _run_test(
        driver,
        mode_name_salt,
        create_typed_connector,
        wait_for_rows,
        typed_table,
        test_id="dt_variant",
        col_ddl="VARIANT",
        good_values=good,
    )
    for a, e in zip(actual, good):
        parsed = json.loads(a) if isinstance(a, str) else a
        assert parsed == e


def test_dt_object(
    driver, mode_name_salt, create_typed_connector, wait_for_rows, typed_table
):
    """OBJECT: JSON object values."""
    good = [
        {"key": "value"},
        {"nested": {"a": 1, "b": 2}},
        {"array_val": [1, 2, 3]},
    ]
    actual = _run_test(
        driver,
        mode_name_salt,
        create_typed_connector,
        wait_for_rows,
        typed_table,
        test_id="dt_object",
        col_ddl="OBJECT",
        good_values=good,
    )
    for a, e in zip(actual, good):
        assert json.loads(a) == e


def test_dt_array(
    driver, mode_name_salt, create_typed_connector, wait_for_rows, typed_table
):
    """ARRAY: JSON array values."""
    good = [
        ["a", "b", "c"],
        [1, 2, 3],
        [{"key": "value"}, {"key": "value2"}],
    ]
    actual = _run_test(
        driver,
        mode_name_salt,
        create_typed_connector,
        wait_for_rows,
        typed_table,
        test_id="dt_array",
        col_ddl="ARRAY",
        good_values=good,
    )
    for a, e in zip(actual, good):
        assert json.loads(a) == e


# ---------------------------------------------------------------------------
# Geospatial data types (unsupported — expected failures)
# ---------------------------------------------------------------------------


@pytest.mark.xfail(
    reason="GEOGRAPHY not supported by Kafka connector via Snowpipe Streaming",
    raises=AssertionError,
    strict=True,
)
def test_dt_geography(
    driver, mode_name_salt, create_typed_connector, wait_for_rows, typed_table
):
    """GEOGRAPHY: GeoJSON point data — expected to fail (unsupported)."""
    good = [
        '{"type":"Point","coordinates":[-122.35,37.55]}',
        '{"type":"Point","coordinates":[0,0]}',
    ]
    _run_test(
        driver,
        mode_name_salt,
        create_typed_connector,
        wait_for_rows,
        typed_table,
        test_id="dt_geography",
        col_ddl="GEOGRAPHY",
        good_values=good,
    )


@pytest.mark.xfail(
    reason="GEOMETRY not supported by Kafka connector via Snowpipe Streaming",
    raises=AssertionError,
    strict=True,
)
def test_dt_geometry(
    driver, mode_name_salt, create_typed_connector, wait_for_rows, typed_table
):
    """GEOMETRY: WKT geometry data — expected to fail (unsupported)."""
    good = ["POINT(-122.35 37.55)", "POINT(0 0)"]
    _run_test(
        driver,
        mode_name_salt,
        create_typed_connector,
        wait_for_rows,
        typed_table,
        test_id="dt_geometry",
        col_ddl="GEOMETRY",
        good_values=good,
    )


# ---------------------------------------------------------------------------
# Vector data type (v4 only)
# ---------------------------------------------------------------------------


def test_dt_vector(
    driver,
    mode_name_salt,
    create_typed_connector,
    wait_for_rows,
    typed_table,
    ingestion_mode,
):
    """VECTOR(FLOAT, 3): vector embeddings (not supported by v3 classic SDK)."""
    if ingestion_mode == "v3":
        pytest.skip("VECTOR not supported by v3 classic Streaming SDK")
    good = [
        [1.0, 2.0, 3.0],
        [0.0, 0.0, 0.0],
        [-1.5, 2.5, -3.5],
    ]
    actual = _run_test(
        driver,
        mode_name_salt,
        create_typed_connector,
        wait_for_rows,
        typed_table,
        test_id="dt_vector",
        col_ddl="VECTOR(FLOAT, 3)",
        good_values=good,
    )
    assert len(actual) == len(good)
