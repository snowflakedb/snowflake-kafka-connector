"""Tests for data types unsupported (or partially supported) by the Kafka connector.

These types crash streaming channels or fail in ways that can't share a batch
connector with well-behaved types. Each test gets its own connector via the
``ingest_one_type_abort`` fixture (abort mode — errors.tolerance=none).

The connector task fails immediately on unsupported types for v3.
For v4 modes (v4-compat and v4-ht), the async SDK flush failure does not
propagate back to the KC task — the task stays RUNNING and 0 rows land.

Types tested:
  - GEOGRAPHY: GeoJSON data — not supported by Snowpipe Streaming
  - GEOMETRY: WKT data — not supported by Snowpipe Streaming
  - VECTOR: embedding arrays — v4 only, not supported by v3 classic SDK
  - Structured OBJECT/ARRAY: typed columns with parameters — v3 rejects,
    v4 accepts (known divergence)
"""

import pytest

pytestmark = pytest.mark.compatibility


def _assert_connector_error(result, ingestion_mode, type_name, expected_fragments):
    """Assert the connector failed with an error matching at least one expected fragment.

    For v4 modes (v4-compat and v4-ht), the SDK async flush failure does not
    propagate back to the KC task — the task stays RUNNING and 0 rows land.
    For v3, the channel open is rejected synchronously and the task fails.
    """
    if ingestion_mode in ("v4-ht", "v4-compat"):
        assert len(result.values) == 0, (
            f"Expected no rows for {type_name} on {ingestion_mode}, got {len(result.values)}"
        )
        return

    assert result.connector_error is not None, (
        f"Expected connector task failure for {type_name} on {ingestion_mode}, "
        f"but connector succeeded with {len(result.values)} rows"
    )
    matched = any(f in result.connector_error for f in expected_fragments)
    assert matched, (
        f"Connector error for {type_name} on {ingestion_mode} did not match "
        f"any expected pattern {expected_fragments}.\n"
        f"Actual error (first 500 chars): {result.connector_error[:500]}"
    )


# Error patterns observed in connector traces for v3:
#   SFException "does not support columns of type" (channel open rejected by server)
# v4 modes never reach this assertion — they exit early with 0 rows.
_GEO_ERROR_FRAGMENTS = [
    "does not support columns of type",
    "TopicPartitionChannelInsertionException",
    "Failed to insert rows",
]

# v3 structured types / unsupported column types: channel open or schema setup failure
_CHANNEL_OPEN_ERROR_FRAGMENTS = [
    "does not support columns of type",
    "Open channel request failed",
    "Unknown data type for column",
]


# ---------------------------------------------------------------------------
# Geospatial types (unsupported by Snowpipe Streaming)
# ---------------------------------------------------------------------------


def test_dt_geography(ingest_one_type_abort, ingestion_mode):
    """GEOGRAPHY: GeoJSON point data — unsupported by Snowpipe Streaming."""
    result = ingest_one_type_abort(
        "dt_geography",
        "GEOGRAPHY",
        [
            '{"type":"Point","coordinates":[-122.35,37.55]}',
            '{"type":"Point","coordinates":[0,0]}',
        ],
    )
    _assert_connector_error(result, ingestion_mode, "GEOGRAPHY", _GEO_ERROR_FRAGMENTS)


def test_dt_geometry(ingest_one_type_abort, ingestion_mode):
    """GEOMETRY: WKT geometry data — unsupported by Snowpipe Streaming."""
    result = ingest_one_type_abort(
        "dt_geometry",
        "GEOMETRY",
        ["POINT(-122.35 37.55)", "POINT(0 0)"],
    )
    _assert_connector_error(result, ingestion_mode, "GEOMETRY", _GEO_ERROR_FRAGMENTS)


# ---------------------------------------------------------------------------
# VECTOR type (v4 only)
# ---------------------------------------------------------------------------


def test_dt_vector(ingest_one_type_abort, ingestion_mode):
    """VECTOR(FLOAT, 3): vector embeddings — not supported by v3 classic SDK."""
    result = ingest_one_type_abort(
        "dt_vector",
        "VECTOR(FLOAT, 3)",
        [[1.0, 2.0, 3.0], [0.0, 0.0, 0.0], [-1.5, 2.5, -3.5]],
    )
    if ingestion_mode == "v3":
        _assert_connector_error(
            result, ingestion_mode, "VECTOR", _CHANNEL_OPEN_ERROR_FRAGMENTS
        )
    else:
        assert len(result.values) == 3, (
            f"Expected 3 VECTOR rows, got {len(result.values)}; "
            f"error={result.connector_error}"
        )


# ---------------------------------------------------------------------------
# Structured OBJECT / ARRAY
#
# KNOWN DIVERGENCE: v3 ColumnSchema rejects OBJECT/ARRAY with typed parameters,
# but v4 accepts them.  v4's SSv2 handles structured types natively.
# ---------------------------------------------------------------------------


def test_dt_structured_object(ingest_one_type_abort, ingestion_mode):
    """Structured OBJECT(name VARCHAR, age NUMBER) — rejected by v3, accepted by v4."""
    result = ingest_one_type_abort(
        "dt_struct_obj",
        "OBJECT(name VARCHAR, age NUMBER)",
        [{"name": "Alice", "age": 30}],
    )
    if ingestion_mode == "v3":
        _assert_connector_error(
            result, ingestion_mode, "structured OBJECT", _CHANNEL_OPEN_ERROR_FRAGMENTS
        )
    else:
        # v4-compat and v4-ht accept structured OBJECT
        assert len(result.values) == 1, (
            f"Expected 1 row for structured OBJECT on {ingestion_mode}, "
            f"got {len(result.values)}; error={result.connector_error}"
        )


def test_dt_structured_array(ingest_one_type_abort, ingestion_mode):
    """Structured ARRAY(NUMBER) — rejected by v3, accepted by v4."""
    result = ingest_one_type_abort(
        "dt_struct_arr",
        "ARRAY(NUMBER)",
        [[1, 2, 3]],
    )
    if ingestion_mode == "v3":
        _assert_connector_error(
            result, ingestion_mode, "structured ARRAY", _CHANNEL_OPEN_ERROR_FRAGMENTS
        )
    else:
        # v4-compat and v4-ht accept structured ARRAY
        assert len(result.values) == 1, (
            f"Expected 1 row for structured ARRAY on {ingestion_mode}, "
            f"got {len(result.values)}; error={result.connector_error}"
        )
