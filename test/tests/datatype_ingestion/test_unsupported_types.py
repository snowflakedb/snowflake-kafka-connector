"""Tests for data types unsupported (or partially supported) by the Kafka connector.

These types crash streaming channels or fail in ways that can't share a batch
connector with well-behaved types. Each test gets its own connector via the
``standalone_ingest`` fixture.

Types tested:
  - GEOGRAPHY: GeoJSON data — not supported by Snowpipe Streaming
  - GEOMETRY: WKT data — not supported by Snowpipe Streaming
  - VECTOR: embedding arrays — v4 only, not supported by v3 classic SDK
  - Structured OBJECT/ARRAY: typed columns with parameters — rejected by ColumnSchema
"""

import pytest


# ---------------------------------------------------------------------------
# Geospatial types (unsupported by Snowpipe Streaming)
# ---------------------------------------------------------------------------


def test_dt_geography(standalone_ingest):
    """GEOGRAPHY: GeoJSON point data — expected to fail (unsupported)."""
    result = standalone_ingest(
        "dt_geography",
        "GEOGRAPHY",
        [
            '{"type":"Point","coordinates":[-122.35,37.55]}',
            '{"type":"Point","coordinates":[0,0]}',
        ],
    )
    # Streaming does not support GEOGRAPHY — expect connector error or DLQ.
    assert result.connector_error or result.dlq_count > 0 or len(result.values) == 0, (
        f"Expected failure for GEOGRAPHY, but got {len(result.values)} rows "
        f"with no errors"
    )


def test_dt_geometry(standalone_ingest):
    """GEOMETRY: WKT geometry data — expected to fail (unsupported)."""
    result = standalone_ingest(
        "dt_geometry",
        "GEOMETRY",
        ["POINT(-122.35 37.55)", "POINT(0 0)"],
    )
    assert result.connector_error or result.dlq_count > 0 or len(result.values) == 0, (
        f"Expected failure for GEOMETRY, but got {len(result.values)} rows "
        f"with no errors"
    )


# ---------------------------------------------------------------------------
# VECTOR type (v4 only)
# ---------------------------------------------------------------------------


def test_dt_vector(standalone_ingest, ingestion_mode):
    """VECTOR(FLOAT, 3): vector embeddings — not supported by v3 classic SDK."""
    if ingestion_mode == "v3":
        pytest.skip("VECTOR not supported by v3 classic Streaming SDK")
    result = standalone_ingest(
        "dt_vector",
        "VECTOR(FLOAT, 3)",
        [[1.0, 2.0, 3.0], [0.0, 0.0, 0.0], [-1.5, 2.5, -3.5]],
    )
    assert len(result.values) == 3, (
        f"Expected 3 VECTOR rows, got {len(result.values)}; "
        f"error={result.connector_error}"
    )


# ---------------------------------------------------------------------------
# Structured OBJECT / ARRAY (rejected by ColumnSchema.java:326-349)
# ---------------------------------------------------------------------------


def test_dt_structured_object(standalone_ingest):
    """Structured OBJECT(name VARCHAR, age NUMBER) — rejected by ColumnSchema.

    ColumnSchema.java lines 326-349 reject OBJECT columns that have typed
    parameters. The connector should fail or DLQ these records.
    """
    result = standalone_ingest(
        "dt_struct_obj",
        "OBJECT(name VARCHAR, age NUMBER)",
        [{"name": "Alice", "age": 30}],
    )
    assert result.connector_error or result.dlq_count > 0 or len(result.values) == 0, (
        f"Expected failure for structured OBJECT, but got {len(result.values)} rows "
        f"with no errors"
    )


def test_dt_structured_array(standalone_ingest):
    """Structured ARRAY(NUMBER) — rejected by ColumnSchema.

    ColumnSchema.java lines 326-349 reject ARRAY columns that have typed
    parameters. The connector should fail or DLQ these records.
    """
    result = standalone_ingest(
        "dt_struct_arr",
        "ARRAY(NUMBER)",
        [[1, 2, 3]],
    )
    assert result.connector_error or result.dlq_count > 0 or len(result.values) == 0, (
        f"Expected failure for structured ARRAY, but got {len(result.values)} rows "
        f"with no errors"
    )
