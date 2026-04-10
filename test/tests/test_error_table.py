"""E2E tests for Snowflake Error Table support in v4 high-throughput mode.

Verifies:
1. Table WITHOUT error logging + v4-ht → connector starts, invalid data silently dropped
2. Table WITH error logging + v4-ht → connector starts, invalid data captured in error table
3. Schema mismatch (extra columns, no schema evolution) + v4-ht → rows captured in error table
4. Same bad record: v4-compat routes to DLQ, v4-ht routes to error table
"""

import json
import logging
import os
import time
from pathlib import Path
from typing import Callable

import pytest
from snowflake.connector.errors import ProgrammingError

from lib.driver import KafkaDriver, quote_name
from lib.fixtures.table import Table

logger = logging.getLogger(__name__)

TEMPLATE_DIR = Path("rest_request_template")
BASE_TEMPLATE = "datatype_ingestion.json"
STABILIZATION_SLEEP = int(os.environ.get("TEST_STABILIZATION_SLEEP", "30"))


def _v4_ht_config() -> dict:
    """Build a v4-ht connector config from the base template.

    Always sets errors.tolerance=all so that channel errors (row rejections
    reported by Snowflake) are tolerated and we can observe error table behavior
    rather than task failure.
    """
    base = json.loads((TEMPLATE_DIR / BASE_TEMPLATE).read_text())
    config = dict(base["config"])
    config["snowflake.enable.schematization"] = "true"
    config["snowflake.validation"] = "server_side"
    config["errors.tolerance"] = "all"
    config["snowflake.streaming.validate.compatibility.with.classic"] = "false"
    return config


def _v4_compat_dlq_config(dlq_topic: str) -> dict:
    """Build a v4-compat connector config with DLQ routing."""
    base = json.loads((TEMPLATE_DIR / BASE_TEMPLATE).read_text())
    config = dict(base["config"])
    config["snowflake.enable.schematization"] = "true"
    config["snowflake.validation"] = "client_side"
    config["errors.tolerance"] = "all"
    config["errors.deadletterqueue.topic.name"] = dlq_topic
    config["errors.deadletterqueue.topic.replication.factor"] = "1"
    config["errors.deadletterqueue.context.headers.enable"] = "true"
    config["snowflake.streaming.validate.compatibility.with.classic"] = "false"
    return config


@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
def test_error_table_without_error_logging(
    driver: KafkaDriver,
    create_table: Callable,
    create_custom_connector: Callable,
):
    """v4-ht targeting a table WITHOUT ERROR_LOGGING — connector starts, errors silently dropped."""
    table: Table = create_table(
        "et_no_logging",
        columns="(ID VARCHAR NOT NULL, VAL NUMBER, RECORD_METADATA VARIANT)",
    )
    driver.createTopics(table.name, partitionNum=1, replicationNum=1)

    connector = create_custom_connector("et_no_logging", _v4_ht_config())
    driver.startConnectorWaitTime()

    records = [
        json.dumps({"ID": "valid_1", "VAL": 42}).encode(),
        json.dumps({"ID": "invalid_1", "VAL": "not_a_number"}).encode(),
    ]
    keys = [json.dumps({"number": str(i)}).encode() for i in range(len(records))]
    driver.sendBytesData(table.name, records, keys)

    time.sleep(STABILIZATION_SLEEP)

    failed = driver.get_failed_tasks(connector.name)
    assert not failed, f"Connector task failed: {failed}"

    count = driver.select_number_of_records(table.name)
    assert count >= 1, f"Expected at least 1 row, got {count}"

    # Without ERROR_LOGGING, ERROR_TABLE() raises (no error logging enabled) or returns 0 rows.
    # Both outcomes confirm that no error logging is happening.
    cursor = driver.snowflake_conn.cursor()
    try:
        cursor.execute(f"SELECT * FROM ERROR_TABLE({quote_name(table.name)})")
        error_rows = cursor.fetchall()
        assert len(error_rows) == 0, (
            f"Expected 0 error table rows without ERROR_LOGGING, got {len(error_rows)}"
        )
        logger.info("Error table query returned 0 rows (no error logging enabled)")
    except ProgrammingError:
        # ERROR_TABLE() raises ProgrammingError when ERROR_LOGGING is not enabled — expected
        logger.info("Error table query raised as expected (ERROR_LOGGING not enabled)")


@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
def test_error_table_with_error_logging(
    driver: KafkaDriver,
    create_table: Callable,
    create_custom_connector: Callable,
):
    """v4-ht targeting a table WITH ERROR_LOGGING — invalid data captured in error table."""
    table: Table = create_table(
        "et_with_logging",
        columns=(
            "(ID VARCHAR NOT NULL, VAL NUMBER, RECORD_METADATA VARIANT) ERROR_LOGGING = TRUE"
        ),
    )
    driver.createTopics(table.name, partitionNum=1, replicationNum=1)

    connector = create_custom_connector("et_with_logging", _v4_ht_config())
    driver.startConnectorWaitTime()

    records = [
        json.dumps({"ID": "valid_1", "VAL": 42}).encode(),
        json.dumps({"ID": "invalid_1", "VAL": "not_a_number"}).encode(),
        json.dumps({"ID": "invalid_2", "VAL": {"nested": True}}).encode(),
    ]
    keys = [json.dumps({"number": str(i)}).encode() for i in range(len(records))]
    driver.sendBytesData(table.name, records, keys)

    time.sleep(STABILIZATION_SLEEP)

    failed = driver.get_failed_tasks(connector.name)
    assert not failed, f"Connector task failed: {failed}"

    count = driver.select_number_of_records(table.name)
    assert count >= 1, f"Expected at least 1 row, got {count}"

    cursor = driver.snowflake_conn.cursor()
    cursor.execute(f"SELECT * FROM ERROR_TABLE({quote_name(table.name)})")
    col_names = [desc[0] for desc in cursor.description]
    error_rows = cursor.fetchall()
    logger.info("Error table rows (with logging): %d", len(error_rows))

    assert len(error_rows) >= 2, (
        f"Expected at least 2 error rows (2 invalid records sent), got {len(error_rows)}"
    )
    for row in error_rows:
        row_dict = dict(zip(col_names, row))
        logger.info("Error table entry: %s", row_dict)
        assert row_dict.get("ERROR_CODE") is not None, (
            f"Error table row missing ERROR_CODE: {row_dict}"
        )


@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
def test_error_table_schema_mismatch(
    driver: KafkaDriver,
    create_table: Callable,
    create_custom_connector: Callable,
):
    """v4-ht: both value validation and schema mismatch errors land in the error table.

    Two distinct rejection reasons are exercised:
    - Value validation: ID value too long for VARCHAR(5) constraint.
    - Schema mismatch: VAL column is NOT NULL but absent from the record
      (SSv2 treats the missing key as NULL, violating the constraint).
    """
    table: Table = create_table(
        "et_schema_mismatch",
        columns=(
            "(ID VARCHAR(5) NOT NULL, VAL NUMBER NOT NULL, RECORD_METADATA VARIANT)"
            " ERROR_LOGGING = TRUE"
        ),
    )
    driver.createTopics(table.name, partitionNum=1, replicationNum=1)

    connector = create_custom_connector("et_schema_mismatch", _v4_ht_config())
    driver.startConnectorWaitTime()

    records = [
        # Valid record.
        json.dumps({"ID": "ok", "VAL": 42}).encode(),
        # Value validation: ID exceeds VARCHAR(5).
        json.dumps({"ID": "toolong", "VAL": 10}).encode(),
        # Schema mismatch: VAL is NOT NULL but missing from the payload.
        json.dumps({"ID": "miss"}).encode(),
    ]
    keys = [json.dumps({"number": str(i)}).encode() for i in range(len(records))]
    driver.sendBytesData(table.name, records, keys)

    time.sleep(STABILIZATION_SLEEP)

    failed = driver.get_failed_tasks(connector.name)
    assert not failed, f"Connector task failed: {failed}"

    count = driver.select_number_of_records(table.name)
    assert count >= 1, f"Expected at least 1 row (valid record), got {count}"

    cursor = driver.snowflake_conn.cursor()
    cursor.execute(f"SELECT * FROM ERROR_TABLE({quote_name(table.name)})")
    col_names = [desc[0] for desc in cursor.description]
    error_rows = cursor.fetchall()
    logger.info("Schema mismatch error table rows: %d", len(error_rows))

    assert len(error_rows) >= 2, (
        f"Expected at least 2 error rows (value overflow + NOT NULL violation),"
        f" got {len(error_rows)}"
    )
    for row in error_rows:
        row_dict = dict(zip(col_names, row))
        assert row_dict.get("ERROR_CODE") is not None, (
            f"Error table row missing ERROR_CODE: {row_dict}"
        )


@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
def test_error_table_vs_dlq_routing(
    driver: KafkaDriver,
    create_table: Callable,
    create_custom_connector: Callable,
):
    """Same bad record routes differently depending on validation mode.

    v4-compat (client_side validation + DLQ): invalid records are caught
    client-side and routed to the DLQ topic.

    v4-ht (server_side validation + error table): invalid records pass through
    to Snowflake, which rejects them into the error table.
    """
    # v4-compat table — schema mismatch caught client-side → DLQ.
    table_compat: Table = create_table(
        "et_routing_compat",
        columns="(ID VARCHAR NOT NULL, VAL NUMBER, RECORD_METADATA VARIANT)",
    )
    # v4-ht table — schema mismatch caught server-side → error table.
    table_ht: Table = create_table(
        "et_routing_ht",
        columns="(ID VARCHAR NOT NULL, VAL NUMBER, RECORD_METADATA VARIANT) ERROR_LOGGING = TRUE",
    )

    driver.createTopics(table_compat.name, partitionNum=1, replicationNum=1)
    driver.createTopics(table_ht.name, partitionNum=1, replicationNum=1)

    # DLQ topic for the compat connector; must exist before the connector starts.
    dlq_topic = f"dlq_{table_compat.name.lower()}"
    driver.createTopics(dlq_topic, partitionNum=1, replicationNum=1)

    # Each connector uses default topic→table routing (connector name = topic = table).
    # No topic2table.map needed.
    connector_compat = create_custom_connector(
        "et_routing_compat", _v4_compat_dlq_config(dlq_topic)
    )
    connector_ht = create_custom_connector("et_routing_ht", _v4_ht_config())

    driver.startConnectorWaitTime()

    # Send the same records to both topics: one valid, two invalid.
    records = [
        json.dumps({"ID": "valid_1", "VAL": 42}).encode(),
        json.dumps({"ID": "invalid_1", "VAL": "not_a_number"}).encode(),
        json.dumps({"ID": "invalid_2", "VAL": {"nested": True}}).encode(),
    ]
    keys = [json.dumps({"number": str(i)}).encode() for i in range(len(records))]
    driver.sendBytesData(table_compat.name, records, keys)
    driver.sendBytesData(table_ht.name, records, keys)

    # Two connectors running simultaneously — allow extra time.
    time.sleep(2 * STABILIZATION_SLEEP)

    assert not driver.get_failed_tasks(connector_compat.name), (
        "Compat connector task failed"
    )
    assert not driver.get_failed_tasks(connector_ht.name), "HT connector task failed"

    # Both tables should have the valid record.
    assert driver.select_number_of_records(table_compat.name) >= 1, (
        "Expected at least 1 row in compat table"
    )
    assert driver.select_number_of_records(table_ht.name) >= 1, (
        "Expected at least 1 row in HT table"
    )

    # v4-compat: invalid records land in DLQ, not error table.
    dlq_count = driver.consume_messages_dlq({"config": connector_compat.config}, 0, 1)
    assert dlq_count >= 2, (
        f"Expected at least 2 records in DLQ (v4-compat), got {dlq_count}"
    )

    # v4-ht: invalid records land in error table, not DLQ.
    cursor = driver.snowflake_conn.cursor()
    cursor.execute(f"SELECT * FROM ERROR_TABLE({quote_name(table_ht.name)})")
    error_rows = cursor.fetchall()
    logger.info("v4-ht error table rows: %d", len(error_rows))
    assert len(error_rows) >= 2, (
        f"Expected at least 2 error table rows (v4-ht), got {len(error_rows)}"
    )
