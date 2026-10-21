"""Interactive table auto-creation E2E tests (snowflake.autocreate.table.type=interactive).

Assumes the test account has interactive table streaming enabled, as the existing
interactive Java IT does. v4-only.
"""

import json

import pytest
from snowflake.connector import DictCursor

from lib.driver import KafkaDriver
from lib.fixtures.table import Table
from tests.interactive import interactive_json_config

pytestmark = pytest.mark.interactive


def _show_table(driver: KafkaDriver, name: str) -> dict:
    rows = (
        driver.snowflake_conn.cursor(DictCursor)
        .execute(f"SHOW TABLES LIKE '{name}'")
        .fetchall()
    )
    assert rows, f"Expected SHOW TABLES to return a row for {name}"
    return rows[0]


@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
@pytest.mark.parametrize(
    "create_options, expected_cluster_by",
    [
        (None, "record_metadata:offset::number"),
        ("CLUSTER BY (RECORD_METADATA:partition::number)", "partition"),
    ],
    ids=["default-cluster-by", "user-cluster-by"],
)
def test_interactive_autocreate_json(
    driver: KafkaDriver,
    create_topics,
    create_connector,
    wait_for_rows,
    create_options,
    expected_cluster_by,
):
    """table.type=interactive with NO pre-created table -> connector auto-creates it.

    Asserts the table is interactive, keeps ERROR_LOGGING on, clusters on the default or
    user key, and that server-side schema evolution added the payload columns.
    """
    topic = create_topics(["interactive_autocreate"], with_tables=False)[0]
    table = Table(driver, topic)
    try:
        create_connector(v4_config=interactive_json_config(topic, create_options))
        driver.startConnectorWaitTime()

        count = 50
        driver.sendBytesData(
            topic,
            [
                json.dumps({"id": i, "city": "Tel Aviv"}).encode("utf-8")
                for i in range(count)
            ],
            partition=0,
        )
        wait_for_rows(topic, count)

        row = _show_table(driver, topic)
        assert row["is_interactive"] == "Y", f"{topic} is not interactive: {row}"
        assert row["error_logging"] == "ON"
        assert expected_cluster_by in row["cluster_by"].lower()
        cols = {r[0] for r in table.schema()}
        assert {"ID", "CITY", "RECORD_METADATA"} <= cols, f"got columns {cols}"
        assert table.select_scalar("COUNT(*)") == count
    finally:
        table.drop()


@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
def test_interactive_options_require_interactive_type(driver, create_topics, create_connector):
    """Interactive create options with table.type=snowflake must fail the task at config time."""
    import time

    topic = create_topics(["interactive_bad_cfg"], with_tables=False)[0]
    cfg = interactive_json_config(topic, "CLUSTER BY (c)")
    cfg["snowflake.autocreate.table.type"] = "snowflake"
    connector = create_connector(v4_config=cfg)
    driver.startConnectorWaitTime()

    deadline = time.monotonic() + 120
    failed = []
    while time.monotonic() < deadline and not failed:
        failed = driver.get_failed_tasks(connector.name)
        time.sleep(3)
    assert failed, "Expected a FAILED task for interactive options with table.type=snowflake"
    assert "snowflake.interactive.create.table.options" in failed[0].get("trace", "")
