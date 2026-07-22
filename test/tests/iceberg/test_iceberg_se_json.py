"""Iceberg schema evolution E2E tests — JSON format.

Tests client-side SE (RowValidator-driven ``ALTER ICEBERG TABLE ADD COLUMN``)
and server-side SE (``ENABLE_SCHEMA_EVOLUTION`` on the table, validation=false).

v4-only: v3 iceberg support was removed in v4.
"""

import json
import logging

import pytest

from lib.driver import KafkaDriver
from tests.iceberg import json_connector_config

logger = logging.getLogger(__name__)


@pytest.mark.iceberg
@pytest.mark.schema_evolution
@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
def test_iceberg_se_add_column(
    driver: KafkaDriver,
    create_iceberg_table,
    create_topics,
    create_connector,
    wait_for_rows,
):
    """Iceberg schema evolution — connector adds a new column mid-stream (client-side SE).

    Table starts with RECORD_METADATA VARIANT + CITY TEXT.  Wave 1 records carry
    ``{city, age}``: the connector's RowValidator detects AGE as new and issues
    ``ALTER ICEBERG TABLE ADD COLUMN``.  Wave 2 adds ``country``: SE adds COUNTRY.

    Uses ``validation=true`` (compat/client-side SE) so the RowValidator drives
    column additions.  Server-side SE (validation=false) does not support typed
    column additions on iceberg tables.
    """
    base_name = "iceberg_se_addcol"
    table = create_iceberg_table(
        base_name,
        columns="(RECORD_METADATA VARIANT, CITY TEXT) ENABLE_SCHEMA_EVOLUTION = TRUE",
        cleanup_topic=False,
    )
    topic = create_topics([base_name], with_tables=False)[0]

    create_connector(
        v4_config=json_connector_config(topic, schematization=True, validation=True)
    )
    driver.startConnectorWaitTime()

    wave1_count = 100
    driver.sendBytesData(
        topic,
        [
            json.dumps({"city": "Hsinchu", "age": i}).encode("utf-8")
            for i in range(wave1_count)
        ],
        partition=0,
    )
    wait_for_rows(table.name, wave1_count)

    cols = {row[0] for row in table.schema()}
    assert "AGE" in cols, (
        f"Expected connector SE to add AGE column after wave 1, got: {cols}"
    )

    wave2_count = 50
    driver.sendBytesData(
        topic,
        [
            json.dumps({"city": "Taipei", "age": 100 + i, "country": "TW"}).encode(
                "utf-8"
            )
            for i in range(wave2_count)
        ],
        partition=0,
    )
    wait_for_rows(table.name, wave1_count + wave2_count)

    rows = table.select('"CITY", "COUNTRY"', "WHERE \"CITY\" = 'Taipei' LIMIT 1")
    assert rows, "Expected at least one wave-2 row with CITY = 'Taipei'"
    assert rows[0]["CITY"] == "Taipei"
    assert rows[0]["COUNTRY"] == "TW", (
        f"Expected COUNTRY='TW', got {rows[0]['COUNTRY']!r}"
    )

    null_country_count = table.select("COUNT(*) AS CNT", 'WHERE "COUNTRY" IS NULL')[0][
        "CNT"
    ]
    assert null_country_count == wave1_count, (
        f"Expected {wave1_count} rows with NULL COUNTRY, got {null_country_count}"
    )


@pytest.mark.iceberg
@pytest.mark.schema_evolution
@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
def test_iceberg_se_multi_wave(
    driver: KafkaDriver,
    create_iceberg_table,
    create_topics,
    create_connector,
    wait_for_rows,
):
    """Iceberg SE — connector adds two successive new columns across three waves.

    Waves:
      1. Wave 1 (50 records): ``{city}`` — no SE needed.
      2. Wave 2 (50 records): ``{city, age}`` — connector SE adds AGE.
      3. Wave 3 (50 records): ``{city, age, country}`` — connector SE adds COUNTRY.

    After all waves:
      - Wave-1 rows: AGE IS NULL, COUNTRY IS NULL
      - Wave-2 rows: AGE set, COUNTRY IS NULL
      - Wave-3 rows: AGE set, COUNTRY set
    """
    base_name = "iceberg_se_multi"
    table = create_iceberg_table(
        base_name,
        columns="(RECORD_METADATA VARIANT, CITY TEXT) ENABLE_SCHEMA_EVOLUTION = TRUE",
        cleanup_topic=False,
    )
    topic = create_topics([base_name], with_tables=False)[0]

    create_connector(
        v4_config=json_connector_config(topic, schematization=True, validation=True)
    )
    driver.startConnectorWaitTime()

    wave1_count = 50
    driver.sendBytesData(
        topic,
        [json.dumps({"city": "Taipei"}).encode("utf-8") for _ in range(wave1_count)],
        partition=0,
    )
    wait_for_rows(table.name, wave1_count)

    wave2_count = 50
    driver.sendBytesData(
        topic,
        [
            json.dumps({"city": "Hsinchu", "age": i}).encode("utf-8")
            for i in range(wave2_count)
        ],
        partition=0,
    )
    wait_for_rows(table.name, wave1_count + wave2_count)

    wave3_count = 50
    driver.sendBytesData(
        topic,
        [
            json.dumps({"city": "Kaohsiung", "age": 200 + i, "country": "TW"}).encode(
                "utf-8"
            )
            for i in range(wave3_count)
        ],
        partition=0,
    )
    wait_for_rows(table.name, wave1_count + wave2_count + wave3_count)

    w1_null = table.select(
        "COUNT(*) AS CNT",
        'WHERE "CITY" = \'Taipei\' AND "AGE" IS NULL AND "COUNTRY" IS NULL',
    )[0]["CNT"]
    assert w1_null == wave1_count, (
        f"Expected {wave1_count} wave-1 rows with NULL AGE+COUNTRY, got {w1_null}"
    )

    w2_rows = table.select('"AGE", "COUNTRY"', "WHERE \"CITY\" = 'Hsinchu' LIMIT 1")
    assert w2_rows, "Expected at least one wave-2 row"
    assert w2_rows[0]["AGE"] is not None, "Expected AGE set for wave-2 rows"
    assert w2_rows[0]["COUNTRY"] is None, (
        f"Expected COUNTRY NULL for wave-2 rows, got {w2_rows[0]['COUNTRY']!r}"
    )

    w3_rows = table.select('"AGE", "COUNTRY"', "WHERE \"CITY\" = 'Kaohsiung' LIMIT 1")
    assert w3_rows, "Expected at least one wave-3 row"
    assert w3_rows[0]["AGE"] is not None, "Expected AGE set for wave-3 rows"
    assert w3_rows[0]["COUNTRY"] == "TW", (
        f"Expected COUNTRY='TW', got {w3_rows[0]['COUNTRY']!r}"
    )


@pytest.mark.iceberg
@pytest.mark.schema_evolution
@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
def test_iceberg_se_json_server_side(
    driver: KafkaDriver,
    name_salt: str,
    create_iceberg_table,
    create_topics,
    create_connector,
    wait_for_rows,
):
    """JSON schema evolution into an iceberg table (server-side SE, HT mode).

    Uses ``validation=false`` (HT mode) so client-side validation is never
    initialized.  Records flow directly to SSv2, which relies on
    ``ENABLE_SCHEMA_EVOLUTION = TRUE`` for server-side column additions.

    Sends two waves:
      1. Wave 1 (100 records): ``{city, age}`` — server-side SE adds CITY, AGE.
      2. Wave 2 (50 records): ``{city, age, country}`` — server-side SE adds COUNTRY.
    """
    base_name = "iceberg_se_json"
    table = create_iceberg_table(
        base_name,
        columns="(RECORD_METADATA VARIANT) ENABLE_SCHEMA_EVOLUTION = TRUE",
        cleanup_topic=False,
    )
    topic = create_topics([base_name], with_tables=False)[0]

    create_connector(
        v4_config={
            **json_connector_config(topic, schematization=True, validation=False),
            "errors.tolerance": "all",
            "errors.log.enable": "true",
            "errors.deadletterqueue.topic.name": f"DLQ_iceberg_se{name_salt}",
            "errors.deadletterqueue.topic.replication.factor": "1",
        }
    )
    driver.startConnectorWaitTime()

    wave1_count = 100
    driver.sendBytesData(
        topic,
        [
            json.dumps({"city": "Hsinchu", "age": i}).encode("utf-8")
            for i in range(wave1_count)
        ],
        partition=0,
    )
    wait_for_rows(table.name, wave1_count)

    wave2_count = 50
    driver.sendBytesData(
        topic,
        [
            json.dumps({"city": "Taipei", "age": 100 + i, "country": "TW"}).encode(
                "utf-8"
            )
            for i in range(wave2_count)
        ],
        partition=0,
    )
    wait_for_rows(table.name, wave1_count + wave2_count)

    cols = {row[0]: row[1] for row in table.schema()}
    assert "CITY" in cols, f"Expected CITY column, got: {list(cols.keys())}"
    assert "AGE" in cols, f"Expected AGE column, got: {list(cols.keys())}"
    assert "COUNTRY" in cols, (
        f"Expected COUNTRY column after wave 2, got: {list(cols.keys())}"
    )

    rows = table.select('"CITY", "AGE", "COUNTRY"', "WHERE \"CITY\" = 'Taipei' LIMIT 1")
    assert rows, "Expected at least one wave-2 row with CITY = 'Taipei'"
    assert rows[0]["CITY"] == "Taipei"
    assert rows[0]["COUNTRY"] == "TW"

    null_country_count = table.select("COUNT(*) AS CNT", 'WHERE "COUNTRY" IS NULL')[0][
        "CNT"
    ]
    assert null_country_count == wave1_count, (
        f"Expected {wave1_count} rows with NULL COUNTRY, got {null_country_count}"
    )
