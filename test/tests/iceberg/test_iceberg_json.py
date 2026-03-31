"""E2E tests for Kafka Connector v4 iceberg JSON ingestion.

These tests are v4-only. V3 is excluded because:
  - v3 requires ``snowflake.streaming.iceberg.enabled=true`` in the connector config
    which the config migration does not add (v3 iceberg was experimental)
  - v3 had custom iceberg code (IcebergInitService, IcebergTableStreamingRecordMapper)
    that has been removed in v4
  - v4 uses SSv2 which handles iceberg tables transparently

Prerequisites:
  - An AWS external volume named ``ICEBERG_EXTERNAL_VOLUME`` must exist in the test
    Snowflake account.  The default is ``kafka_push_e2e_volume_aws``.  Override
    with the environment variable ``ICEBERG_EXTERNAL_VOLUME``.
"""

import json
import logging

import pytest

from lib.driver import KafkaDriver
from tests.iceberg import json_connector_config

logger = logging.getLogger(__name__)

_SAMPLE_MESSAGE = {
    "id": 1,
    "body_temperature": 36.6,
    "name": "Steve",
    "approved_coffee_types": ["Espresso", "Doppio", "Ristretto", "Lungo"],
    "animals_possessed": {"dogs": True, "cats": False},
}
RECORD_COUNT = 100


@pytest.mark.iceberg
@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
@pytest.mark.parametrize(
    "schematization", [True, False], ids=["schema=on", "schema=off"]
)
@pytest.mark.parametrize("validation", [True, False], ids=["compat", "ht"])
def test_iceberg_json_ingestion(
    driver: KafkaDriver,
    create_iceberg_table,
    create_topics,
    create_connector,
    wait_for_rows,
    validation: bool,
    schematization: bool,
):
    """JSON ingestion into an iceberg table — full 2x2 matrix (validation x schematization).

    Matrix axes:
      - validation (compat=true / ht=false): controls whether the client-side
        RowValidator runs.
      - schematization (on/off): controls how the connector maps records to columns.

    ``schema=off`` (bag-of-bits): table has ``RECORD_METADATA VARIANT, RECORD_CONTENT
      VARIANT``.  Full JSON payload goes into RECORD_CONTENT.  Assertions use
      ``PARSE_JSON(RECORD_CONTENT):field::TYPE`` because iceberg stores VARIANT as a
      string-encoded JSON literal.

    ``schema=on`` (typed columns): table pre-declares all columns from the sample
      message — scalar fields as typed (ID NUMBER, BODY_TEMPERATURE FLOAT, NAME STRING)
      and complex fields as VARIANT (APPROVED_COFFEE_TYPES, ANIMALS_POSSESSED).
      RECORD_METADATA remains VARIANT.  No schema evolution is needed.
      Typed columns are accessed directly; VARIANT columns still need PARSE_JSON().
    """
    val_tag = "compat" if validation else "ht"
    sch_tag = "s1" if schematization else "s0"
    base_name = f"iceberg_jv_{val_tag}_{sch_tag}"

    if schematization:
        columns = (
            "(RECORD_METADATA VARIANT, "
            "ID BIGINT, "
            "BODY_TEMPERATURE DOUBLE, "
            "NAME TEXT, "
            "APPROVED_COFFEE_TYPES VARIANT, "
            "ANIMALS_POSSESSED VARIANT)"
        )
    else:
        columns = "(RECORD_METADATA VARIANT, RECORD_CONTENT VARIANT)"

    table = create_iceberg_table(base_name, columns=columns, cleanup_topic=False)
    topic = create_topics([base_name], with_tables=False)[0]

    create_connector(
        v4_config=json_connector_config(
            topic, schematization=schematization, validation=validation
        )
    )
    driver.startConnectorWaitTime()

    records = [json.dumps(_SAMPLE_MESSAGE).encode("utf-8") for _ in range(RECORD_COUNT)]
    driver.sendBytesData(topic, records, partition=0)

    wait_for_rows(table.name, RECORD_COUNT)

    if not schematization:
        rows = table.select(
            "PARSE_JSON(RECORD_CONTENT):id::NUMBER            AS ID, "
            "PARSE_JSON(RECORD_CONTENT):body_temperature::FLOAT AS BODY_TEMPERATURE, "
            "PARSE_JSON(RECORD_CONTENT):name::STRING           AS NAME, "
            "PARSE_JSON(RECORD_METADATA):offset::NUMBER        AS OFFSET, "
            "PARSE_JSON(RECORD_METADATA):partition::NUMBER     AS PARTITION, "
            "PARSE_JSON(RECORD_METADATA):topic::STRING         AS TOPIC, "
            "PARSE_JSON(RECORD_METADATA):SnowflakeConnectorPushTime::STRING AS PUSH_TIME",
            "ORDER BY PARSE_JSON(RECORD_METADATA):offset::NUMBER LIMIT 1",
        )
        assert rows, "Expected at least one row in the iceberg table"
        row = rows[0]
        assert row["ID"] == 1, f"Expected id=1, got {row['ID']}"
        assert abs(float(row["BODY_TEMPERATURE"]) - 36.6) < 0.01, (
            f"Expected body_temperature≈36.6, got {row['BODY_TEMPERATURE']}"
        )
        assert row["NAME"] == "Steve", f"Expected name='Steve', got {row['NAME']}"
        assert row["OFFSET"] == 0, f"Expected offset=0, got {row['OFFSET']}"
        assert row["PARTITION"] == 0, f"Expected partition=0, got {row['PARTITION']}"
        assert row["TOPIC"] == topic, f"Expected topic={topic!r}, got {row['TOPIC']!r}"
        assert row["PUSH_TIME"] is not None, (
            "Expected SnowflakeConnectorPushTime to be set"
        )
    else:
        rows = table.select(
            '"ID", "BODY_TEMPERATURE", "NAME", '
            "PARSE_JSON(RECORD_METADATA):offset::NUMBER        AS OFFSET, "
            "PARSE_JSON(RECORD_METADATA):partition::NUMBER     AS PARTITION, "
            "PARSE_JSON(RECORD_METADATA):topic::STRING         AS TOPIC, "
            "PARSE_JSON(RECORD_METADATA):SnowflakeConnectorPushTime::STRING AS PUSH_TIME",
            "ORDER BY PARSE_JSON(RECORD_METADATA):offset::NUMBER LIMIT 1",
        )
        assert rows, "Expected at least one row"
        row = rows[0]
        assert row["ID"] == 1, f"Expected id=1, got {row['ID']}"
        assert abs(float(row["BODY_TEMPERATURE"]) - 36.6) < 0.01, (
            f"Expected body_temperature≈36.6, got {row['BODY_TEMPERATURE']}"
        )
        assert row["NAME"] == "Steve", f"Expected name='Steve', got {row['NAME']}"
        assert row["OFFSET"] == 0, f"Expected offset=0, got {row['OFFSET']}"
        assert row["PARTITION"] == 0, f"Expected partition=0, got {row['PARTITION']}"
        assert row["TOPIC"] == topic, f"Expected topic={topic!r}, got {row['TOPIC']!r}"
        assert row["PUSH_TIME"] is not None, (
            "Expected SnowflakeConnectorPushTime to be set"
        )
