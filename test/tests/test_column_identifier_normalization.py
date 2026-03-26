"""E2E tests for column identifier normalization."""

import json

import pytest

from lib.config_migration import V4_CONFIG_TEMPLATE

TWO_CITY_DDL = '(ID NUMBER, "city" VARCHAR, CITY VARCHAR, RECORD_METADATA VARIANT) ENABLE_SCHEMA_EVOLUTION = TRUE'
NORM_MATRIX = [True, False]
NORM_IDS = ["norm=on", "norm=off"]


@pytest.mark.parametrize("normalization", NORM_MATRIX, ids=NORM_IDS)
def test_with_validation(
    driver,
    name_salt,
    connector_version,
    create_connector,
    create_table,
    wait_for_rows,
    normalization,
):
    """val=ON, schema_evo=ON. KCv3 always normalizes, so skip v3+norm=OFF.
    Row 3 triggers schema evolution to add age and AGE columns.
    """
    if connector_version == "v3" and not normalization:
        pytest.skip("KCv3 always normalizes; norm=OFF is KCv4-only")

    tag = f"val_n{'1' if normalization else '0'}"
    table = create_table(
        f"column_identifier_normalization_{tag}".upper(),
        columns=TWO_CITY_DDL,
    )
    topic = f"column_identifier_normalization_{tag}{name_salt}"
    dlq = f"DLQ_NORM_{name_salt}_{tag}"

    connector = create_connector(
        v4_config={
            **V4_CONFIG_TEMPLATE,
            "tasks.max": "1",
            "key.converter": "org.apache.kafka.connect.storage.StringConverter",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter.schemas.enable": "false",
            "snowflake.enable.schematization": "true",
            "snowflake.compatibility.enable.column.identifier.normalization": str(
                normalization
            ).lower(),
            "snowflake.validation": "client_side",
            "errors.tolerance": "all",
            "errors.log.enable": "true",
            "errors.deadletterqueue.topic.name": dlq,
            "errors.deadletterqueue.topic.replication.factor": "1",
            "topics": topic,
            "jmx": "true",
        }
    )
    driver.startConnectorWaitTime()

    city_key, age_key = ('"city"', '"age"') if normalization else ("city", "age")
    rows = [
        {"ID": 0, city_key: "lower_0", "CITY": "upper_0"},
        {"ID": 1, city_key: "lower_only"},
        {"ID": 2, "CITY": "upper_only"},
        {"ID": 3, age_key: 10, "AGE": 20},
    ]
    driver.sendBytesData(
        topic, [json.dumps(r).encode("utf-8") for r in rows], partition=0
    )
    wait_for_rows(table.name, 4, connector_name=connector.name)

    row0 = table.select("*", 'WHERE "ID" = 0')[0]
    assert row0["city"] == "lower_0"
    assert row0["CITY"] == "upper_0"

    row1 = table.select("*", 'WHERE "ID" = 1')[0]
    assert row1["city"] == "lower_only"

    row2 = table.select("*", 'WHERE "ID" = 2')[0]
    assert row2["CITY"] == "upper_only"

    row3 = table.select("*", 'WHERE "ID" = 3')[0]
    assert row3["city"] is None
    assert row3["CITY"] is None
    assert row3["age"] == 10
    assert row3["AGE"] == 20

    cols = {row[0]: row[1] for row in table.schema()}
    assert "age" in cols
    assert "AGE" in cols


@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
@pytest.mark.parametrize("normalization", NORM_MATRIX, ids=NORM_IDS)
def test_without_validation(
    driver,
    name_salt,
    connector_version,
    create_connector,
    create_table,
    wait_for_rows,
    normalization,
):
    """val=OFF, schema_evo=ON, KCv4 only.
    Server-side MBCN CI fallback writes to ALL case-insensitive-matching columns.
    Server-side schema evolution uppercases new column names.
    """
    tag = f"noval_n{'1' if normalization else '0'}"
    table = create_table(
        f"column_identifier_normalization_{tag}".upper(),
        columns=TWO_CITY_DDL,
    )
    topic = f"column_identifier_normalization_{tag}{name_salt}"
    dlq = f"DLQ_NORM_{name_salt}_{tag}"

    connector = create_connector(
        v4_config={
            **V4_CONFIG_TEMPLATE,
            "tasks.max": "1",
            "key.converter": "org.apache.kafka.connect.storage.StringConverter",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter.schemas.enable": "false",
            "snowflake.enable.schematization": "true",
            "snowflake.compatibility.enable.column.identifier.normalization": str(
                normalization
            ).lower(),
            "snowflake.validation": "server_side",
            "errors.tolerance": "all",
            "errors.log.enable": "true",
            "errors.deadletterqueue.topic.name": dlq,
            "errors.deadletterqueue.topic.replication.factor": "1",
            "topics": topic,
            "jmx": "true",
        }
    )
    driver.startConnectorWaitTime()

    city_key, age_key = ('"city"', '"age"') if normalization else ("city", "age")
    rows = [
        {"ID": 0, city_key: "lower_0", "CITY": "upper_0"},
        {"ID": 1, city_key: "lower_only"},
        {"ID": 2, "CITY": "upper_only"},
        {"ID": 3, age_key: 10},
    ]
    driver.sendBytesData(
        topic, [json.dumps(r).encode("utf-8") for r in rows], partition=0
    )
    wait_for_rows(table.name, 4, connector_name=connector.name)

    row0 = table.select("*", 'WHERE "ID" = 0')[0]
    assert row0["city"] == "lower_0"
    assert row0["CITY"] == "upper_0"

    # MBCN CI fallback: single key writes to both CI-matching columns
    row1 = table.select("*", 'WHERE "ID" = 1')[0]
    assert row1["city"] == "lower_only"
    assert row1["CITY"] == "lower_only"

    row2 = table.select("*", 'WHERE "ID" = 2')[0]
    assert row2["city"] == "upper_only"
    assert row2["CITY"] == "upper_only"

    # Server-side schema evo uppercases new column names
    row3 = table.select("*", 'WHERE "ID" = 3')[0]
    assert row3["city"] is None
    assert row3["CITY"] is None
    assert row3["AGE"] == 10

    cols = {row[0]: row[1] for row in table.schema()}
    assert "AGE" in cols
