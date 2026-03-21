"""E2E tests for column identifier normalization."""

import json

import pytest
from snowflake.connector import DictCursor

FILE_NAME = "column_identifier_normalization"
CONFIG_FILE = f"{FILE_NAME}.json"
TWO_CITY_DDL = 'ID NUMBER, "city" VARCHAR, CITY VARCHAR, RECORD_METADATA VARIANT'
NORM_MATRIX = [True, False]
NORM_IDS = ["norm=on", "norm=off"]


def _make_overrides(normalization, validation, dlq_topic, topic_override):
    return {
        "snowflake.enable.schematization": "true",
        "snowflake.enable.column.identifier.normalization": str(normalization).lower(),
        "snowflake.client.validation.enabled": str(validation).lower(),
        "errors.deadletterqueue.topic.name": dlq_topic,
        "topics": topic_override,
    }


def _dlq_topic(name_salt, tag):
    return f"DLQ_NORM_{FILE_NAME}_{name_salt}_{tag}"


def _send_rows(driver, topic, rows):
    records = [json.dumps(r).encode("utf-8") for r in rows]
    driver.sendBytesData(topic, records, partition=0)


def _query_by_id(driver, topic, row_id):
    return (
        driver.snowflake_conn.cursor(DictCursor)
        .execute(f'SELECT * FROM {topic} WHERE "ID" = {row_id}')
        .fetchone()
    )


def _describe_columns(driver, topic):
    return {
        row[0]: row[1]
        for row in driver.snowflake_conn.cursor()
        .execute(f"DESCRIBE TABLE {topic}")
        .fetchall()
    }


def _city_and_age_keys(normalization):
    """Return (city_key, age_key) based on normalization flag.

    norm=ON:  '"city"' (quoted SQL identifier) normalizes to raw 'city'.
              '"age"' normalizes to raw 'age'.
    norm=OFF: 'city' and 'age' sent as-is.
    """
    if normalization:
        return '"city"', '"age"'
    return "city", "age"


def _setup(
    driver,
    name_salt,
    tag,
    normalization,
    validation,
    create_connector_from_file,
    create_table,
):
    base_name = f"{FILE_NAME}_{tag}"
    table = create_table(
        base_name,
        columns=f"({TWO_CITY_DDL}) ENABLE_SCHEMA_EVOLUTION = TRUE",
    )
    topic = table.name
    driver.createTopics(topic, partitionNum=1, replicationNum=1)
    dlq = _dlq_topic(name_salt, tag)
    overrides = _make_overrides(normalization, validation, dlq, topic)
    config = create_connector_from_file(CONFIG_FILE, config_overrides=overrides)
    driver.startConnectorWaitTime()
    return topic, config["name"]


@pytest.mark.parametrize("normalization", NORM_MATRIX, ids=NORM_IDS)
def test_with_validation(
    driver,
    name_salt,
    connector_version,
    create_connector_from_file,
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
    topic, connector_name = _setup(
        driver,
        name_salt,
        tag,
        normalization,
        True,
        create_connector_from_file,
        create_table,
    )

    city_key, age_key = _city_and_age_keys(normalization)
    _send_rows(
        driver,
        topic,
        [
            {"ID": 0, city_key: "lower_0", "CITY": "upper_0"},
            {"ID": 1, city_key: "v1"},
            {"ID": 2, "CITY": "v2"},
            {"ID": 3, age_key: 1, "AGE": 2},
        ],
    )
    wait_for_rows(topic, 4, connector_name=connector_name)

    row0 = _query_by_id(driver, topic, 0)
    assert row0["city"] == "lower_0"
    assert row0["CITY"] == "upper_0"

    row1 = _query_by_id(driver, topic, 1)
    assert row1["city"] == "v1"

    row2 = _query_by_id(driver, topic, 2)
    assert row2["CITY"] == "v2"

    row3 = _query_by_id(driver, topic, 3)
    assert row3["city"] is None
    assert row3["CITY"] is None
    assert row3["age"] == 1
    assert row3["AGE"] == 2

    cols = _describe_columns(driver, topic)
    assert "age" in cols
    assert "AGE" in cols


@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
@pytest.mark.parametrize("normalization", NORM_MATRIX, ids=NORM_IDS)
def test_without_validation(
    driver,
    name_salt,
    connector_version,
    create_connector_from_file,
    create_table,
    wait_for_rows,
    normalization,
):
    """val=OFF, schema_evo=ON, KCv4 only.
    Server-side MBCN CI fallback writes to ALL case-insensitive-matching columns.
    Server-side schema evolution uppercases new column names.
    """

    tag = f"noval_n{'1' if normalization else '0'}"
    topic, connector_name = _setup(
        driver,
        name_salt,
        tag,
        normalization,
        False,
        create_connector_from_file,
        create_table,
    )

    city_key, age_key = _city_and_age_keys(normalization)
    _send_rows(
        driver,
        topic,
        [
            {"ID": 0, city_key: "lower_0", "CITY": "upper_0"},
            {"ID": 1, city_key: "v1"},
            {"ID": 2, "CITY": "v2"},
            {"ID": 3, age_key: 1},
        ],
    )
    wait_for_rows(topic, 4, connector_name=connector_name)

    row0 = _query_by_id(driver, topic, 0)
    assert row0["city"] == "lower_0"
    assert row0["CITY"] == "upper_0"

    # MBCN CI fallback: single key writes to both CI-matching columns
    row1 = _query_by_id(driver, topic, 1)
    assert row1["city"] == "v1"
    assert row1["CITY"] == "v1"

    row2 = _query_by_id(driver, topic, 2)
    assert row2["city"] == "v2"
    assert row2["CITY"] == "v2"

    # Server-side schema evo uppercases new column names
    row3 = _query_by_id(driver, topic, 3)
    assert row3["city"] is None
    assert row3["CITY"] is None
    assert row3["AGE"] == 1

    cols = _describe_columns(driver, topic)
    assert "AGE" in cols
