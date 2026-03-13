import json

import pytest
from snowflake.connector import DictCursor

FILE_NAME = "snowpipe_streaming_schema_evolution"
CONFIG_FILE = f"{FILE_NAME}.json"


def _assert_success_rows(driver, topic, schematization, record_count):
    """Shared assertions for successful schema evolution tests."""
    cols = {
        row[0]: row[1]
        for row in driver.snowflake_conn.cursor()
        .execute(f"DESCRIBE TABLE {topic}")
        .fetchall()
    }

    if schematization:
        assert "CITY" in cols, f"Expected CITY column, got: {list(cols.keys())}"
        assert "AGE" in cols, f"Expected AGE column, got: {list(cols.keys())}"

        row = (
            driver.snowflake_conn.cursor(DictCursor)
            .execute(
                f'SELECT "CITY", "AGE" FROM {topic} '
                f'WHERE RECORD_METADATA:"offset"::number = 0'
            )
            .fetchone()
        )
        assert row is not None, "Expected row with offset 0"
        assert row["CITY"] == "Hsinchu"
        assert row["AGE"] == 0
    else:
        assert "RECORD_CONTENT" in cols, (
            f"Expected RECORD_CONTENT column, got: {list(cols.keys())}"
        )

        row = (
            driver.snowflake_conn.cursor(DictCursor)
            .execute(
                f"SELECT RECORD_CONTENT FROM {topic} "
                f'WHERE RECORD_METADATA:"offset"::number = 0'
            )
            .fetchone()
        )
        assert row is not None, "Expected row with offset 0"
        content = json.loads(row["RECORD_CONTENT"])
        assert content["city"] == "Hsinchu"
        assert content["age"] == 0

    count = driver.select_number_of_records(topic)
    assert count == record_count, f"Expected {record_count} rows, got {count}"


def _assert_dlq(driver, config, topic, record_count):
    """Shared assertions for DLQ tests."""
    offsets_in_dlq = driver.consume_messages_dlq(config, 0, record_count - 1)
    assert offsets_in_dlq == record_count, (
        f"Expected {record_count} records in DLQ, got {offsets_in_dlq}"
    )

    count = driver.select_number_of_records(topic)
    assert count == 0, f"Expected 0 rows in table (DLQ), got {count}"


def test_schema_evolution_add_columns(
    driver, name_salt, create_connector, snowflake_table, wait_for_rows
):
    """ENABLE_SCHEMA_EVOLUTION=TRUE, schematization=on, send records with extra fields.

    Runs for both v3 and v4. Flat columns CITY, AGE are added via schema evolution.
    """
    topic = snowflake_table(
        FILE_NAME,
        f"CREATE OR REPLACE TABLE {FILE_NAME}{name_salt} "
        f"(RECORD_METADATA VARIANT) ENABLE_SCHEMA_EVOLUTION = TRUE",
    )

    driver.createTopics(topic, partitionNum=1, replicationNum=1)

    create_connector(CONFIG_FILE)
    driver.startConnectorWaitTime()

    record_count = 100
    values = [
        json.dumps({"city": "Hsinchu", "age": i}).encode("utf-8")
        for i in range(record_count)
    ]
    driver.sendBytesData(topic, values, [], partition=0)

    wait_for_rows(topic, record_count)

    _assert_success_rows(driver, topic, schematization=True, record_count=record_count)


def test_schema_evolution_multi_wave(
    driver, name_salt, create_connector, snowflake_table, wait_for_rows
):
    """Send two waves of records with different schemas.

    Wave 1: {city, age}           -> ADD COLUMN for CITY, AGE
    Wave 2: {city, age, country}  -> ADD COLUMN for COUNTRY
    Verifies that wave-1 rows have NULL for COUNTRY.
    """
    topic = snowflake_table(
        FILE_NAME,
        f"CREATE OR REPLACE TABLE {FILE_NAME}{name_salt} "
        f"(RECORD_METADATA VARIANT) ENABLE_SCHEMA_EVOLUTION = TRUE",
    )

    driver.createTopics(topic, partitionNum=1, replicationNum=1)

    create_connector(CONFIG_FILE)
    driver.startConnectorWaitTime()

    wave1_count = 50
    wave1 = [
        json.dumps({"city": "Hsinchu", "age": i}).encode("utf-8")
        for i in range(wave1_count)
    ]
    driver.sendBytesData(topic, wave1, [], partition=0)

    wait_for_rows(topic, wave1_count)

    wave2_count = 50
    wave2 = [
        json.dumps(
            {
                "city": "Taipei",
                "age": 100 + i,
                "country": "TW",
            }
        ).encode("utf-8")
        for i in range(wave2_count)
    ]
    driver.sendBytesData(topic, wave2, [], partition=0)

    total_expected = wave1_count + wave2_count
    wait_for_rows(topic, total_expected)

    cols = {
        row[0]: row[1]
        for row in driver.snowflake_conn.cursor()
        .execute(f"DESCRIBE TABLE {topic}")
        .fetchall()
    }
    assert "CITY" in cols
    assert "AGE" in cols
    assert "COUNTRY" in cols, (
        f"Expected COUNTRY column after wave 2, got: {list(cols.keys())}"
    )

    row = (
        driver.snowflake_conn.cursor(DictCursor)
        .execute(
            f'SELECT "CITY", "AGE", "COUNTRY" FROM {topic} '
            f'WHERE RECORD_METADATA:"offset"::number = {wave1_count}'
        )
        .fetchone()
    )
    assert row is not None, f"Expected row at offset {wave1_count}"
    assert row["CITY"] == "Taipei"
    assert row["COUNTRY"] == "TW"

    null_country_count = (
        driver.snowflake_conn.cursor()
        .execute(f"SELECT count(*) FROM {topic} WHERE COUNTRY IS NULL")
        .fetchone()[0]
    )
    assert null_country_count == wave1_count, (
        f"Expected {wave1_count} rows with NULL country, got {null_country_count}"
    )


def test_schema_evolution_happy_path(
    driver, name_salt, create_connector, snowflake_table, wait_for_rows
):
    """Send records that match the existing table schema exactly.

    Validation passes without triggering schema evolution. Verifies that
    client-side validation does not interfere with normal ingestion.
    """
    topic = snowflake_table(
        FILE_NAME,
        f"CREATE OR REPLACE TABLE {FILE_NAME}{name_salt} "
        f"(RECORD_METADATA VARIANT, CITY VARCHAR, AGE NUMBER) "
        f"ENABLE_SCHEMA_EVOLUTION = TRUE",
    )

    driver.createTopics(topic, partitionNum=1, replicationNum=1)

    create_connector(CONFIG_FILE)
    driver.startConnectorWaitTime()

    record_count = 100
    values = [
        json.dumps({"city": "Hsinchu", "age": i}).encode("utf-8")
        for i in range(record_count)
    ]
    driver.sendBytesData(topic, values, [], partition=0)

    wait_for_rows(topic, record_count)

    row = (
        driver.snowflake_conn.cursor(DictCursor)
        .execute(
            f'SELECT "CITY", "AGE" FROM {topic} '
            f'WHERE RECORD_METADATA:"offset"::number = 0'
        )
        .fetchone()
    )
    assert row is not None, "Expected row with offset 0"
    assert row["CITY"] == "Hsinchu"
    assert row["AGE"] == 0


def test_schema_evolution_drop_not_null(
    driver, name_salt, create_connector, snowflake_table, wait_for_rows
):
    """Table has a NOT NULL column, but records omit it.

    Schema evolution should drop the NOT NULL constraint and add the extra
    column, allowing records to be ingested with NULL for the original column.
    """
    topic = snowflake_table(
        FILE_NAME,
        f"CREATE OR REPLACE TABLE {FILE_NAME}{name_salt} "
        f"(RECORD_METADATA VARIANT, STATUS VARCHAR NOT NULL) "
        f"ENABLE_SCHEMA_EVOLUTION = TRUE",
    )

    driver.createTopics(topic, partitionNum=1, replicationNum=1)

    create_connector(CONFIG_FILE)
    driver.startConnectorWaitTime()

    record_count = 50
    values = [
        json.dumps({"city": "Hsinchu", "age": i}).encode("utf-8")
        for i in range(record_count)
    ]
    driver.sendBytesData(topic, values, [], partition=0)

    wait_for_rows(topic, record_count)

    cols = {
        row[0]: row[1]
        for row in driver.snowflake_conn.cursor()
        .execute(f"DESCRIBE TABLE {topic}")
        .fetchall()
    }
    assert "CITY" in cols, f"Expected CITY column, got: {list(cols.keys())}"
    assert "AGE" in cols, f"Expected AGE column, got: {list(cols.keys())}"
    assert "STATUS" in cols

    null_status_count = (
        driver.snowflake_conn.cursor()
        .execute(f"SELECT count(*) FROM {topic} WHERE STATUS IS NULL")
        .fetchone()[0]
    )
    assert null_status_count == record_count, (
        f"Expected {record_count} rows with NULL STATUS, got {null_status_count}"
    )


def test_schema_evolution_disabled_to_dlq(
    driver, name_salt, create_connector, snowflake_table
):
    """ENABLE_SCHEMA_EVOLUTION not set, schematization=on: extra columns go to DLQ.

    Runs for both v3 and v4. With schematization=on, extra flat columns are
    detected but cannot be evolved, so records are routed to DLQ.
    """
    topic = snowflake_table(
        FILE_NAME,
        f"CREATE OR REPLACE TABLE {FILE_NAME}{name_salt} "
        f"(RECORD_METADATA VARIANT) ENABLE_SCHEMA_EVOLUTION = FALSE",
    )

    driver.createTopics(topic, partitionNum=1, replicationNum=1)

    config = create_connector(CONFIG_FILE)
    driver.startConnectorWaitTime()

    record_count = 5
    values = [
        json.dumps({"city": "Hsinchu", "age": i}).encode("utf-8")
        for i in range(record_count)
    ]
    driver.sendBytesData(topic, values, [], partition=0)

    _assert_dlq(driver, config, topic, record_count)


# v4-only: full config matrix (schema_evo x schematization x validation)
# When schematization=off, client validation is implicitly disabled
@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
@pytest.mark.parametrize(
    "schema_evo, schematization, validation",
    [
        (True, True, True),
        (True, True, False),
        (True, False, None),
        (False, True, True),
        (False, True, False),
        (False, False, None),
    ],
    ids=[
        "evo=on,schema=on,valid=on",
        "evo=on,schema=on,valid=off",
        "evo=on,schema=off",
        "evo=off,schema=on,valid=on",
        "evo=off,schema=on,valid=off",
        "evo=off,schema=off",
    ],
)
def test_schema_evolution_v4_config_variants(
    driver,
    name_salt,
    create_connector,
    snowflake_table,
    wait_for_rows,
    schema_evo,
    schematization,
    validation,
):
    """V4-only test covering combinations of ENABLE_SCHEMA_EVOLUTION,
    schematization, and client.validation.enabled.

    When schema_evo=True, extra columns are added and records are ingested.
    When schema_evo=False, records with extra columns are routed to DLQ
    (schematization=on) or handled by the server (schematization=off).

    validation=None means schematization=off so client validation is implicitly
    off and we must set it to false to satisfy the config validator.
    """
    evo_clause = "TRUE" if schema_evo else "FALSE"
    ddl = (
        f"CREATE OR REPLACE TABLE {FILE_NAME}{name_salt} "
        f"(RECORD_METADATA VARIANT) ENABLE_SCHEMA_EVOLUTION = {evo_clause}"
    )

    topic = snowflake_table(FILE_NAME, ddl)

    driver.createTopics(topic, partitionNum=1, replicationNum=1)

    overrides = {
        "snowflake.enable.schematization": str(schematization).lower(),
        "errors.deadletterqueue.topic.name": f"DLQ_MATRIX_{FILE_NAME}_{name_salt}",
    }
    if validation is not None:
        overrides["snowflake.client.validation.enabled"] = str(validation).lower()

    config = create_connector(CONFIG_FILE, config_overrides=overrides)
    driver.startConnectorWaitTime()

    if schema_evo:
        record_count = 100
        values = [
            json.dumps({"city": "Hsinchu", "age": i}).encode("utf-8")
            for i in range(record_count)
        ]
        driver.sendBytesData(topic, values, [], partition=0)

        wait_for_rows(topic, record_count)

        _assert_success_rows(driver, topic, schematization, record_count)
    else:
        if not schematization or validation is None or not validation:
            # No client-side validation -> server handles the error via Error Table.
            # DLQ routing only works when client validation is on.
            return

        record_count = 5
        values = [
            json.dumps({"city": "Hsinchu", "age": i}).encode("utf-8")
            for i in range(record_count)
        ]
        driver.sendBytesData(topic, values, [], partition=0)

        _assert_dlq(driver, config, topic, record_count)
