import json

import pytest

pytestmark = pytest.mark.schema_evolution

FILE_NAME = "snowpipe_streaming_schema_evolution"
CONFIG_FILE = f"{FILE_NAME}.json"


def _assert_success_rows(table, schematization, record_count):
    """Shared assertions for successful schema evolution tests."""
    cols = {row[0]: row[1] for row in table.schema()}

    if schematization:
        assert "CITY" in cols, f"Expected CITY column, got: {list(cols.keys())}"
        assert "AGE" in cols, f"Expected AGE column, got: {list(cols.keys())}"

        rows = table.select(
            '"CITY", "AGE"',
            'WHERE RECORD_METADATA:"offset"::number = 0',
        )
        assert rows, "Expected row with offset 0"
        assert rows[0]["CITY"] == "Hsinchu"
        assert rows[0]["AGE"] == 0
    else:
        assert "RECORD_CONTENT" in cols, (
            f"Expected RECORD_CONTENT column, got: {list(cols.keys())}"
        )

        rows = table.select(
            "RECORD_CONTENT",
            'WHERE RECORD_METADATA:"offset"::number = 0',
        )
        assert rows, "Expected row with offset 0"
        content = json.loads(rows[0]["RECORD_CONTENT"])
        assert content["city"] == "Hsinchu"
        assert content["age"] == 0

    count = table.select_scalar("count(*)")
    assert count == record_count, f"Expected {record_count} rows, got {count}"


def _assert_dlq(driver, config, table, record_count):
    """Shared assertions for DLQ tests."""
    offsets_in_dlq = driver.consume_messages_dlq(config, 0, record_count - 1)
    assert offsets_in_dlq == record_count, (
        f"Expected {record_count} records in DLQ, got {offsets_in_dlq}"
    )

    count = table.select_scalar("count(*)")
    assert count == 0, f"Expected 0 rows in table (DLQ), got {count}"


def test_schema_evolution_add_columns(
    driver, create_connector_from_file, create_table, create_topics, wait_for_rows
):
    """ENABLE_SCHEMA_EVOLUTION=TRUE, schematization=on, send records with extra fields.

    Runs for both v3 and v4. Flat columns CITY, AGE are added via schema evolution.
    """
    table = create_table(
        FILE_NAME.upper(),
        columns="(RECORD_METADATA VARIANT) ENABLE_SCHEMA_EVOLUTION = TRUE",
        cleanup_topic=False,
    )
    topic = create_topics([FILE_NAME], with_tables=False)[0]

    create_connector_from_file(CONFIG_FILE)
    driver.startConnectorWaitTime()

    record_count = 100
    values = [
        json.dumps({"city": "Hsinchu", "age": i}).encode("utf-8")
        for i in range(record_count)
    ]
    driver.sendBytesData(topic, values, [], partition=0)

    wait_for_rows(table.name, record_count)

    _assert_success_rows(table, schematization=True, record_count=record_count)


def test_schema_evolution_multi_wave(
    driver, create_connector_from_file, create_table, create_topics, wait_for_rows
):
    """Send two waves of records with different schemas.

    Wave 1: {city, age}           -> ADD COLUMN for CITY, AGE
    Wave 2: {city, age, country}  -> ADD COLUMN for COUNTRY
    Verifies that wave-1 rows have NULL for COUNTRY.
    """
    table = create_table(
        FILE_NAME.upper(),
        columns="(RECORD_METADATA VARIANT) ENABLE_SCHEMA_EVOLUTION = TRUE",
        cleanup_topic=False,
    )
    topic = create_topics([FILE_NAME], with_tables=False)[0]

    create_connector_from_file(CONFIG_FILE)
    driver.startConnectorWaitTime()

    wave1_count = 50
    wave1 = [
        json.dumps({"city": "Hsinchu", "age": i}).encode("utf-8")
        for i in range(wave1_count)
    ]
    driver.sendBytesData(topic, wave1, [], partition=0)

    wait_for_rows(table.name, wave1_count)

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
    wait_for_rows(table.name, total_expected)

    cols = {row[0]: row[1] for row in table.schema()}
    assert "CITY" in cols
    assert "AGE" in cols
    assert "COUNTRY" in cols, (
        f"Expected COUNTRY column after wave 2, got: {list(cols.keys())}"
    )

    rows = table.select(
        '"CITY", "AGE", "COUNTRY"',
        f'WHERE RECORD_METADATA:"offset"::number = {wave1_count}',
    )
    assert rows, f"Expected row at offset {wave1_count}"
    assert rows[0]["CITY"] == "Taipei"
    assert rows[0]["COUNTRY"] == "TW"

    null_country_count = table.select("count(*)", "WHERE COUNTRY IS NULL")[0][
        "COUNT(*)"
    ]
    assert null_country_count == wave1_count, (
        f"Expected {wave1_count} rows with NULL country, got {null_country_count}"
    )


def test_schema_evolution_disabled_mid_stream(
    driver, create_connector_from_file, create_table, create_topics, wait_for_rows
):
    """ENABLE_SCHEMA_EVOLUTION toggled off after initial evolution."""
    table = create_table(
        FILE_NAME.upper(),
        columns="(RECORD_METADATA VARIANT) ENABLE_SCHEMA_EVOLUTION = TRUE",
        cleanup_topic=False,
    )
    topic = create_topics([FILE_NAME], with_tables=False)[0]

    create_connector_from_file(CONFIG_FILE)
    driver.startConnectorWaitTime()

    # Wave 1: evolve schema while ENABLE_SCHEMA_EVOLUTION=TRUE
    wave1_count = 50
    wave1 = [
        json.dumps({"city": "Hsinchu", "age": i}).encode("utf-8")
        for i in range(wave1_count)
    ]
    driver.sendBytesData(topic, wave1, [], partition=0)
    wait_for_rows(table.name, wave1_count)

    _assert_success_rows(table, schematization=True, record_count=wave1_count)

    # Disable schema evolution on the table
    driver.snowflake_conn.cursor().execute(
        "ALTER TABLE identifier(%s) SET ENABLE_SCHEMA_EVOLUTION = FALSE", (table.name,)
    )

    # Wave 2: new column COUNTRY — DDL is still attempted and succeeds
    # because the test role has OWNERSHIP privilege.
    wave2_count = 50
    wave2 = [
        json.dumps({"city": "Taipei", "age": 100 + i, "country": "TW"}).encode("utf-8")
        for i in range(wave2_count)
    ]
    driver.sendBytesData(topic, wave2, [], partition=0)

    total = wave1_count + wave2_count
    wait_for_rows(table.name, total)

    cols = {row[0]: row[1] for row in table.schema()}
    assert "COUNTRY" in cols, (
        f"Expected COUNTRY column (DDL succeeded via OWNERSHIP), got: {list(cols.keys())}"
    )

    count = table.select_scalar("count(*)")
    assert count == total, f"Expected {total} rows, got {count}"


def test_schema_evolution_happy_path(
    driver, create_connector_from_file, create_table, create_topics, wait_for_rows
):
    """Send records that match the existing table schema exactly.

    Validation passes without triggering schema evolution. Verifies that
    client-side validation does not interfere with normal ingestion.
    """
    table = create_table(
        FILE_NAME.upper(),
        columns="(RECORD_METADATA VARIANT, CITY VARCHAR, AGE NUMBER) "
        "ENABLE_SCHEMA_EVOLUTION = TRUE",
        cleanup_topic=False,
    )
    topic = create_topics([FILE_NAME], with_tables=False)[0]

    create_connector_from_file(CONFIG_FILE)
    driver.startConnectorWaitTime()

    record_count = 100
    values = [
        json.dumps({"city": "Hsinchu", "age": i}).encode("utf-8")
        for i in range(record_count)
    ]
    driver.sendBytesData(topic, values, [], partition=0)

    wait_for_rows(table.name, record_count)

    rows = table.select(
        '"CITY", "AGE"',
        'WHERE RECORD_METADATA:"offset"::number = 0',
    )
    assert rows, "Expected row with offset 0"
    assert rows[0]["CITY"] == "Hsinchu"
    assert rows[0]["AGE"] == 0


def test_schema_evolution_drop_not_null(
    driver, create_connector_from_file, create_table, create_topics, wait_for_rows
):
    """Table has a NOT NULL column, but records omit it.

    Schema evolution should drop the NOT NULL constraint and add the extra
    column, allowing records to be ingested with NULL for the original column.
    """
    table = create_table(
        FILE_NAME.upper(),
        columns="(RECORD_METADATA VARIANT, STATUS VARCHAR NOT NULL) "
        "ENABLE_SCHEMA_EVOLUTION = TRUE",
        cleanup_topic=False,
    )
    topic = create_topics([FILE_NAME], with_tables=False)[0]

    create_connector_from_file(CONFIG_FILE)
    driver.startConnectorWaitTime()

    record_count = 50
    values = [
        json.dumps({"city": "Hsinchu", "age": i}).encode("utf-8")
        for i in range(record_count)
    ]
    driver.sendBytesData(topic, values, [], partition=0)

    wait_for_rows(table.name, record_count)

    cols = {row[0]: row[1] for row in table.schema()}
    assert "CITY" in cols, f"Expected CITY column, got: {list(cols.keys())}"
    assert "AGE" in cols, f"Expected AGE column, got: {list(cols.keys())}"
    assert "STATUS" in cols

    null_status_count = table.select("count(*)", "WHERE STATUS IS NULL")[0]["COUNT(*)"]
    assert null_status_count == record_count, (
        f"Expected {record_count} rows with NULL STATUS, got {null_status_count}"
    )


@pytest.mark.parametrize("schema_evo", [True, False], ids=["evo=on", "evo=off"])
@pytest.mark.parametrize(
    "schematization", [True, False], ids=["schema=on", "schema=off"]
)
@pytest.mark.parametrize("validation", [True, False], ids=["valid=on", "valid=off"])
def test_schema_evolution_config_variants(
    driver,
    name_salt,
    connector_version,
    create_connector_from_file,
    create_table,
    create_topics,
    wait_for_rows,
    schema_evo,
    schematization,
    validation,
):
    """Full config matrix for ENABLE_SCHEMA_EVOLUTION x schematization x validation.

    Runs for both v3 and v4. Combinations that are inapplicable to a given
    connector version are skipped with a reason (serving as documentation of
    the known v3/v4 differences).

    v4 (KC v4):
      - Client-side validation works for both schematization=on and off.
      - schematization=on: validates individual columns (CITY, AGE, etc.)
      - schematization=off: validates RECORD_CONTENT/RECORD_METADATA VARIANT
        columns against the table schema.
      - validation can be toggled via snowflake.validation.

    v3 (KC v3):
      - V1 Ingest SDK always performs client-side validation; it cannot be
        disabled, so all validation=False combos are skipped.
      - Schema evolution is gated behind schematization
        (enableSchemaEvolution = enableSchematization && hasSchemaEvolution-
        Permission), so schematization=off combos are skipped because the
        connector never attempts to evolve RECORD_CONTENT.

    Behaviour matrix (for combos that run):
      schema_evo=True:  extra columns are added and records are ingested.
      schema_evo=False + validation=True: extra columns route to DLQ.
      schema_evo=False + validation=False (v4 only): server Error Table
        handles errors; test returns early (no client-side assertion).
    """
    if connector_version == "v3":
        if not validation:
            pytest.skip(
                "KC v3 uses V1 Ingest SDK which always performs client-side "
                "validation; validation cannot be disabled"
            )
        if not schematization:
            pytest.skip(
                "KC v3 gates schema evolution behind schematization "
                "(enableSchemaEvolution = enableSchematization && "
                "hasSchemaEvolutionPermission), so RECORD_CONTENT is never "
                "evolved and the V1 SDK rejects it as an extra column"
            )

    evo_clause = "TRUE" if schema_evo else "FALSE"
    table = create_table(
        FILE_NAME.upper(),
        columns=f"(RECORD_METADATA VARIANT) ENABLE_SCHEMA_EVOLUTION = {evo_clause}",
        cleanup_topic=False,
    )
    topic = create_topics([FILE_NAME], with_tables=False)[0]

    evo_tag = "evo" if schema_evo else "noevo"
    sch_tag = "sch" if schematization else "nosch"
    val_tag = "val" if validation else "noval"
    dlq_topic = f"DLQ_MATRIX_{FILE_NAME}_{name_salt}_{evo_tag}_{sch_tag}_{val_tag}"

    overrides = {
        "snowflake.enable.schematization": str(schematization).lower(),
        "snowflake.validation": "client_side" if validation else "server_side",
        "errors.deadletterqueue.topic.name": dlq_topic,
    }

    config = create_connector_from_file(CONFIG_FILE, config_overrides=overrides)
    driver.startConnectorWaitTime()

    if schema_evo:
        record_count = 100
        values = [
            json.dumps({"city": "Hsinchu", "age": i}).encode("utf-8")
            for i in range(record_count)
        ]
        driver.sendBytesData(topic, values, [], partition=0)

        wait_for_rows(table.name, record_count)

        _assert_success_rows(table, schematization, record_count)
    else:
        if not validation:
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


@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
@pytest.mark.parametrize("validation", [True, False], ids=["valid=on", "valid=off"])
def test_schema_evolution_nested_record_content(
    driver, name_salt, create_connector, snowflake_table, wait_for_rows, validation
):
    """Schema evolution with schematization=off and nested data.

    Verifies that RECORD_CONTENT is created as VARIANT (not VARCHAR) and that
    nested objects, arrays, and mixed types are preserved and queryable.
    """
    topic = snowflake_table(
        FILE_NAME,
        f"CREATE OR REPLACE TABLE {FILE_NAME}{name_salt} "
        f"(RECORD_METADATA VARIANT) ENABLE_SCHEMA_EVOLUTION = TRUE",
    )

    driver.createTopics(topic, partitionNum=1, replicationNum=1)

    overrides = {
        "snowflake.enable.schematization": "false",
        "snowflake.client.validation.enabled": str(validation).lower(),
    }
    create_connector(CONFIG_FILE, config_overrides=overrides)
    driver.startConnectorWaitTime()

    record_count = 50
    values = [
        json.dumps(
            {
                "user": {"name": "Alice", "scores": [1, 2, 3]},
                "tags": ["a", "b"],
                "count": i,
            }
        ).encode("utf-8")
        for i in range(record_count)
    ]
    driver.sendBytesData(topic, values, [], partition=0)

    wait_for_rows(topic, record_count)

    # Verify RECORD_CONTENT column exists and is VARIANT
    cols = {
        row[0]: row[1]
        for row in driver.snowflake_conn.cursor()
        .execute(f"DESCRIBE TABLE {topic}")
        .fetchall()
    }
    assert "RECORD_CONTENT" in cols, (
        f"Expected RECORD_CONTENT column, got: {list(cols.keys())}"
    )
    assert "VARIANT" in cols["RECORD_CONTENT"].upper(), (
        f"Expected RECORD_CONTENT to be VARIANT, got: {cols['RECORD_CONTENT']}"
    )

    # Verify nested data is queryable
    row = (
        driver.snowflake_conn.cursor(DictCursor)
        .execute(
            f"SELECT "
            f"RECORD_CONTENT:user.name::string AS user_name, "
            f"RECORD_CONTENT:user.scores[0]::number AS first_score, "
            f"RECORD_CONTENT:tags[0]::string AS first_tag, "
            f"RECORD_CONTENT:count::number AS cnt "
            f"FROM {topic} "
            f'WHERE RECORD_METADATA:"offset"::number = 0'
        )
        .fetchone()
    )
    assert row is not None, "Expected row with offset 0"
    assert row["USER_NAME"] == "Alice"
    assert row["FIRST_SCORE"] == 1
    assert row["FIRST_TAG"] == "a"
    assert row["CNT"] == 0

    count = driver.select_number_of_records(topic)
    assert count == record_count, f"Expected {record_count} rows, got {count}"
