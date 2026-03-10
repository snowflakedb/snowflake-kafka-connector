import json

from snowflake.connector import DictCursor

FILE_NAME = "snowpipe_streaming_schema_evolution"
CONFIG_FILE = f"{FILE_NAME}.json"


def test_schema_evolution_add_columns(
    driver, name_salt, create_connector, snowflake_table, wait_for_rows
):
    """Create a table with only RECORD_METADATA, send records with extra fields.

    Verifies that unquoted column names (city, age) are correctly evolved as CITY, AGE.
    """
    topic = snowflake_table(
        FILE_NAME,
        f"CREATE OR REPLACE TABLE {FILE_NAME}{name_salt} "
        f"(RECORD_METADATA VARIANT)",
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

    cols = {
        row[0]: row[1]
        for row in driver.snowflake_conn.cursor()
        .execute(f"DESCRIBE TABLE {topic}")
        .fetchall()
    }
    assert "CITY" in cols, f"Expected CITY column, got: {list(cols.keys())}"
    assert "AGE" in cols, f"Expected AGE column, got: {list(cols.keys())}"
    assert "RECORD_METADATA" in cols

    row = (
        driver.snowflake_conn.cursor(DictCursor)
        .execute(
            f'SELECT "CITY", "AGE", RECORD_METADATA '
            f"FROM {topic} "
            f"WHERE RECORD_METADATA:\"offset\"::number = 0"
        )
        .fetchone()
    )
    assert row is not None, "Expected row with offset 0"
    assert row["CITY"] == "Hsinchu"
    assert row["AGE"] == 0


def test_schema_evolution_multi_wave(
    driver, name_salt, create_connector, snowflake_table, wait_for_rows
):
    """Send two waves of records with different schemas.

    Wave 1: {city, age}           → ADD COLUMN for CITY, AGE
    Wave 2: {city, age, country}  → ADD COLUMN for COUNTRY
    Verifies that wave-1 rows have NULL for COUNTRY.
    """
    topic = snowflake_table(
        FILE_NAME,
        f"CREATE OR REPLACE TABLE {FILE_NAME}{name_salt} "
        f"(RECORD_METADATA VARIANT)",
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
            f"WHERE RECORD_METADATA:\"offset\"::number = {wave1_count}"
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
