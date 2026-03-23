import json

from snowflake.connector import DictCursor

FILE_NAME = "nested_json_ingested_as_variant"
CONFIG_FILE = f"{FILE_NAME}.json"


def test_nested_json_ingested_as_variant(
    driver, name_salt, create_connector, wait_for_rows
):
    """Nested JSON data lands as queryable VARIANT in RECORD_CONTENT.

    Table is NOT pre-created — the connector auto-creates it.
    KCv3 auto-creates with (RECORD_METADATA VARIANT, RECORD_CONTENT VARIANT).
    KCv4 auto-creates with both columns as VARIANT when schematization=off.

    Runs for both v3 and v4 to verify compatibility.
    """
    topic = f"{FILE_NAME}{name_salt}"

    driver.createTopics(topic, partitionNum=1, replicationNum=1)

    try:
        create_connector(CONFIG_FILE)
        driver.startConnectorWaitTime()

        values = [
            # 0: nested object with arrays
            json.dumps(
                {
                    "user": {"name": "Alice", "scores": [1, 2, 3]},
                    "tags": ["a", "b"],
                    "count": 42,
                }
            ).encode("utf-8"),
            # 1: deeply nested
            json.dumps({"a": {"b": {"c": {"d": "deep"}}}}).encode("utf-8"),
            # 2: flat object
            json.dumps({"city": "Hsinchu", "age": 30}).encode("utf-8"),
        ]
        record_count = len(values)
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

        # Verify nested object with arrays (offset 0)
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
        assert row["CNT"] == 42

        # Verify deeply nested (offset 1)
        row = (
            driver.snowflake_conn.cursor(DictCursor)
            .execute(
                f"SELECT RECORD_CONTENT:a.b.c.d::string AS val "
                f"FROM {topic} "
                f'WHERE RECORD_METADATA:"offset"::number = 1'
            )
            .fetchone()
        )
        assert row is not None, "Expected row with offset 1"
        assert row["VAL"] == "deep"

        # Verify flat object (offset 2)
        row = (
            driver.snowflake_conn.cursor(DictCursor)
            .execute(
                f"SELECT "
                f"RECORD_CONTENT:city::string AS city, "
                f"RECORD_CONTENT:age::number AS age "
                f"FROM {topic} "
                f'WHERE RECORD_METADATA:"offset"::number = 2'
            )
            .fetchone()
        )
        assert row is not None, "Expected row with offset 2"
        assert row["CITY"] == "Hsinchu"
        assert row["AGE"] == 30

        count = driver.select_number_of_records(topic)
        assert count == record_count, f"Expected {record_count} rows, got {count}"
    finally:
        driver.deleteTopic(topic)
