from typing import List
import pytest
from lib.driver import KafkaDriver


@pytest.fixture()
def create_table(driver: KafkaDriver, name_salt: str, request: pytest.FixtureRequest):
    """Creates a table in the test schema. Defaults to the test name.

    `columns` can also be followed with table options, e.g.
    ``"(col1 TYPE, col2 TYPE) ENABLE_SCHEMA_EVOLUTION = TRUE"``.

    The Kafka topic is cleaned up after the test.  The Snowflake table
    (and associated stage/pipe) is left for the session-scoped
    `test_schema` teardown (`DROP SCHEMA ... CASCADE`) to remove.
    """

    created_tables: List[str] = []
    topics_to_cleanup: List[str] = []

    def _create(
        unsalted_name: str = None, *, columns: str, cleanup_topic: bool = True
    ) -> str:
        unsalted_name = unsalted_name or request.node.originalname
        table_name = unsalted_name + name_salt
        driver.snowflake_conn.cursor().execute(
            f"CREATE OR REPLACE TABLE identifier(%s) {columns}",
            (table_name,),
        )
        created_tables.append(table_name)
        if cleanup_topic:
            topics_to_cleanup.append(table_name)
        return table_name

    try:
        yield _create
    finally:
        for table in created_tables:
            driver.drop_table(table)
        for topic in topics_to_cleanup:
            driver.deleteTopic(topic)
