import logging
import os
from typing import List

import pytest
from lib.driver import KafkaDriver, quote_name
from snowflake.connector import DictCursor

logger = logging.getLogger(__name__)

ICEBERG_EXTERNAL_VOLUME = os.environ.get(
    "ICEBERG_EXTERNAL_VOLUME", "kafka_push_e2e_volume_aws"
)


@pytest.fixture()
def snowflake_table(
    driver: KafkaDriver, name_salt: str, request: pytest.FixtureRequest
):
    """Tears down the Snowflake table named after the current test at teardown.

    Table name: ``{test_function_name_without_test_prefix}{name_salt}``

    Tests that manually create a table (or rely on auto-table-creation) declare
    this fixture to ensure the table is dropped after the test completes.
    """
    table_name = (request.node.originalname.removeprefix("test_") + name_salt).upper()
    yield table_name
    driver.drop_table(table_name)


class Table:
    """Class with helper functions for working with a Snowflake table.
    Doesn't create the table unless you call `create`."""

    def __init__(self, driver: KafkaDriver, name: str):
        self.driver = driver
        self.name = name

    def create(self, columns: str):
        self.driver.snowflake_conn.cursor().execute(
            f"CREATE OR REPLACE TABLE {quote_name(self.name)} {columns}"
        )

    def select(self, projections: str, extra_clauses: str = ""):
        return (
            self.driver.snowflake_conn.cursor(DictCursor)
            .execute(
                f"SELECT {projections} FROM {quote_name(self.name)} {extra_clauses}"
            )
            .fetchall()
        )

    def select_scalar(self, projection: str):
        return (
            self.driver.snowflake_conn.cursor()
            .execute(f"SELECT {projection} FROM {quote_name(self.name)}")
            .fetchone()[0]
        )

    def schema(self):
        return (
            self.driver.snowflake_conn.cursor()
            .execute(f"DESC TABLE {quote_name(self.name)}")
            .fetchall()
        )

    def drop(self):
        self.driver.drop_table(self.name)


class IcebergTable(Table):
    """Iceberg table variant — uses ``CREATE/DROP ICEBERG TABLE`` DDL.

    ``columns`` follows the same convention as :class:`Table` and can include
    table-level options after the column list, e.g.
    ``"(RECORD_METADATA VARIANT, CITY TEXT) ENABLE_SCHEMA_EVOLUTION = TRUE"``.

    The iceberg-specific clauses (``EXTERNAL_VOLUME``, ``CATALOG``,
    ``BASE_LOCATION``, ``ICEBERG_VERSION``) are appended automatically.
    """

    def create(self, columns: str):
        self.driver.snowflake_conn.cursor().execute(
            f"CREATE OR REPLACE ICEBERG TABLE {quote_name(self.name)} "
            f"{columns} "
            f"EXTERNAL_VOLUME = '{ICEBERG_EXTERNAL_VOLUME}' "
            f"CATALOG = 'SNOWFLAKE' "
            f"BASE_LOCATION = '{self.name}' "
            f"ICEBERG_VERSION = 3"
        )

    def drop(self):
        self.driver.snowflake_conn.cursor().execute(
            f"DROP ICEBERG TABLE IF EXISTS {quote_name(self.name)}"
        )


@pytest.fixture(scope="session")
def iceberg_external_volume(driver: KafkaDriver):
    """Session-scoped probe: checks whether the iceberg external volume exists.

    Returns the volume name if available, otherwise calls ``pytest.skip()``.
    Every test that uses ``create_iceberg_table`` transitively depends on this
    fixture, so all iceberg tests are skipped in environments where the volume
    is not provisioned (e.g. AZURE, GCP CI accounts).
    """
    try:
        rows = (
            driver.snowflake_conn.cursor()
            .execute(f"DESC EXTERNAL VOLUME {ICEBERG_EXTERNAL_VOLUME}")
            .fetchall()
        )
        if rows:
            logger.info(
                "Iceberg external volume %s is available", ICEBERG_EXTERNAL_VOLUME
            )
            return ICEBERG_EXTERNAL_VOLUME
    except Exception:
        logger.debug(
            "Failed to describe external volume %s",
            ICEBERG_EXTERNAL_VOLUME,
            exc_info=True,
        )
    pytest.skip(
        f"Iceberg external volume '{ICEBERG_EXTERNAL_VOLUME}' not found — "
        f"skipping iceberg tests (set ICEBERG_EXTERNAL_VOLUME env var to override)"
    )


@pytest.fixture()
def create_iceberg_table(
    driver: KafkaDriver,
    name_salt: str,
    request: pytest.FixtureRequest,
    iceberg_external_volume: str,
):
    """Creates an iceberg table in the test schema.  Mirrors :func:`create_table`
    but produces :class:`IcebergTable` objects.

    ``columns`` can include table-level options after the column list, e.g.
    ``"(RECORD_METADATA VARIANT, CITY TEXT) ENABLE_SCHEMA_EVOLUTION = TRUE"``.

    Teardown: drops the iceberg table and, when ``cleanup_topic=True`` (the
    default), also deletes the matching Kafka topic.
    """

    created_tables: List[IcebergTable] = []
    topics_to_cleanup: List[str] = []

    def _create(
        unsalted_name: str = None, *, columns: str, cleanup_topic: bool = True
    ) -> IcebergTable:
        unsalted_name = unsalted_name or request.node.originalname
        table_name = unsalted_name + name_salt
        table = IcebergTable(driver, table_name)
        table.create(columns)
        created_tables.append(table)
        if cleanup_topic:
            topics_to_cleanup.append(table.name)
        return table

    try:
        yield _create
    finally:
        for table in created_tables:
            table.drop()
        for topic in topics_to_cleanup:
            driver.deleteTopic(topic)


@pytest.fixture()
def create_table(driver: KafkaDriver, name_salt: str, request: pytest.FixtureRequest):
    """Creates a table in the test schema. Defaults to the test name.

    `columns` can also be followed with table options, e.g.
    ``"(col1 TYPE, col2 TYPE) ENABLE_SCHEMA_EVOLUTION = TRUE"``.

    The Kafka topic is cleaned up after the test.  The Snowflake table
    (and associated stage/pipe) is left for the session-scoped
    `test_schema` teardown (`DROP SCHEMA ... CASCADE`) to remove.
    """

    created_tables: List[Table] = []
    topics_to_cleanup: List[str] = []

    def _create(
        unsalted_name: str = None, *, columns: str, cleanup_topic: bool = True
    ) -> Table:
        unsalted_name = unsalted_name or request.node.originalname
        table_name = unsalted_name + name_salt
        table = Table(driver, table_name)
        table.create(columns)
        created_tables.append(table)
        if cleanup_topic:
            topics_to_cleanup.append(table.name)
        return table

    try:
        yield _create
    finally:
        for table in created_tables:
            table.drop()
        for topic in topics_to_cleanup:
            driver.deleteTopic(topic)
