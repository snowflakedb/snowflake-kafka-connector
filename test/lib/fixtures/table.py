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

    def select_scalar(self, projection: str, extra_clauses: str = ""):
        return (
            self.driver.snowflake_conn.cursor()
            .execute(
                f"SELECT {projection} FROM {quote_name(self.name)} {extra_clauses}"
            )
            .fetchone()[0]
        )

    def schema(self, *, as_dict: bool = False):
        return (
            (
                self.driver.snowflake_conn.cursor(DictCursor)
                if as_dict
                else self.driver.snowflake_conn.cursor()
            )
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

    Note: Snowflake-managed storage (``EXTERNAL_VOLUME='SNOWFLAKE_MANAGED'``)
    rejects ``BASE_LOCATION`` ("not supported for ... Snowflake Managed
    Storage"), so it is omitted in that case — Snowflake constructs the path.
    """

    def create(self, columns: str):
        managed = ICEBERG_EXTERNAL_VOLUME.upper() == "SNOWFLAKE_MANAGED"
        base_location = "" if managed else f"BASE_LOCATION = '{self.name}' "
        self.driver.snowflake_conn.cursor().execute(
            f"CREATE OR REPLACE ICEBERG TABLE {quote_name(self.name)} "
            f"{columns} "
            f"EXTERNAL_VOLUME = '{ICEBERG_EXTERNAL_VOLUME}' "
            f"CATALOG = 'SNOWFLAKE' "
            f"{base_location}"
            f"ICEBERG_VERSION = 3"
        )

    def drop(self):
        self.driver.snowflake_conn.cursor().execute(
            f"DROP ICEBERG TABLE IF EXISTS {quote_name(self.name)}"
        )


@pytest.fixture(scope="session")
def iceberg_external_volume(driver: KafkaDriver):
    """Session-scoped probe: verifies the effective ``ICEBERG_EXTERNAL_VOLUME`` can
    actually create (and drop) an Iceberg table, otherwise calls ``pytest.skip()``.

    We create-and-drop rather than ``DESC EXTERNAL VOLUME`` because:
      * ``DESC`` needs OWNERSHIP on the volume (the CI role only has USAGE), and
      * the reserved value ``SNOWFLAKE_MANAGED`` is not a describable volume object.
    A create-probe validates real external volumes AND Snowflake-managed storage
    uniformly, on any cloud (AWS has ``kafka_push_e2e_volume_aws``; AZURE supports
    ``SNOWFLAKE_MANAGED``; GCP supports neither today → its iceberg tests skip).

    Enforced for *every* ``@pytest.mark.iceberg`` test via the autouse
    ``_require_iceberg_volume`` gate in conftest — not only those using
    ``create_iceberg_table`` — so connector-auto-create tests can't bypass it.

    Skips only when the volume is genuinely unavailable ("does not exist or not
    authorized"). Any other probe failure (token expiry, rate limit, network) is
    re-raised loudly via ``pytest.fail`` rather than silently skipping the whole
    iceberg suite — the session-scoped skip is cached, so a swallowed transient
    error would masquerade as "0 failed" for every iceberg test.
    """
    probe = IcebergTable(driver, "KC_ICEBERG_VOLUME_PROBE")
    try:
        probe.create("(id INT)")
        logger.info("Iceberg external volume %s is usable", ICEBERG_EXTERNAL_VOLUME)
        return ICEBERG_EXTERNAL_VOLUME
    except Exception as e:
        if "does not exist or not authorized" in str(e).lower():
            logger.info(
                "Iceberg external volume %s not available, skipping iceberg tests",
                ICEBERG_EXTERNAL_VOLUME,
            )
            pytest.skip(
                f"Iceberg external volume '{ICEBERG_EXTERNAL_VOLUME}' not available — "
                f"skipping iceberg tests (set ICEBERG_EXTERNAL_VOLUME env var to override)"
            )
        logger.error(
            "Iceberg external volume %s probe failed unexpectedly",
            ICEBERG_EXTERNAL_VOLUME,
            exc_info=True,
        )
        pytest.fail(
            f"Iceberg external volume '{ICEBERG_EXTERNAL_VOLUME}' probe failed "
            f"unexpectedly (not a 'volume unavailable' error): {e}"
        )
    finally:
        try:
            probe.drop()
        except Exception:
            pass


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
