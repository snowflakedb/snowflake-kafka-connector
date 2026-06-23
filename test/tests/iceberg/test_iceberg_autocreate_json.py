"""Iceberg table auto-creation E2E tests (autocreate.table.type routing).

Exercises the ``snowflake.autocreate.table.type`` connector setting end-to-end through real
Kafka -> Connect -> Snowflake:
  * ``iceberg`` with no pre-created table -> connector auto-creates a managed Iceberg table.
  * ``iceberg`` with a pre-existing (higher-version) table -> connector ingests into it as-is.

v4-only.
"""

import json
import logging

import pytest
from snowflake.connector import DictCursor

from lib.driver import KafkaDriver, quote_name
from lib.fixtures.table import ICEBERG_EXTERNAL_VOLUME, IcebergTable
from tests.iceberg import json_connector_config

logger = logging.getLogger(__name__)


def _is_iceberg_table(driver: KafkaDriver, name: str) -> bool:
    rows = (
        driver.snowflake_conn.cursor()
        .execute(f"SHOW ICEBERG TABLES LIKE '{name}'")
        .fetchall()
    )
    return len(rows) > 0


def _error_logging_enabled(driver: KafkaDriver, name: str) -> str:
    """Return the ``error_logging`` flag SHOW TABLES reports for ``name``.

    SHOW TABLES renders this column as 'ON'/'OFF' (not 'Y'/'N'). Raises if the
    column is absent, which means the account lacks
    ``ENABLE_ERROR_LOGGING_COLUMN_IN_SHOW_TABLES`` — a real environment problem we
    want surfaced loudly, not silently skipped.
    """
    cursor = driver.snowflake_conn.cursor(DictCursor)
    rows = cursor.execute(f"SHOW TABLES LIKE '{name}'").fetchall()
    assert rows, f"Expected SHOW TABLES to return a row for {name}"
    row = rows[0]
    assert "error_logging" in row, (
        f"SHOW TABLES has no error_logging column for {name}; account is missing "
        f"ENABLE_ERROR_LOGGING_COLUMN_IN_SHOW_TABLES. Columns: {list(row.keys())}"
    )
    return row["error_logging"]


def _desc(driver, table_name):
    """Return {column_name: column_type} for a table via DESCRIBE.

    Quotes the name: connector-auto-created tables and fixture tables can be
    case-sensitive (lowercase) identifiers, which an unquoted DESCRIBE would
    uppercase and fail to find.
    """
    return {
        row[0]: row[1]
        for row in driver.snowflake_conn.cursor()
        .execute(f"DESCRIBE TABLE {quote_name(table_name)}")
        .fetchall()
    }


def _multi_topic_iceberg_config(topics, table_name, *, create_options=None, extra=None):
    """Build a server-side iceberg connector config fanning ``topics`` into one table.

    Mirrors the FDN multi-topic pattern (``snowflake.topic2table.map``) but for
    managed Iceberg: server-side validation only (the connector rejects iceberg +
    schematization + client-side), ``table.type=iceberg``. Pass ``create_options``
    to drive auto-creation of ``table_name``; omit when the table is pre-created.
    """
    cfg = {
        **json_connector_config(
            ",".join(topics),
            schematization=True,
            validation=False,
            table_type="iceberg",
            iceberg_create_table_options=create_options,
        ),
        "snowflake.topic2table.map": ",".join(f"{t}:{table_name}" for t in topics),
    }
    if extra:
        cfg.update(extra)
    return cfg


# Datatype-coverage payloads for the multi-topic autocreate test: two records whose
# union of fields exercises VARCHAR/NUMBER/BOOLEAN inference. PERF_GOLD_TYPES is the
# expected DESCRIBE type prefix per column after server-side SE adds them.
PERF_RECORDS = [
    {"PERFORMANCE_STRING": "Excellent", "PERFORMANCE_CHAR": "A", "RATING_INT": 100},
    {"PERFORMANCE_STRING": "Excellent", "RATING_DOUBLE": 0.99, "APPROVAL": True},
]
PERF_GOLD_TYPES = {
    "PERFORMANCE_STRING": "VARCHAR",
    "PERFORMANCE_CHAR": "VARCHAR",
    "RATING_INT": "NUMBER",
    # Iceberg server-side SE infers NUMBER(38,2) for a JSON float (e.g. 0.99),
    # unlike FDN which infers FLOAT.
    "RATING_DOUBLE": "NUMBER",
    "APPROVAL": "BOOLEAN",
}


@pytest.mark.iceberg
@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
@pytest.mark.parametrize(
    "iceberg_version",
    [None, 3],
    ids=["version-unset(v2)", "version-3"],
)
def test_iceberg_autocreate_json(
    driver, create_topics, create_connector, wait_for_rows, iceberg_version
):
    """table.type=iceberg with NO pre-created table -> connector auto-creates it.

    Parametrized over the Iceberg format version of the auto-created table:
      * ``version-unset(v2)`` — no ICEBERG_VERSION clause, so the table is created
        at the account-default format version (2). This is the case the structured
        RECORD_METADATA OBJECT exists for: format v2 cannot use a VARIANT metadata
        column (VARIANT-in-Iceberg requires v3), so ingestion *must* go through the
        structured-OBJECT metadata that ``conformIcebergMetadata`` produces.
      * ``version-3`` — explicit ICEBERG_VERSION=3.

    Uses server-side SE (validation=false): the connector auto-creates the
    metadata-only Iceberg table, then server-side schema evolution adds the ID/CITY
    columns as records flow. Client-side validation is intentionally not used with
    Iceberg + SE (the connector rejects that combination); server-side SE is the
    supported mechanism.

    Also asserts the auto-created table comes out with ERROR_LOGGING enabled (the
    connector's CREATE includes ``error_logging = true``).
    """
    base_name = "iceberg_autocreate"
    # with_tables=False -> neither table nor pipe pre-created; topic name == target table name.
    topic = create_topics([base_name], with_tables=False)[0]
    options = f"EXTERNAL_VOLUME='{ICEBERG_EXTERNAL_VOLUME}'"
    if iceberg_version is not None:
        options += f" ICEBERG_VERSION={iceberg_version}"
    table = IcebergTable(driver, topic)
    try:
        create_connector(
            v4_config=json_connector_config(
                topic,
                schematization=True,
                validation=False,
                table_type="iceberg",
                iceberg_create_table_options=options,
            )
        )
        driver.startConnectorWaitTime()

        count = 50
        driver.sendBytesData(
            topic,
            [
                json.dumps({"id": i, "city": "Hsinchu"}).encode("utf-8")
                for i in range(count)
            ],
            partition=0,
        )
        wait_for_rows(topic, count)

        assert _is_iceberg_table(driver, topic), (
            f"Expected {topic} to have been auto-created as an ICEBERG table"
        )
        assert _error_logging_enabled(driver, topic) == "ON", (
            f"Expected auto-created iceberg table {topic} to have ERROR_LOGGING enabled"
        )
        cols = {row[0] for row in table.schema()}
        assert "ID" in cols, f"Expected SE to add ID column, got: {cols}"
        assert "CITY" in cols, f"Expected SE to add CITY column, got: {cols}"
    finally:
        table.drop()


@pytest.mark.iceberg
@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
def test_iceberg_config_existing_higher_version_table_harmless(
    driver, create_iceberg_table, create_topics, create_connector, wait_for_rows
):
    """config table.type=iceberg (version unset/2) + pre-existing v3 table -> harmless.

    The version knob is create-time-only; an existing table is used as-is. The e2e
    create_iceberg_table fixture creates a v3 table, so this proves the connector happily
    ingests into a higher-version table than its (default v2) config would create. Plain
    ingest into the pre-declared columns -- no SE, so independent of the server-side SE stack.
    """
    base_name = "iceberg_existing_v3"
    table = create_iceberg_table(
        base_name,
        columns="(RECORD_METADATA VARIANT, ID NUMBER(19,0), CITY TEXT)",
        cleanup_topic=False,
    )
    topic = create_topics([base_name], with_tables=False)[0]

    create_connector(
        v4_config=json_connector_config(
            topic,
            schematization=True,
            validation=False,
            table_type="iceberg",  # version unset -> default 2; existing table is v3
            iceberg_create_table_options=f"EXTERNAL_VOLUME='{ICEBERG_EXTERNAL_VOLUME}'",
        )
    )
    driver.startConnectorWaitTime()

    count = 50
    driver.sendBytesData(
        topic,
        [json.dumps({"id": i, "city": "Taipei"}).encode("utf-8") for i in range(count)],
        partition=0,
    )
    wait_for_rows(table.name, count)

    landed = table.select_scalar("COUNT(*)")
    assert landed == count, (
        f"Expected {count} rows in the pre-existing v3 table, got {landed}"
    )


@pytest.mark.iceberg
@pytest.mark.schema_evolution
@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
def test_iceberg_autocreate_multi_topic(
    driver: KafkaDriver,
    name_salt: str,
    create_connector,
    wait_for_rows,
):
    """Two topics with different schemas auto-create + evolve one iceberg table.

    Server-side mirror of FDN ``test_se_auto_table_creation_json``. The table does
    not exist; the connector auto-creates the metadata-only iceberg table (no
    ICEBERG_VERSION -> default format v2, exercising structured-OBJECT metadata),
    then server-side SE adds every payload column from both topics.
    """
    initial_batch = 12
    flush_batch = 300
    record_count = initial_batch + flush_batch

    base = f"iceberg_se_autocreate_multi{name_salt}"
    table_name = base.upper()
    topics = [f"{base}{i}" for i in range(2)]
    for t in topics:
        driver.createTopics(t, partitionNum=1, replicationNum=1)

    connector = create_connector(
        v4_config=_multi_topic_iceberg_config(
            topics,
            table_name,
            create_options=f"EXTERNAL_VOLUME='{ICEBERG_EXTERNAL_VOLUME}'",
        )
    )
    table = IcebergTable(driver, table_name)
    try:
        driver.startConnectorWaitTime()

        for i, topic in enumerate(topics):
            for batch_size in (initial_batch, flush_batch):
                keys = [
                    json.dumps({"number": str(e)}).encode("utf-8")
                    for e in range(batch_size)
                ]
                values = [
                    json.dumps(PERF_RECORDS[i]).encode("utf-8")
                    for _ in range(batch_size)
                ]
                driver.sendBytesData(topic, values, keys)

        wait_for_rows(
            table_name, record_count * len(topics), connector_name=connector.name
        )

        assert _is_iceberg_table(driver, table_name), (
            f"Expected {table_name} to be auto-created as ICEBERG"
        )
        cols = _desc(driver, table_name)
        for col_name, prefix in PERF_GOLD_TYPES.items():
            assert col_name in cols, (
                f"Missing column {col_name}, got: {list(cols.keys())}"
            )
            assert cols[col_name].startswith(prefix), (
                f"Column {col_name}: expected {prefix}, got {cols[col_name]}"
            )
    finally:
        table.drop()


@pytest.mark.iceberg
@pytest.mark.schema_evolution
@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
@pytest.mark.parametrize(
    "validation", ["server_side", "client_side"], ids=["valid=off", "valid=on"]
)
@pytest.mark.parametrize(
    "schematization", [True, False], ids=["schema=on", "schema=off"]
)
def test_iceberg_config_variants(
    driver: KafkaDriver,
    create_iceberg_table,
    create_topics,
    create_connector,
    wait_for_rows,
    schematization,
    validation,
):
    """Iceberg-applicable slice of the FDN config matrix (schematization x validation).

    The FDN ``test_schema_evolution_config_variants`` cells collapse for managed
    Iceberg:
      * schematization=on + client_side -> rejected by the connector guard
        (iceberg SE is server-side only); skipped here, asserted by the unit test
        ``SinkTaskConfigTest`` / the e2e fail-fast behavior.
      * schematization=on + server_side -> SE path: extra columns are added.
      * schematization=off (either validation) -> data lands in RECORD_CONTENT
        VARIANT; client-side validation IS permitted because the guard only blocks
        schematization+client_side.
    """
    if schematization and validation == "client_side":
        pytest.skip(
            "iceberg + schematization + client_side is rejected by the connector"
            " (server-side SE only); covered by SinkTaskConfig unit test"
        )

    use_client_side = validation == "client_side"
    base_name = "iceberg_se_variant"

    if schematization:
        table = create_iceberg_table(
            base_name,
            columns="(RECORD_METADATA VARIANT) ENABLE_SCHEMA_EVOLUTION = TRUE",
            cleanup_topic=False,
        )
    else:
        # Non-schematized: data is wrapped into RECORD_CONTENT VARIANT (v3 table).
        table = create_iceberg_table(
            base_name,
            columns="(RECORD_METADATA VARIANT, RECORD_CONTENT VARIANT)",
            cleanup_topic=False,
        )
    topic = create_topics([base_name], with_tables=False)[0]

    create_connector(
        v4_config=json_connector_config(
            topic,
            schematization=schematization,
            validation=use_client_side,
            table_type="iceberg",
        )
    )
    driver.startConnectorWaitTime()

    record_count = 100
    driver.sendBytesData(
        topic,
        [
            json.dumps({"city": "Hsinchu", "age": i}).encode("utf-8")
            for i in range(record_count)
        ],
        partition=0,
    )
    wait_for_rows(table.name, record_count)

    if schematization:
        cols = _desc(driver, table.name)
        assert "CITY" in cols, f"Expected CITY column, got: {list(cols.keys())}"
        assert "AGE" in cols, f"Expected AGE column, got: {list(cols.keys())}"
    else:
        rows = table.select(
            "RECORD_CONTENT", 'WHERE RECORD_METADATA:"offset"::number = 0'
        )
        assert rows, "Expected row with offset 0"
        content = json.loads(rows[0]["RECORD_CONTENT"])
        assert content["city"] == "Hsinchu"
        assert content["age"] == 0
    assert table.select_scalar("COUNT(*)") == record_count
