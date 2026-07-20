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
    # This test routes both topics into one table via snowflake.topic2table.map (see
    # _multi_topic_iceberg_config). The connector resolves the map target as an
    # unquoted identifier -> UPPERCASES it, so the auto-created table is base.upper();
    # query by that name. (Contrast the default-mapping tests below, which keep the
    # topic's exact case and query by `base`.)
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


# ---------------------------------------------------------------------------
# Test A: table.type=none + missing table -> fail-fast ERROR_0034
# ---------------------------------------------------------------------------


@pytest.mark.iceberg
@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
def test_iceberg_autocreate_none_missing_table_fails(
    driver: KafkaDriver,
    create_topics,
    create_connector,
):
    """table.type=none with no pre-existing table -> connector fails fast with ERROR_0034.

    The ``none`` routing tells the connector NOT to auto-create any table: if the
    target table does not exist the connector must fail immediately with
    "does not exist and snowflake.autocreate.table.type=none" (ERROR_0034) rather
    than silently dropping data.

    Complementary to #1491's schematized auto-create happy paths: this exercises
    the fail-fast guard on the ``none`` branch of ``createTableIfNotExists``.
    No ingest wait — the task must fail before any rows can land.
    """
    import time

    base_name = "iceberg_none_missing"
    # with_tables=False -> neither table nor pipe pre-created; topic name == target table name.
    topic = create_topics([base_name], with_tables=False)[0]
    connector = create_connector(
        v4_config=json_connector_config(
            topic,
            schematization=True,
            validation=False,
            table_type="none",
        )
    )
    driver.startConnectorWaitTime()

    driver.sendBytesData(
        topic,
        [json.dumps({"id": 0, "city": "Seoul"}).encode("utf-8")],
        partition=0,
    )

    # Poll for a FAILED task — no ingest wait; the connector must never create the table.
    deadline = time.monotonic() + 120
    failed_tasks = []
    while time.monotonic() < deadline:
        failed_tasks = driver.get_failed_tasks(connector.name)
        if failed_tasks:
            break
        time.sleep(3)

    assert failed_tasks, (
        "Expected at least one FAILED task within 120s when table.type=none and "
        "the target table does not exist"
    )
    trace = failed_tasks[0].get("trace", "")
    assert "does not exist and snowflake.autocreate.table.type=none" in trace, (
        f"Expected ERROR_0034 trace containing "
        f"'does not exist and snowflake.autocreate.table.type=none', got:\n{trace}"
    )
    # Table must never have been created.
    assert not _is_iceberg_table(driver, topic), (
        "Expected no iceberg table to be auto-created with table.type=none, "
        "but SHOW ICEBERG TABLES found one"
    )


# ---------------------------------------------------------------------------
# Test B: table.type=iceberg + schematization=false + missing ICEBERG_VERSION -> ERROR_0034
# ---------------------------------------------------------------------------


@pytest.mark.iceberg
@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
def test_iceberg_autocreate_non_schematized_requires_v3(
    driver: KafkaDriver,
    create_topics,
    create_connector,
):
    """table.type=iceberg + schematization=false + no ICEBERG_VERSION -> fails with ERROR_0034.

    Without ICEBERG_VERSION=3 the connector cannot lay down a RECORD_CONTENT VARIANT
    column (VARIANT-in-Iceberg requires format v3). The connector must fail fast with
    "requires ICEBERG_VERSION=3 ... RECORD_CONTENT VARIANT column, supported only on
    Iceberg format v3" rather than silently creating a broken table.

    Complementary to test C (which succeeds by supplying ICEBERG_VERSION=3): this
    isolates the non-schematized fail-fast guard in ``createTableIfNotExists``.
    """
    import time

    base_name = "iceberg_nonschema_nov3"
    topic = create_topics([base_name], with_tables=False)[0]
    # Intentionally omit ICEBERG_VERSION so the connector hits the guard.
    connector = create_connector(
        v4_config=json_connector_config(
            topic,
            schematization=False,
            validation=False,
            table_type="iceberg",
            iceberg_create_table_options=f"EXTERNAL_VOLUME='{ICEBERG_EXTERNAL_VOLUME}'",
        )
    )
    driver.startConnectorWaitTime()

    driver.sendBytesData(
        topic,
        [json.dumps({"id": 0, "city": "Tokyo"}).encode("utf-8")],
        partition=0,
    )

    deadline = time.monotonic() + 120
    failed_tasks = []
    while time.monotonic() < deadline:
        failed_tasks = driver.get_failed_tasks(connector.name)
        if failed_tasks:
            break
        time.sleep(3)

    assert failed_tasks, (
        "Expected a FAILED task within 120s when table.type=iceberg + "
        "schematization=false + ICEBERG_VERSION omitted"
    )
    trace = failed_tasks[0].get("trace", "")
    assert "requires ICEBERG_VERSION=3" in trace, (
        f"Expected ERROR_0034 trace containing 'requires ICEBERG_VERSION=3', got:\n{trace}"
    )
    assert not _is_iceberg_table(driver, topic), (
        "Expected no iceberg table to be created when the v3 guard fires, "
        "but SHOW ICEBERG TABLES found one"
    )


# ---------------------------------------------------------------------------
# Test C: table.type=iceberg + schematization=false + ICEBERG_VERSION=3 -> happy path
# ---------------------------------------------------------------------------


@pytest.mark.iceberg
@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
def test_iceberg_autocreate_non_schematized_v3(
    driver: KafkaDriver,
    create_topics,
    create_connector,
    wait_for_rows,
):
    """table.type=iceberg + schematization=false + ICEBERG_VERSION=3 -> RECORD_CONTENT VARIANT.

    The non-schematized auto-create happy path: the connector creates the table with a
    RECORD_CONTENT VARIANT column (requires Iceberg format v3) and ingests each record
    into that column as a JSON blob.  A spot-check reads back RECORD_CONTENT:id to
    confirm data fidelity.

    This scenario is DISTINCT from every test in #1491, which exclusively uses
    schematization=True + server-side SE.  Here schematization is off and the table
    gets a single VARIANT data column rather than typed per-field columns.
    """
    base_name = "iceberg_nonschema_v3"
    topic = create_topics([base_name], with_tables=False)[0]
    table = IcebergTable(driver, topic)
    try:
        create_connector(
            v4_config=json_connector_config(
                topic,
                schematization=False,
                validation=False,
                table_type="iceberg",
                iceberg_create_table_options=(
                    f"EXTERNAL_VOLUME='{ICEBERG_EXTERNAL_VOLUME}' ICEBERG_VERSION=3"
                ),
            )
        )
        driver.startConnectorWaitTime()

        count = 30
        driver.sendBytesData(
            topic,
            [
                json.dumps({"id": i, "city": "Osaka"}).encode("utf-8")
                for i in range(count)
            ],
            partition=0,
        )
        wait_for_rows(topic, count)

        assert _is_iceberg_table(driver, topic), (
            f"Expected {topic} to be auto-created as an ICEBERG table"
        )
        cols = _desc(driver, topic)
        assert "RECORD_CONTENT" in cols, (
            f"Expected a RECORD_CONTENT column in the non-schematized auto-created "
            f"iceberg table, got columns: {list(cols.keys())}"
        )
        assert "VARIANT" in cols["RECORD_CONTENT"].upper(), (
            f"Expected RECORD_CONTENT to be VARIANT, got: {cols['RECORD_CONTENT']!r}"
        )
        assert table.select_scalar("COUNT(*)") == count, (
            f"Expected {count} rows, got {table.select_scalar('COUNT(*)')}"
        )
        # Spot-check: confirm data fidelity by reading back id=0 from RECORD_CONTENT.
        val = table.select_scalar(
            "RECORD_CONTENT:id::NUMBER",
            "WHERE RECORD_CONTENT:id::NUMBER = 0",
        )
        assert val == 0, (
            f"Expected RECORD_CONTENT:id::NUMBER = 0 for the first record, got {val!r}"
        )
    finally:
        table.drop()


# ---------------------------------------------------------------------------
# Test D: table.type=none + pre-existing iceberg table -> connector ingests as-is
# ---------------------------------------------------------------------------


@pytest.mark.iceberg
@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
def test_iceberg_none_existing_table_ingests(
    driver: KafkaDriver,
    create_iceberg_table,
    create_topics,
    create_connector,
    wait_for_rows,
):
    """table.type=none + pre-existing iceberg table -> connector uses it as-is.

    When ``table.type=none`` and the target table ALREADY EXISTS the connector must
    not create or modify it — it just resolves the pipe (creating it lazily if needed)
    and starts ingesting.  This proves the ``none`` guard is only a fail-fast for a
    MISSING table, not a blanket blocker for all ``none`` configurations.

    The pre-created table carries explicit columns (RECORD_METADATA VARIANT, ID NUMBER,
    CITY TEXT) and the connector uses:
      - snowflake.autocreate.table.type=none  (no CREATE statement issued)
      - schematization=True                    (maps JSON fields to named columns)
      - validation=server_side                 (server-side ingest)
    """
    base_name = "iceberg_none_existing"
    # Pre-create the iceberg table with the columns the connector will write into.
    # cleanup_topic=False: the topic is managed by create_topics below, not this fixture.
    table = create_iceberg_table(
        base_name,
        columns="(RECORD_METADATA VARIANT, ID NUMBER(19,0), CITY TEXT)",
        cleanup_topic=False,
    )
    # Topic name must match the table name (the connector resolves topic->table by default).
    topic = create_topics([base_name], with_tables=False)[0]

    create_connector(
        v4_config=json_connector_config(
            topic,
            schematization=True,
            validation=False,
            table_type="none",
        )
    )
    driver.startConnectorWaitTime()

    count = 30
    driver.sendBytesData(
        topic,
        [json.dumps({"id": i, "city": "Busan"}).encode("utf-8") for i in range(count)],
        partition=0,
    )
    wait_for_rows(table.name, count)

    landed = table.select_scalar("COUNT(*)")
    assert landed == count, (
        f"Expected {count} rows in the pre-existing iceberg table, got {landed}"
    )


# ---------------------------------------------------------------------------
# Test E: create-option passthrough — COMMENT is present on the auto-created table
# ---------------------------------------------------------------------------


@pytest.mark.iceberg
@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
def test_iceberg_autocreate_extra_create_option(
    driver: KafkaDriver,
    create_topics,
    create_connector,
    wait_for_rows,
):
    """Auto-create with an extra passthrough create option (COMMENT) is preserved.

    ``snowflake.iceberg.create.table.options`` may contain arbitrary SQL clauses that
    are spliced into the CREATE ICEBERG TABLE statement after the column list.  This
    test supplies a unique COMMENT string alongside the mandatory EXTERNAL_VOLUME and
    ICEBERG_VERSION=3, then asserts that the auto-created table carries that comment
    via INFORMATION_SCHEMA.TABLES.

    Choice rationale: COMMENT is a standard DDL property available on Iceberg tables,
    safe to combine with the metadata-only auto-create (no data column required at
    create time), and reliably queryable without ALTER TABLE.  CLUSTER BY was
    rejected because the auto-created table has no data column to cluster on at
    create time.  DATA_RETENTION_TIME_IN_DAYS is usable as a fallback but COMMENT is
    simpler to assert.
    """

    _AUTOCREATE_COMMENT = "kc-autocreate-e2e"

    base_name = "iceberg_autocreate_comment"
    topic = create_topics([base_name], with_tables=False)[0]
    table = IcebergTable(driver, topic)
    try:
        create_connector(
            v4_config=json_connector_config(
                topic,
                schematization=True,
                validation=False,
                table_type="iceberg",
                iceberg_create_table_options=(
                    f"EXTERNAL_VOLUME='{ICEBERG_EXTERNAL_VOLUME}' "
                    f"ICEBERG_VERSION=3 "
                    f"COMMENT='{_AUTOCREATE_COMMENT}'"
                ),
            )
        )
        driver.startConnectorWaitTime()

        count = 10
        driver.sendBytesData(
            topic,
            [json.dumps({"id": i}).encode("utf-8") for i in range(count)],
            partition=0,
        )
        wait_for_rows(topic, count)

        assert _is_iceberg_table(driver, topic), (
            f"Expected {topic} to be auto-created as an ICEBERG table"
        )
        # Assert the COMMENT was preserved. SHOW TABLES (DictCursor) exposes a
        # 'comment' column and includes managed-Iceberg tables (same source the
        # error_logging helper reads). Managed-Iceberg tables are TABLE_TYPE='BASE
        # TABLE' (IS_ICEBERG='YES') in INFORMATION_SCHEMA, so an 'EXTERNAL TABLE'
        # filter there would miss them — SHOW TABLES avoids that footgun.
        cursor = driver.snowflake_conn.cursor(DictCursor)
        rows = cursor.execute(f"SHOW TABLES LIKE '{topic}'").fetchall()
        assert rows, f"Expected SHOW TABLES to return a row for {topic}"
        comment_value = rows[0].get("comment")
        assert comment_value == _AUTOCREATE_COMMENT, (
            f"Expected auto-created table comment to be {_AUTOCREATE_COMMENT!r}, "
            f"got {comment_value!r}"
        )
    finally:
        table.drop()


# ---------------------------------------------------------------------------
# Test F: single topic with 3 partitions -> all partitions land in the auto-created table
# ---------------------------------------------------------------------------


@pytest.mark.iceberg
@pytest.mark.schema_evolution
@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
def test_iceberg_autocreate_multi_partition(
    driver: KafkaDriver,
    name_salt: str,
    create_connector,
    wait_for_rows,
):
    """Single topic, 3 partitions -> per-partition records all land in one auto-created table.

    This test is DISTINCT from #1491's ``test_iceberg_se_multi_channel_converge``, which
    is a multi-TOPIC fan-in (multiple topics via ``snowflake.topic2table.map`` converging
    to a single pre-existing table).  Here there is ONE topic with three partitions; the
    connector opens one SSv2 channel per partition and they all write to the single
    auto-created iceberg table.  The assertion checks per-partition completeness: the
    final row count must equal the sum of all records sent across every partition,
    proving that no partition's channel was starved or lost during the parallel
    auto-create race.
    """
    per_partition = 30
    num_partitions = 3
    total = per_partition * num_partitions

    base = f"iceberg_multipart{name_salt}"
    # Default topic->table mapping (no topic2table.map): the connector creates the
    # table under the topic's exact (mixed) case, so query by `base`, NOT base.upper()
    # (uppercasing would miss the case-sensitive table and select_number_of_records
    # returns None). base already carries name_salt, so it's unique per run.
    table_name = base
    # Create the topic with 3 partitions directly (bypassing create_topics fixture which
    # only supports 1 partition via default, and the topic name here must equal table_name
    # for the default topic->table mapping to work).
    driver.createTopics(base, partitionNum=num_partitions, replicationNum=1)
    table = IcebergTable(driver, table_name)
    try:
        connector = create_connector(
            v4_config=json_connector_config(
                base,
                schematization=True,
                validation=False,
                table_type="iceberg",
                iceberg_create_table_options=(
                    f"EXTERNAL_VOLUME='{ICEBERG_EXTERNAL_VOLUME}' ICEBERG_VERSION=3"
                ),
            )
        )
        driver.startConnectorWaitTime()

        # Send per_partition distinct records to each partition so we can verify
        # per-partition completeness (all offsets must land).
        for partition_idx in range(num_partitions):
            driver.sendBytesData(
                base,
                [
                    json.dumps({"pnum": partition_idx, "seq": seq}).encode("utf-8")
                    for seq in range(per_partition)
                ],
                partition=partition_idx,
            )

        wait_for_rows(table_name, total, connector_name=connector.name)

        assert _is_iceberg_table(driver, table_name), (
            f"Expected {table_name} to be auto-created as an ICEBERG table"
        )
        landed = table.select_scalar("COUNT(*)")
        assert landed == total, (
            f"Expected {total} rows across {num_partitions} partitions "
            f"({per_partition} each), got {landed}"
        )
    finally:
        table.drop()
        driver.deleteTopic(base)


# ---------------------------------------------------------------------------
# Test G: broken record -> DLQ; valid records land; task stays RUNNING
# ---------------------------------------------------------------------------


@pytest.mark.iceberg
@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
def test_iceberg_autocreate_broken_record_to_dlq(
    driver: KafkaDriver,
    name_salt: str,
    create_connector,
    wait_for_rows,
):
    """Auto-created iceberg table: broken (non-JSON) record goes to DLQ; task stays RUNNING.

    With ``errors.tolerance=all`` + a DLQ configured the connector must:
      1. Auto-create the iceberg table from the first valid record (schematization=True).
      2. Route the broken record to the DLQ topic without failing the task.
      3. Land all valid records in the iceberg table.

    This is DISTINCT from #1491's ``test_iceberg_se_ignore_tombstone``, which drops
    NULL/empty-string tombstones via ``behavior.on.null.values=IGNORE``.  Here we test
    a structurally corrupt payload (non-JSON bytes) that the value converter rejects,
    exercising the Kafka Connect ``errors.tolerance`` + DLQ machinery rather than the
    connector's tombstone-handling logic.

    DLQ config keys used:
      - errors.tolerance=all          (tolerate converter and transformation errors)
      - errors.deadletterqueue.topic.name=<dlq_topic>  (DLQ topic name)
      - errors.deadletterqueue.topic.replication.factor=1  (single-replica, test env)
    """

    valid_count = 20
    base = f"iceberg_dlq{name_salt}"
    # Default topic->table mapping (no topic2table.map): the connector creates the
    # table under the topic's exact (mixed) case, so query by `base`, NOT base.upper()
    # (uppercasing would miss the case-sensitive table and select_number_of_records
    # returns None). base already carries name_salt, so it's unique per run.
    table_name = base
    dlq_topic = f"{base}_dlq"

    driver.createTopics(base, partitionNum=1, replicationNum=1)
    table = IcebergTable(driver, table_name)
    try:
        connector = create_connector(
            v4_config={
                **json_connector_config(
                    base,
                    schematization=True,
                    validation=False,
                    table_type="iceberg",
                    iceberg_create_table_options=(
                        f"EXTERNAL_VOLUME='{ICEBERG_EXTERNAL_VOLUME}' ICEBERG_VERSION=3"
                    ),
                ),
                "errors.tolerance": "all",
                "errors.deadletterqueue.topic.name": dlq_topic,
                "errors.deadletterqueue.topic.replication.factor": "1",
            }
        )
        driver.startConnectorWaitTime()

        # Send valid JSON records first.
        driver.sendBytesData(
            base,
            [
                json.dumps({"id": i, "city": "Nairobi"}).encode("utf-8")
                for i in range(valid_count)
            ],
            partition=0,
        )
        # Then inject one structurally broken payload (non-JSON bytes).
        driver.sendBytesData(
            base,
            [b"{not json"],
            partition=0,
        )

        # Valid records must land in the iceberg table.
        wait_for_rows(table_name, valid_count, connector_name=connector.name)

        # The task must still be RUNNING (errors.tolerance=all; no task failure).
        status = driver.get_connector_status(connector.name)
        assert status is not None, "Could not query connector status"
        tasks = status.get("tasks", [])
        assert tasks, "Expected at least one task"
        failed = [t for t in tasks if t.get("state") == "FAILED"]
        assert not failed, (
            f"Expected all tasks to remain RUNNING with errors.tolerance=all, "
            f"but found FAILED tasks: {failed}"
        )

        # The broken record must appear in the DLQ.
        # consume_messages_dlq reads config["config"]["errors.deadletterqueue.topic.name"]
        # and polls until target_dlq_offset_number (0-based offset) is reached.
        # We sent exactly one broken record so we wait for offset 0.
        dlq_config = {
            "config": {
                "errors.deadletterqueue.topic.name": dlq_topic,
            }
        }
        dlq_count = driver.consume_messages_dlq(
            config=dlq_config,
            partition_no=0,
            target_dlq_offset_number=0,
        )
        assert dlq_count >= 1, (
            f"Expected at least 1 broken record in DLQ topic {dlq_topic!r}, "
            f"but consume_messages_dlq returned {dlq_count}"
        )
    finally:
        table.drop()
        driver.deleteTopic(base)
        driver.deleteTopic(dlq_topic)
