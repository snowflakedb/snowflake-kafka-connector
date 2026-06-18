"""Iceberg schema evolution E2E tests — JSON format.

Tests server-side schema evolution (the connector sends rows with new columns
and the SSv2 server adds them via ALTER ICEBERG TABLE ADD COLUMN). Managed-Iceberg
SE is server-side only; the connector rejects client-side validation for
Iceberg + schema evolution, so all tests use ``validation=false``.

v4-only: v3 iceberg support was removed in v4.
"""

import json
import logging
import time

import pytest

from lib.driver import KafkaDriver, quote_name
from lib.fixtures.table import ICEBERG_EXTERNAL_VOLUME, IcebergTable
from tests.iceberg import json_connector_config, record_metadata_column

logger = logging.getLogger(__name__)


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


# Datatype-coverage payloads for the multi-topic SE test: two records whose union
# of fields exercises VARCHAR/NUMBER/BOOLEAN inference. PERF_GOLD_TYPES is the
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
@pytest.mark.schema_evolution
@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
@pytest.mark.parametrize("iceberg_version", [2, 3], ids=["v2", "v3"])
def test_iceberg_se_add_column(
    driver: KafkaDriver,
    create_iceberg_table,
    create_topics,
    create_connector,
    wait_for_rows,
    iceberg_version,
):
    """Iceberg schema evolution — server adds a new column mid-stream (server-side SE).

    Table starts with RECORD_METADATA VARIANT + CITY TEXT. Wave 1 records carry
    ``{city, age}``: the server detects AGE as new and issues
    ``ALTER ICEBERG TABLE ADD COLUMN``. Wave 2 adds ``country``: SE adds COUNTRY.

    Uses ``validation=false`` (server-side SE) — the supported mechanism for
    managed-Iceberg; the connector rejects client-side validation for Iceberg + SE.
    AGE/COUNTRY are primitive columns, which server-side SE supports.
    """
    base_name = "iceberg_se_addcol"
    table = create_iceberg_table(
        base_name,
        columns=f"({record_metadata_column(iceberg_version)}, CITY TEXT) ENABLE_SCHEMA_EVOLUTION = TRUE",
        cleanup_topic=False,
        iceberg_version=iceberg_version,
    )
    topic = create_topics([base_name], with_tables=False)[0]

    connector = create_connector(
        v4_config=json_connector_config(
            topic, schematization=True, validation=False, table_type="iceberg"
        )
    )
    driver.startConnectorWaitTime()

    wave1_count = 100
    driver.sendBytesData(
        topic,
        [
            json.dumps({"city": "Hsinchu", "age": i}).encode("utf-8")
            for i in range(wave1_count)
        ],
        partition=0,
    )
    wait_for_rows(table.name, wave1_count, connector_name=connector.name)

    cols = {row[0] for row in table.schema()}
    assert "AGE" in cols, (
        f"Expected connector SE to add AGE column after wave 1, got: {cols}"
    )

    wave2_count = 50
    driver.sendBytesData(
        topic,
        [
            json.dumps({"city": "Taipei", "age": 100 + i, "country": "TW"}).encode(
                "utf-8"
            )
            for i in range(wave2_count)
        ],
        partition=0,
    )
    wait_for_rows(table.name, wave1_count + wave2_count, connector_name=connector.name)

    rows = table.select('"CITY", "COUNTRY"', "WHERE \"CITY\" = 'Taipei' LIMIT 1")
    assert rows, "Expected at least one wave-2 row with CITY = 'Taipei'"
    assert rows[0]["CITY"] == "Taipei"
    assert rows[0]["COUNTRY"] == "TW", (
        f"Expected COUNTRY='TW', got {rows[0]['COUNTRY']!r}"
    )

    null_country_count = table.select("COUNT(*) AS CNT", 'WHERE "COUNTRY" IS NULL')[0][
        "CNT"
    ]
    assert null_country_count == wave1_count, (
        f"Expected {wave1_count} rows with NULL COUNTRY, got {null_country_count}"
    )


@pytest.mark.iceberg
@pytest.mark.schema_evolution
@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
@pytest.mark.parametrize("iceberg_version", [2, 3], ids=["v2", "v3"])
def test_iceberg_se_nullable_values_after_smt(
    driver: KafkaDriver,
    name_salt: str,
    create_iceberg_table,
    create_connector,
    wait_for_rows,
    iceberg_version,
):
    """SMT-produced nulls dropped; evolved column added as nullable.

    Server-side mirror of FDN ``test_se_nullable_values_after_smt``. An
    ExtractField$Value SMT pulls the ``optionalField`` sub-object; only every
    other event has it, so half the events become null and are dropped via
    behavior.on.null.values=IGNORE. INDEX is always present (stays NOT NULL);
    FROM_OPTIONAL_FIELD is evolved in as nullable BOOLEAN.
    """
    base_name = "iceberg_se_smt"
    table = create_iceberg_table(
        base_name,
        columns=f"({record_metadata_column(iceberg_version)}, INDEX NUMBER(19,0) NOT NULL) "
        "ENABLE_SCHEMA_EVOLUTION = TRUE",
        cleanup_topic=False,
        iceberg_version=iceberg_version,
    )
    topic = table.name
    driver.createTopics(topic, partitionNum=1, replicationNum=1)

    connector = create_connector(
        v4_config={
            **json_connector_config(
                topic, schematization=True, validation=False, table_type="iceberg"
            ),
            "behavior.on.null.values": "IGNORE",
            "transforms": "extractField",
            "transforms.extractField.type": "org.apache.kafka.connect.transforms.ExtractField$Value",
            "transforms.extractField.field": "optionalField",
        }
    )
    driver.startConnectorWaitTime()

    total_events = 200
    expected_rows = 100
    values = []
    for idx in range(total_events):
        event = {"index": idx, "someKey": "someValue"}
        if idx % 2 == 0:
            event["optionalField"] = {"INDEX": idx, "FROM_OPTIONAL_FIELD": True}
        values.append(json.dumps(event).encode("utf-8"))
    driver.sendBytesData(topic, values)

    wait_for_rows(table.name, expected_rows, connector_name=connector.name)

    desc = table.schema(as_dict=True)
    col_map = {row["name"]: row for row in desc}
    assert col_map["INDEX"]["null?"] == "N", "INDEX should remain NOT NULL"
    assert "FROM_OPTIONAL_FIELD" in col_map, (
        f"Expected evolved FROM_OPTIONAL_FIELD, got: {list(col_map.keys())}"
    )
    assert col_map["FROM_OPTIONAL_FIELD"]["null?"] == "Y", (
        "Evolved FROM_OPTIONAL_FIELD should be nullable"
    )
    assert col_map["FROM_OPTIONAL_FIELD"]["type"].startswith("BOOLEAN")


@pytest.mark.iceberg
@pytest.mark.schema_evolution
@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
def test_iceberg_se_ignore_tombstone(
    driver: KafkaDriver,
    name_salt: str,
    create_connector,
    wait_for_rows,
):
    """Two topics into one auto-created iceberg table; tombstones dropped.

    Server-side mirror of FDN ``test_se_json_ignore_tombstone``. Each topic sends
    (RECORD_COUNT - 2) real records plus a null and an empty-string tombstone;
    behavior.on.null.values=IGNORE drops both tombstones, so the expected row
    count is 2 * (RECORD_COUNT - 2). SE must still create every payload column.
    """
    record_count = 100
    base = f"iceberg_se_tombstone{name_salt}"
    table_name = base.upper()
    topics = [f"{base}{i}" for i in range(2)]
    for t in topics:
        driver.createTopics(t, partitionNum=1, replicationNum=1)

    connector = create_connector(
        v4_config=_multi_topic_iceberg_config(
            topics,
            table_name,
            create_options=f"EXTERNAL_VOLUME='{ICEBERG_EXTERNAL_VOLUME}'",
            extra={"behavior.on.null.values": "IGNORE"},
        )
    )
    table = IcebergTable(driver, table_name)
    try:
        driver.startConnectorWaitTime()

        for i, topic in enumerate(topics):
            real_count = record_count - 2
            keys = [
                json.dumps({"number": str(e)}).encode("utf-8")
                for e in range(real_count)
            ]
            values = [
                json.dumps(PERF_RECORDS[i]).encode("utf-8") for _ in range(real_count)
            ]
            keys.append(json.dumps({"number": str(real_count)}).encode("utf-8"))
            values.append(None)
            keys.append(json.dumps({"number": str(real_count + 1)}).encode("utf-8"))
            values.append(b"")
            driver.sendBytesData(topic, values, keys)

        expected_rows = len(topics) * (record_count - 2)
        wait_for_rows(table_name, expected_rows, connector_name=connector.name)

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
def test_iceberg_se_datatypes(driver, create_topics, create_connector, wait_for_rows):
    """Server-side SE infers the right Iceberg column type per JSON value type.

    Auto-create (default format v2) -> SE adds the typed data columns. JSON can
    express int/string/float/bool natively; logical date/timestamp types are
    covered by the Avro suite. Note Iceberg SE infers NUMBER(38,s) for decimal JSON
    floats (e.g. 1.5 -> NUMBER(38,1)); exponent-notation values (1e-10) infer FLOAT,
    mirroring the Snowfort rowset data_types matrix.
    """
    base_name = "iceberg_se_dtypes"
    topic = create_topics([base_name], with_tables=False)[0]
    table = IcebergTable(driver, topic)
    try:
        create_connector(
            v4_config=json_connector_config(
                topic,
                schematization=True,
                validation=False,
                table_type="iceberg",
                iceberg_create_table_options=f"EXTERNAL_VOLUME='{ICEBERG_EXTERNAL_VOLUME}'",
            )
        )
        driver.startConnectorWaitTime()
        count = 50
        driver.sendBytesData(
            topic,
            [
                json.dumps(
                    {
                        "i_int": i,
                        "s_str": "x",
                        "f_float": 1.5,
                        "f_exp": 1e-10,
                        "b_bool": True,
                    }
                ).encode("utf-8")
                for i in range(count)
            ],
            partition=0,
        )
        wait_for_rows(topic, count)
        cols = _desc(driver, topic)
        assert cols["I_INT"].startswith("NUMBER"), cols
        assert cols["S_STR"].startswith("VARCHAR"), cols
        assert cols["F_FLOAT"].startswith("NUMBER"), cols
        assert cols["F_EXP"].startswith("FLOAT"), cols
        assert cols["B_BOOL"].startswith("BOOLEAN"), cols
    finally:
        table.drop()


def _assert_se_rejects(
    driver, topic, connector, table, bad_payload, rejected, *, limit_sec=120
):
    """Assert server-side SE rejects ``rejected`` (a column name or an iterable of names)
    carried by ``bad_payload``: the offending column(s) are never added.

    KC cannot observe the SDK channel_status_code, and the rejection does not uniformly
    fail the sink task: a list -> untyped ARRAY escalates to a FAILED task, but VARIANT on a
    v2 table leaves the task RUNNING with the channel stuck. So neither a task failure nor
    "the column simply never appeared" is on its own a reliable rejection signal — the latter
    is indistinguishable from a dead/stuck data plane. To disambiguate, we first prove SE is
    LIVE on this exact table by evolving a known-good control column and waiting for it to be
    added; only then is the rejected column's sustained absence a genuine rejection. The
    control is sent BEFORE the bad payload so it lands before the rejection can stall the
    channel. (This is why the function never returns on a bare timeout: a silent timeout would
    pass even against a dead data plane.)
    """
    targets = (
        {rejected.upper()}
        if isinstance(rejected, str)
        else {c.upper() for c in rejected}
    )
    control_col = "SE_LIVENESS_PROBE"

    # 1. Liveness: a known-good STRING column SE must add (supported on every Iceberg version).
    #    Sent on the same channel as the bad payload, BEFORE it, so it commits before any
    #    rejection can stall the channel. If it never lands, SE/the data plane is dead and a
    #    rejection cannot be concluded -> fail loudly rather than passing silently.
    driver.sendBytesData(
        topic,
        [json.dumps({"id": 0, control_col.lower(): "ok"}).encode("utf-8")],
        partition=0,
    )
    live_deadline = time.monotonic() + limit_sec
    while control_col not in set(_desc(driver, table.name)):
        for failed in driver.get_failed_tasks(connector.name):
            raise AssertionError(
                f"connector task failed before the SE-liveness control column "
                f"{control_col} was added, so rejection of {sorted(targets)} cannot be "
                f"tested; trace:\n{failed.get('trace')}"
            )
        assert time.monotonic() < live_deadline, (
            f"SE-liveness control column {control_col} was never added within {limit_sec}s; "
            f"SE/the data plane appears dead, so the absence of {sorted(targets)} does not "
            f"prove a rejection"
        )
        time.sleep(3)

    # 2. SE is provably live on this table. Send the bad payload and require the rejected
    #    column(s) to stay absent for the full window.
    driver.sendBytesData(topic, [json.dumps(bad_payload).encode("utf-8")], partition=0)
    deadline = time.monotonic() + limit_sec
    while time.monotonic() < deadline:
        leaked = targets & set(_desc(driver, table.name))
        assert not leaked, (
            f"Expected SE to reject {sorted(targets)}, but these were added: {sorted(leaked)}"
        )
        for failed in driver.get_failed_tasks(connector.name):
            trace = (failed.get("trace") or "").upper()
            if any(k in trace for k in ("SCHEMA_EVOLUTION", "DATATYPE", "ERR_CHANNEL")):
                return  # task failure caused by the SE rejection -> confirmed (ARRAY/list case)
            raise AssertionError(
                f"connector task failed for a reason unrelated to the SE rejection "
                f"of {sorted(targets)}; trace:\n{failed.get('trace')}"
            )
        time.sleep(3)
    # Liveness was proven (control column added) AND the rejected column(s) stayed absent for
    # the full window while SE was demonstrably working -> genuine rejection.


@pytest.mark.iceberg
@pytest.mark.schema_evolution
@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
@pytest.mark.parametrize("iceberg_version", [2, 3], ids=["v2", "v3"])
def test_iceberg_se_nested_json_variant(
    driver: KafkaDriver,
    create_iceberg_table,
    create_topics,
    create_connector,
    wait_for_rows,
    iceberg_version,
):
    """Nested JSON object -> VARIANT data column via server-side SE.

    Mirrors Snowfort ``test_iceberg_schema_evolution_nested_json_object``: v3 accepts
    the VARIANT column (added, row commits); v2 rejects it (VARIANT-in-Iceberg requires
    format v3 -> ICEBERG_DATATYPE_NOT_SUPPORTED, column not added, rows don't land).
    Unlike TIMESTAMP_TZ / sub-microsecond scale, a nested JSON object IS expressible
    through the Kafka Connect JSON converter, so this v2-reject / v3-accept split is
    reachable end-to-end through KC.
    """
    base_name = "iceberg_se_variant"
    table = create_iceberg_table(
        base_name,
        columns=f"({record_metadata_column(iceberg_version)}, ID NUMBER(19,0)) "
        "ENABLE_SCHEMA_EVOLUTION = TRUE",
        cleanup_topic=False,
        iceberg_version=iceberg_version,
    )
    topic = create_topics([base_name], with_tables=False)[0]
    connector = create_connector(
        v4_config=json_connector_config(
            topic, schematization=True, validation=False, table_type="iceberg"
        )
    )
    driver.startConnectorWaitTime()

    nested = {"nested_obj": {"a": 1, "b": "two"}}
    if iceberg_version == 3:
        count = 10
        driver.sendBytesData(
            topic,
            [json.dumps({"id": i, **nested}).encode("utf-8") for i in range(count)],
            partition=0,
        )
        wait_for_rows(table.name, count, connector_name=connector.name)
        cols = _desc(driver, table.name)
        assert "NESTED_OBJ" in cols, cols
        assert "VARIANT" in cols["NESTED_OBJ"].upper(), (
            f"Expected NESTED_OBJ to be VARIANT on v3, got {cols.get('NESTED_OBJ')!r}"
        )
    else:
        _assert_se_rejects(
            driver, topic, connector, table, {"id": 1, **nested}, "NESTED_OBJ"
        )


@pytest.mark.iceberg
@pytest.mark.schema_evolution
@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
@pytest.mark.parametrize(
    "col_name",
    ["empty_arr", "tags", "scores", "list_of_structs"],
    ids=["empty", "list_of_strings", "list_of_ints", "list_of_structs"],
)
def test_iceberg_se_list_rejected(
    driver: KafkaDriver,
    create_iceberg_table,
    create_topics,
    create_connector,
    col_name,
):
    """JSON list value -> rejected by server-side SE on every Iceberg version.

    Mirrors Snowfort ``test_iceberg_schema_evolution_list_value_rejected``: streaming
    type inference emits an untyped semi ``ARRAY`` (not ``STRUCTURED_ARRAY``), which SE
    rejects -> ICEBERG_DATATYPE_NOT_SUPPORTED ("ARRAY"). Uses a **v3** table so the
    rejection isolates the ARRAY gap (SNOW-3053493), not the v2 VARIANT restriction.
    This is the case that blocks real KC -> Iceberg accounts that ingest list/struct
    DATA columns; expressible through KC, so it is exercised end-to-end here.
    """
    # Resolve the list value from the id here rather than parametrizing on it:
    # name_salt builds a per-variant discriminator from the first character of
    # each parametrize value, and a list repr (e.g. "[") is an invalid Kafka
    # topic character -> the consumer crashes with InvalidTopicException instead
    # of exercising the ARRAY rejection.
    value = {
        "empty_arr": [],
        "tags": ["red", "blue", "green"],
        "scores": [10, 20, 30],
        "list_of_structs": [{"k": 1}, {"k": 2}],
    }[col_name]
    base_name = "iceberg_se_list"
    table = create_iceberg_table(
        base_name,
        columns="(RECORD_METADATA VARIANT, ID NUMBER(19,0)) ENABLE_SCHEMA_EVOLUTION = TRUE",
        cleanup_topic=False,
        iceberg_version=3,
    )
    topic = create_topics([base_name], with_tables=False)[0]
    connector = create_connector(
        v4_config=json_connector_config(
            topic, schematization=True, validation=False, table_type="iceberg"
        )
    )
    driver.startConnectorWaitTime()

    _assert_se_rejects(
        driver, topic, connector, table, {"id": 1, col_name: value}, col_name
    )


@pytest.mark.iceberg
@pytest.mark.schema_evolution
@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
def test_iceberg_se_multi_channel_converge(
    driver: KafkaDriver,
    create_iceberg_table,
    create_topics,
    create_connector,
    wait_for_rows,
):
    """Concurrent server-side SE across multiple channels converges on one table.

    Three topics fan into a single pre-created Iceberg table (one channel per
    topic), each sending records that introduce the SAME new column AGE. SE must
    add AGE once and land every row. Mirrors Snowfort
    ``test_schema_evolution_multiple_channels_same_column``.
    """
    # Upper-case the table name: the connector resolves ``topic2table.map``
    # targets as unquoted identifiers (normalized to upper case), so a
    # lower-case pre-created table is bypassed and the connector auto-creates a
    # separate upper-cased one that these quoted (case-sensitive) queries never
    # see. Mirrors ``test_iceberg_se_avro_sr_multi_schema``.
    base = "iceberg_se_multichan"
    topics = create_topics([f"{base}_t{i}" for i in range(3)], with_tables=False)
    table = create_iceberg_table(
        base.upper(),
        columns="(RECORD_METADATA VARIANT, CITY TEXT) ENABLE_SCHEMA_EVOLUTION = TRUE",
        cleanup_topic=False,
        iceberg_version=3,
    )
    connector = create_connector(
        v4_config=_multi_topic_iceberg_config(topics, table.name)
    )
    driver.startConnectorWaitTime()

    per_topic = 30
    for t in topics:
        driver.sendBytesData(
            t,
            [
                json.dumps({"city": "Hsinchu", "age": i}).encode("utf-8")
                for i in range(per_topic)
            ],
            partition=0,
        )

    total = per_topic * len(topics)
    wait_for_rows(table.name, total, connector_name=connector.name)
    cols = {row[0] for row in table.schema()}
    assert "AGE" in cols, (
        f"Expected server-side SE to add AGE once across the {len(topics)} "
        f"converging channels, got: {sorted(cols)}"
    )


@pytest.mark.iceberg
@pytest.mark.schema_evolution
@pytest.mark.skip(
    reason=(
        "TIMESTAMP_TZ / nanosecond (scale-9) TIME-TIMESTAMP rejection on v2 Iceberg"
        " ADD COLUMN (SNOW-3647727) is not expressible through the Kafka Connector"
        " ingestion path: Kafka Connect converters cannot produce a TIMESTAMP_TZ"
        " value or sub-microsecond precision (logical types top out at micros, with"
        " no tz-aware type), so server-side SE never attempts to add such a column"
        " via KC. These rejections are covered at the Snowfort/rowset layer where the"
        " column type is constructed directly. (The other documented rejections that"
        " ARE KC-expressible -- VARIANT on v2 and list/ARRAY -- are covered by"
        " test_iceberg_se_nested_json_variant and test_iceberg_se_list_rejected.)"
    )
)
def test_iceberg_se_type_rejection_not_kc_expressible():
    pass


@pytest.mark.iceberg
@pytest.mark.schema_evolution
@pytest.mark.skip(
    reason=(
        "FDN test_se_replace_table / test_se_multi_topic_replace_table are"
        " @parametrize(connector_version=['v3'])-only: CREATE OR REPLACE TABLE"
        " mid-stream invalidates streaming channels and the SSv2 SDK does not"
        " surface pipe invalidation through isClosed(). Managed Iceberg is"
        " SSv2-only, so the recover-and-re-evolve scenario is structurally"
        " untestable here."
    )
)
def test_iceberg_se_replace_table_v3_only():
    pass


@pytest.mark.iceberg
@pytest.mark.schema_evolution
@pytest.mark.skip(
    reason=(
        "Snowfort test_schema_evolution_rollback toggles the server-side SE feature"
        " flag/param off mid-stream and asserts new columns stop being added. There is"
        " no connector-level lever for this: managed-Iceberg SE is server-side, driven"
        " by the table's ENABLE_SCHEMA_EVOLUTION and the GS param -- the connector"
        " never issues the ALTER, so it can neither enable nor disable evolution"
        " mid-stream. Whether disabling routes new-column rows to the error table is"
        " GS-side behavior, covered by the Snowfort/rowset rollback test, outside this"
        " PR's connector-wiring scope."
    )
)
def test_iceberg_se_disabled_mid_stream_server_side_na():
    pass
