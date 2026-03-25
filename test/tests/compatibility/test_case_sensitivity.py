from dataclasses import dataclass
import json
from typing import Any, Optional

import pytest
from snowflake.connector import DictCursor

from lib.config_migration import V3_CONFIG_TEMPLATE
from lib.driver import KafkaDriver
from lib.fixtures.table import Table


@pytest.fixture
def case(connector_version: str):
    """Switches values depending on the connector version."""

    def _case(*, v3, v4):
        match connector_version:
            case "v3":
                return v3
            case "v4":
                return v4
            case _:
                raise ValueError(f"Unsupported connector version: {connector_version}")

    return _case


@pytest.mark.parametrize(
    "connector_version",
    [
        "v3",
        pytest.param(
            "v4",
            marks=pytest.mark.xfail(
                strict=True,
                reason="quoted/unicode topic2table cases are currently broken in compatibility mode",
            ),
        ),
    ],
    indirect=True,
)
def test_compatibility_case_sensitivity_table_name(
    driver: KafkaDriver,
    case,
    connector_version: str,
    create_connector,
    create_topics,
    name_salt: str,
    wait_for_rows,
):
    """Assert table name derived by the connector matches our expectations.

    Validates compatibility with KC v3, i.e. client-side validation is enabled.
    """

    @dataclass(frozen=True)
    class TableNameCase:
        case_name: str  # description
        unsalted_topic_name: str
        topic2table_value: Optional[str]
        expected_table_name: str

    test_cases = [
        # If no topic2table.map is provided, the table name is the same as the topic name.
        # NB the topic name is salted by the driver.
        TableNameCase("lower_a", "a", None, f"A{name_salt}"),
        TableNameCase("upper_b", "B", None, f"B{name_salt}"),
        TableNameCase("lower_c_mapped", "c_topic", f"c{name_salt}", f"C{name_salt}"),
        TableNameCase("upper_d_mapped", "D_topic", f"D{name_salt}", f"D{name_salt}"),
        *case(
            # KC v3 does not support:
            # - quoted table names in topic2table.map
            # - arbitrary unicode characters in topic2table.map
            v3=[],
            v4=[
                # TODO(skurella/alhuang): quoted table names and unicode characters fail in KC v4
                # - schema evolution looks up the table with "show tables like ? limit 1"
                # - I think it's passing the topic2table *before* quotes are removed?
                # - it doesn't find anything, or fails
                TableNameCase(
                    "lower_e_mapped_quoted",
                    "e_topic",
                    f'"e{name_salt}"',
                    f"e{name_salt}",
                ),
                TableNameCase(
                    "upper_f_mapped_quoted",
                    "f_topic",
                    f'"F{name_salt}"',
                    f"F{name_salt}",
                ),
                TableNameCase(
                    "unicode_mapped_quoted",
                    "g_topic",
                    f'"❄️{name_salt}"',
                    f"❄️{name_salt}",
                ),
            ],
        ),
    ]

    topics = create_topics(
        [test_case.unsalted_topic_name for test_case in test_cases], with_tables=False
    )

    topic2table_map = ",".join(
        f"{test_case.unsalted_topic_name}{name_salt}:{test_case.topic2table_value}"
        for test_case in test_cases
        if test_case.topic2table_value is not None
    )

    if connector_version == "v3" and topic2table_map == "":
        # In KC v3, topic2table.map cannot be empty.
        topic2table_map = None

    connector = create_connector(
        v3_config={
            key: value
            for key, value in {
                **V3_CONFIG_TEMPLATE,
                "topics": ",".join(topics),
                "tasks.max": "1",
                "key.converter": "org.apache.kafka.connect.storage.StringConverter",
                "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                "value.converter.schemas.enable": "false",
                "snowflake.enable.schematization": "true",
                "snowflake.topic2table.map": topic2table_map,
            }.items()
            if value is not None
        }
    )
    driver.startConnectorWaitTime()

    for test_case in test_cases:
        driver.sendBytesData(
            f"{test_case.unsalted_topic_name}{name_salt}",
            [json.dumps({"case_name": test_case.case_name}).encode("utf-8")],
        )

    for test_case in test_cases:
        expected_table = Table(driver, test_case.expected_table_name)
        wait_for_rows(expected_table.name, 1, connector_name=connector.name)

        tables = (
            driver.snowflake_conn.cursor(DictCursor).execute("show tables").fetchall()
        )
        assert test_case.expected_table_name in [table["name"] for table in tables]

        # Make sure it's the correct one, i.e. has the data we sent it.
        assert expected_table.select_scalar("CASE_NAME") == test_case.case_name


def test_compatibility_case_sensitivity_ingestion_columns(
    driver: KafkaDriver,
    case,
    create_connector,
    create_topics,
    create_table,
    wait_for_rows,
):
    @dataclass(frozen=True)
    class ColumnIngestionCase:
        case_name: str
        column_names: list[str]
        column_types: list[str]
        payload: dict[str, str]
        expected_values: list[Any]

    test_cases = [
        ColumnIngestionCase(
            case_name="upper_A",
            column_names=["A"],
            column_types=["VARCHAR"],
            payload={"A": "upper A"},
            expected_values=["upper A"],
        ),
        # In KC v3, unquoted key `a` is uppercased to `A`.
        # In KC v4, unquoted key `a` is left as is but is matched by the pipe.
        ColumnIngestionCase(
            case_name="lower_b_into_upper_B",
            column_names=["B"],
            column_types=["VARCHAR"],
            payload={"b": "lower b into upper B"},
            expected_values=["lower b into upper B"],
        ),
        # KC v4 requires columns to be quoted to pass client-side validation
        # and it doesn't process quoted column names when ingesting. This is a bug.
        *case(
            v3=[
                ColumnIngestionCase(
                    case_name="lower_c_into_lower_c",
                    column_names=["c"],
                    column_types=["VARCHAR"],
                    # KC v3 requires quotes to not uppercase the key.
                    payload={'"c"': "lower c into lower c"},
                    expected_values=["lower c into lower c"],
                ),
                # NB this only works in KC v4 because the schema validator checks the wrong column.
                # If we tried to ingest into columns "D" and "e", it would fail client-side validation.
                # TODO(skurella/alhuang): rename to `pair_D_e`
                ColumnIngestionCase(
                    case_name="pair_D_d",
                    column_names=["D", "d"],
                    column_types=["VARCHAR", "VARCHAR"],
                    payload={"D": "upper D", case(v3='"d"', v4="d"): "lower d"},
                    expected_values=["upper D", "lower d"],
                ),
            ],
            v4=[],
        ),
        ColumnIngestionCase(
            case_name="unicode",
            column_names=["❄️"],
            column_types=["VARCHAR"],
            # KC v3 doesn't support special characters in unquoted column names.
            payload={case(v3='"❄️"', v4="❄️"): "unicode ❄️"},
            expected_values=["unicode ❄️"],
        ),
        # We don't process keys beyond the first level.
        ColumnIngestionCase(
            case_name="variant",
            column_names=["V"],
            column_types=["VARIANT"],
            payload={"V": {"a": "b", "C": "D", '"e"': "❄️"}},
            expected_values=[{"a": "b", "C": "D", '"e"': "❄️"}],
        ),
    ]

    topics = create_topics(
        [test_case.case_name for test_case in test_cases],
        with_tables=False,
    )
    tables = [
        create_table(
            test_case.case_name.upper(),
            columns=(
                "("
                + ", ".join(
                    f'"{column_name}" {column_type}'
                    for column_name, column_type in zip(
                        test_case.column_names, test_case.column_types, strict=True
                    )
                )
                + ', "RECORD_METADATA" VARIANT)'
            ),
            cleanup_topic=False,
        )
        for test_case in test_cases
    ]

    connector = create_connector(
        v3_config={
            **V3_CONFIG_TEMPLATE,
            "topics": ",".join(topics),
            "tasks.max": "1",
            "key.converter": "org.apache.kafka.connect.storage.StringConverter",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter.schemas.enable": "false",
            "snowflake.enable.schematization": "true",
        }
    )
    driver.startConnectorWaitTime()

    for topic, test_case in zip(topics, test_cases, strict=True):
        driver.sendBytesData(topic, [json.dumps(test_case.payload).encode("utf-8")])

    for test_case, table in zip(test_cases, tables, strict=True):
        wait_for_rows(table.name, 1, connector_name=connector.name)

        actual_row = table.select("*")[0]
        for column_name, expected_value, column_type in zip(
            test_case.column_names,
            test_case.expected_values,
            test_case.column_types,
            strict=True,
        ):
            if column_type == "VARIANT":
                actual_value = json.loads(actual_row[column_name])
            else:
                actual_value = actual_row[column_name]
            assert actual_value == expected_value, (
                f"{test_case.case_name}.{column_name}: "
                f"expected {expected_value}, got {actual_value}"
            )
