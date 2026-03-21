from dataclasses import dataclass
import json
from typing import Optional

from snowflake.connector import DictCursor

from lib.config_migration import V3_CONFIG_TEMPLATE
from lib.driver import KafkaDriver
from lib.fixtures.table import Table


def test_case_sensitivity_table_name(
    driver: KafkaDriver,
    connector_version: str,
    create_connector,
    create_topics,
    name_salt,
    wait_for_rows,
):
    """Assert table name derived by the connector matches our expectations."""

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
    ]
    if connector_version == "v4":
        # KC v3 does not support:
        # - quoted table names in topic2table.map
        # - arbitrary unicode characters in topic2table.map
        test_cases.extend(
            [
                # TODO(skurella/alhuang): quoted table names and unicode characters fail in KC v4
                # - schema evolution looks up the table with "show tables like ? limit 1"
                # - I think it's passing the topic2table *before* quotes are removed?
                # - it doesn't find anything, or fails
                # TableNameCase("lower_e_mapped_quoted", "e_topic", f'"e{name_salt}"', f"e{name_salt}"),
                # TableNameCase("upper_f_mapped_quoted", "f_topic", f'"F{name_salt}"', f"F{name_salt}"),
                # TableNameCase("unicode_mapped", "g_topic", f"❄️{name_salt}", f"❄️{name_salt}"),
                # TableNameCase("unicode_mapped_quoted", "h_topic", f'"❄️{name_salt}"', f"❄️{name_salt}"),
            ]
        )

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
