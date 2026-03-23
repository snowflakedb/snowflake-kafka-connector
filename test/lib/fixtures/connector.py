from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
import logging
from typing import Dict, List
import pytest
from lib.driver import KafkaDriver
from lib.config_migration import v3_config_to_v4, v4_config_to_v3

logger = logging.getLogger(__name__)


@pytest.fixture
def create_topics(driver: KafkaDriver, name_salt):
    """Use for creating multiple topics and tables in parallel."""

    created_topics: List[str] = []
    created_tables: List[str] = []

    def _create_one(topic, num_partitions, replication_factor, with_table):
        salted = f"{topic}{name_salt}"
        logger.info(f"Creating topic {salted}")
        driver.createTopics(salted, num_partitions, replication_factor)
        created_topics.append(salted)
        if with_table:
            driver.create_table(salted)
            created_tables.append(salted)
        return salted

    def _create(
        topics: List[str], *, num_partitions=1, replication_factor=1, with_tables=True
    ):
        with ThreadPoolExecutor(max_workers=10) as executor:
            for t in topics:
                executor.submit(
                    _create_one, t, num_partitions, replication_factor, with_tables
                )
        return [f"{t}{name_salt}" for t in topics]

    try:
        yield _create
    finally:
        with ThreadPoolExecutor(max_workers=10) as executor:
            for _ in executor.map(driver.deleteTopic, created_topics):
                pass
            for _ in executor.map(driver.drop_table, created_tables):
                pass


@pytest.fixture()
def create_custom_connector(driver: KafkaDriver, name_salt: str):
    @dataclass
    class Connector:
        name: str
        config: Dict[str, str]

        def close(self, **kwargs):
            created.remove(self)
            return driver.closeConnector(self.name, **kwargs)

    created: List[Connector] = []

    def _create(
        unsalted_name: str,
        config_template: Dict[str, str],
    ) -> Connector:
        rest_request = driver.createConnector(
            name_salt=name_salt,
            unsalted_name=unsalted_name,
            config_template=config_template,
        )
        connector = Connector(name=rest_request["name"], config=rest_request["config"])
        created.append(connector)
        return connector

    try:
        yield _create
    finally:
        for connector in reversed(created):
            driver.closeConnector(connector.name)


@pytest.fixture
def create_connector(create_custom_connector, connector_version: str, request):
    test_name = request.node.originalname

    def _create(
        *,
        v3_config: dict[str, str] = None,
        v4_config: dict[str, str] = None,
    ):
        assert v3_config or v4_config
        assert not (v3_config and v4_config)
        config = None
        match (connector_version, v3_config, v4_config):
            case ("v3", _, None):
                config = v3_config
            case ("v3", None, _):
                config = v4_config_to_v3(v4_config)
            case ("v4", _, None):
                config = v3_config_to_v4(v3_config)
            case ("v4", None, _):
                config = v4_config
            case _:
                raise ValueError(f"Invalid connector version: {connector_version}")
        return create_custom_connector(test_name, config)

    return _create
