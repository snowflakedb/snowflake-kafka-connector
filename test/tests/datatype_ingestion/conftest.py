import json
import logging
import time
from pathlib import Path

import pytest
import requests

from lib.config_migration import v4_config_to_v3

logger = logging.getLogger(__name__)

TEMPLATE_DIR = Path("rest_request_template")
BASE_TEMPLATE = "datatype_ingestion.json"


def _replace_values(obj, replacements):
    """Recursively replace placeholder strings in a parsed JSON object."""
    if isinstance(obj, dict):
        return {k: _replace_values(v, replacements) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_replace_values(item, replacements) for item in obj]
    elif isinstance(obj, str):
        for old, new in replacements.items():
            obj = obj.replace(old, new)
        return obj
    return obj


@pytest.fixture(params=["v3", "v4-compat"])
def ingestion_mode(request):
    """Two ingestion modes under test.

    - v3:        SnowflakeSinkConnector + SNOWPIPE_STREAMING
    - v4-compat: SnowflakeStreamingSinkConnector + client.validation.enabled=true
    """
    return request.param


@pytest.fixture
def mode_name_salt(session_name_salt, ingestion_mode):
    """Name salt diversified by ingestion mode to avoid connector/table collisions."""
    suffix = {"v3": "_v3", "v4-compat": ""}[ingestion_mode]
    return f"{session_name_salt}{suffix}"


@pytest.fixture
def typed_table(driver, mode_name_salt):
    """Factory: create a Snowflake table + Kafka topic for a data-type test.

    Returns the topic/table name (same string, so the connector auto-maps).
    """
    created = []

    def _create(test_id, col_ddl):
        topic = f"{test_id}{mode_name_salt}"
        driver.snowflake_conn.cursor().execute(
            f"CREATE OR REPLACE TABLE {topic} "
            f"(VALUE_COL {col_ddl}, RECORD_METADATA VARIANT)"
        )
        driver.createTopics(topic, partitionNum=1, replicationNum=1)
        created.append(topic)
        return topic

    try:
        yield _create
    finally:
        for t in created:
            try:
                driver.deleteTopic(t)
            except Exception:
                pass


@pytest.fixture
def create_typed_connector(driver, mode_name_salt, ingestion_mode):
    """Factory: register a connector for the current ingestion mode.

    Builds the connector config in memory from the base template, applies
    mode-specific transforms, and registers via the Kafka Connect REST API.
    Each call uses the *test_id* to derive a unique connector/topic name.

    Returns the generated config dict.
    """
    created = []
    http_headers = {"Content-type": "application/json", "Accept": "application/json"}

    def _create(test_id, *, extra_config=None):
        base = json.loads((TEMPLATE_DIR / BASE_TEMPLATE).read_text())

        connector_name = f"{test_id}{mode_name_salt}"
        config = _replace_values(
            base,
            {
                "SNOWFLAKE_HOST": driver.credentials.make_url(),
                "SNOWFLAKE_DATABASE": driver.credentials.database,
                "SNOWFLAKE_SCHEMA": driver.credentials.schema,
                "SNOWFLAKE_USER": driver.credentials.user,
                "SNOWFLAKE_ROLE": driver.credentials.role,
                "SNOWFLAKE_PRIVATE_KEY": driver.credentials.private_key,
                "CONFLUENT_SCHEMA_REGISTRY": driver.schemaRegistryAddress,
                "SNOWFLAKE_TEST_TOPIC": connector_name,
                "SNOWFLAKE_CONNECTOR_NAME": connector_name,
                "_NAME_SALT": mode_name_salt,
            },
        )

        match ingestion_mode:
            case "v3":
                config["config"] = v4_config_to_v3(config["config"])
            case "v4-compat":
                config["config"]["snowflake.client.validation.enabled"] = "true"

        if extra_config:
            config["config"].update(extra_config)

        gen_path = Path("rest_request_generated")
        gen_path.mkdir(parents=True, exist_ok=True)
        (gen_path / f"{test_id}.json").write_text(json.dumps(config, indent=4))

        delete_url = f"http://{driver.kafkaConnectAddress}/connectors/{connector_name}"
        post_url = f"http://{driver.kafkaConnectAddress}/connectors"

        logger.info("Delete request: %s", delete_url)
        try:
            code = requests.delete(delete_url, timeout=10).status_code
            logger.info("Delete request returned: %d", code)
        except Exception:
            pass

        logger.info("POST connector: %s", connector_name)
        r = requests.post(post_url, json=config, headers=http_headers)
        logger.info("POST response: %d", r.status_code)
        if not r.ok:
            time.sleep(10)
            r = requests.post(post_url, json=config, headers=http_headers)
            if not r.ok:
                raise RuntimeError(
                    f"Failed to create connector {connector_name}: "
                    f"{r.status_code} {r.text}"
                )

        created.append(connector_name)
        return config

    try:
        yield _create
    finally:
        for name in reversed(created):
            url = f"http://{driver.kafkaConnectAddress}/connectors/{name}"
            logger.info("=== Delete connector %s ===", name)
            try:
                code = requests.delete(url, timeout=10).status_code
                logger.info("Delete response code: %d", code)
            except Exception:
                logger.warning("Failed to delete connector %s", name, exc_info=True)
