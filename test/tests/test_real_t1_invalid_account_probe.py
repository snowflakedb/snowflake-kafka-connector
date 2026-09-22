"""Temporary probe for the SDK status exposed by a real Snowflake T1 404.

This test intentionally fails after collecting connector state. Remove it after
one conclusive CI run.
"""

import os
import time
import uuid

import pytest
from lib.config_migration import V4_CONFIG_TEMPLATE

PROBE_WAIT_S = 20


@pytest.mark.correctness
@pytest.mark.skipif(
    os.environ.get("KAFKA_PLATFORM") != "apache",
    reason="temporary probe runs once on the direct-network Apache lane",
)
@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
def test_real_t1_invalid_account_probe(
    driver,
    name_salt,
    create_connector,
):
    """Send hostname discovery directly to T1 using a nonexistent account."""
    topic = f"test_real_t1_invalid_account_probe{name_salt}"
    driver.createTopics(topic, partitionNum=1, replicationNum=1)

    invalid_host = f"kc-nr-probe-{uuid.uuid4().hex}.snowflakecomputing.com"
    config = {
        **V4_CONFIG_TEMPLATE,
        "topics": topic,
        "tasks.max": "1",
        "snowflake.url.name": invalid_host,
        "snowflake.validation": "client_side",
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
    }

    connector = create_connector(v4_config=config)
    started = time.monotonic()
    failed_tasks = []
    while time.monotonic() - started < PROBE_WAIT_S:
        failed_tasks = driver.get_failed_tasks(connector.name)
        if failed_tasks:
            break
        time.sleep(1)

    elapsed = time.monotonic() - started
    status = driver.get_connector_status(connector.name)
    pytest.fail(
        "REAL_T1_INVALID_ACCOUNT_PROBE "
        f"host={invalid_host} elapsed_s={elapsed:.1f} "
        f"failed_tasks={failed_tasks} status={status}"
    )
