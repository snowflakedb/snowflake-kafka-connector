"""SPCS-only overrides. The host owns schema creation and independent verification."""

import os
from pathlib import Path

import pytest
import snowflake.connector

from lib.config import Profile
from lib.driver import KafkaDriver


@pytest.fixture(scope="session")
def credentials():
    if os.environ.get("SNOWFLAKE_RUNNING_INSIDE_SPCS", "").lower() != "true":
        pytest.fail("SPCS smoke must run inside a real SPCS job")
    return Profile(
        host=os.environ["SNOWFLAKE_HOST"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
        warehouse=os.environ["SPCS_QUERY_WAREHOUSE"],
        user="",
        role="",
        private_key="",
    )


@pytest.fixture(scope="session")
def driver(request, credentials):
    connection = snowflake.connector.connect(
        host=credentials.host,
        account=credentials.account,
        authenticator="oauth",
        token=Path("/snowflake/session/token").read_text().strip(),
        database=credentials.database,
        schema=credentials.schema,
        warehouse=credentials.warehouse,
        login_timeout=30,
        network_timeout=30,
        session_parameters={"STATEMENT_TIMEOUT_IN_SECONDS": 30},
    )
    instance = KafkaDriver(
        kafkaAddress=request.config.getoption("--kafka-address"),
        schemaRegistryAddress="",
        kafkaConnectAddress=request.config.getoption("--kafka-connect-address"),
        credentials=credentials,
        testVersion=request.config.getoption("--platform-version"),
        enableSSL=False,
        snowflake_connection=connection,
    )
    try:
        yield instance
    finally:
        instance.consumer.close()
        connection.close()
