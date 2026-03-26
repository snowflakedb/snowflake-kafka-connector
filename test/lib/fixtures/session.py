import logging
import os
from pathlib import Path
import random
import string
import subprocess
import pytest
from lib.config import Profile, SnowflakeConnectorConfig
import snowflake

from lib.driver import KafkaDriver

logger = logging.getLogger(__name__)

_PROTO_DIR = Path(__file__).parents[2] / "test_data"


@pytest.fixture(scope="session")
def sensor_pb2():
    """Compile sensor.proto and return the generated module."""
    subprocess.run(
        ["protoc", "--python_out=.", "sensor.proto"],
        cwd=_PROTO_DIR,
        check=True,
    )
    import test_data.sensor_pb2

    return test_data.sensor_pb2


@pytest.fixture(scope="session")
def credentials_unsalted():
    """Load the credentials from the environment variable SNOWFLAKE_CREDENTIAL_FILE."""
    credential_path = Path(os.environ["SNOWFLAKE_CREDENTIAL_FILE"])
    assert credential_path.is_file(), (
        f"SNOWFLAKE_CREDENTIAL_FILE={credential_path} does not exist"
    )
    return Profile.load(credential_path)


@pytest.fixture(scope="session")
def session_name_salt(request):
    """Common name salt for all tests in this session."""
    salt = request.config.getoption("--name-salt")
    if salt is None:
        chars = string.ascii_uppercase + string.digits
        salt = "_" + "".join(random.choices(chars, k=7))
    logger.info(f"Using session name salt: {salt}")
    return salt


@pytest.fixture(scope="session")
def test_schema(credentials_unsalted, session_name_salt):
    """Create an isolated schema for this test session and drop it on teardown.

    The schema name is `<original_schema><session_name_salt>`.
    """
    original_schema = credentials_unsalted.schema
    salted_schema = f"{original_schema}{session_name_salt}"
    fqn = f"{credentials_unsalted.database}.{salted_schema}"

    conn_config = SnowflakeConnectorConfig.from_profile(credentials_unsalted)
    try:
        logger.info(f"Creating test schema: {fqn}")
        conn = snowflake.connector.connect(**conn_config.to_dict())
        conn.cursor().execute(f"CREATE SCHEMA IF NOT EXISTS {fqn}")
        yield salted_schema
    finally:
        logger.info(f"Dropping test schema: {fqn}")
        conn = snowflake.connector.connect(**conn_config.to_dict())
        conn.cursor().execute(f"DROP SCHEMA IF EXISTS {fqn} CASCADE")
        conn.close()


@pytest.fixture(scope="session")
def credentials(credentials_unsalted, test_schema):
    """Load the credentials from the environment variable SNOWFLAKE_CREDENTIAL_FILE and replaces the schema with its salted version.

    Mutating
    `credentials.schema` before the driver is built ensures that every
    Snowflake object (tables, pipes, channels) created by both the test
    harness and the Kafka connector lands in the throwaway schema.
    """
    credentials_unsalted.schema = test_schema
    return credentials_unsalted


@pytest.fixture(scope="session")
def driver(request, credentials):
    return KafkaDriver(
        kafkaAddress=request.config.getoption("--kafka-address"),
        schemaRegistryAddress=request.config.getoption("--schema-registry-address"),
        kafkaConnectAddress=request.config.getoption("--kafka-connect-address"),
        credentials=credentials,
        testVersion=request.config.getoption("--platform-version"),
        enableSSL=request.config.getoption("--enable-ssl"),
    )
