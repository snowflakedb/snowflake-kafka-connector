"""E2E test: ingest with OAuth-only auth (no private key in the connector config).

The connector authenticates to Snowflake using OAuth credentials from
profile.json (client id/secret + refresh token) instead of a private key. This
mirrors the only OAuth grant KC v3 supported (refresh_token).

Verification queries still run over the harness's own private-key Snowflake
connection, which is independent of how the connector authenticates.
"""

import logging

import pytest

from lib.config_migration import V4_CONFIG_TEMPLATE
from lib.utils import RecordProducer

logger = logging.getLogger(__name__)

RECORD_COUNT = 100


@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
def test_oauth_ingestion(
    connector_version,
    driver,
    credentials,
    name_salt,
    create_connector,
    wait_for_rows,
):
    if not (
        credentials.oauth_client_id
        and credentials.oauth_client_secret
        and credentials.oauth_refresh_token
        and credentials.oauth_token_endpoint
    ):
        pytest.fail("OAuth credentials not present in profile.json")

    topic = f"test_oauth_ingestion{name_salt}"
    table_name = topic

    # Build a v4 connector config that authenticates via OAuth only.
    config = {
        k: v for k, v in V4_CONFIG_TEMPLATE.items() if k != "snowflake.private.key"
    }
    config |= {
        "topics": topic,
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
        "snowflake.authenticator": "oauth",
        "snowflake.oauth.client.id": credentials.oauth_client_id,
        "snowflake.oauth.client.secret": credentials.oauth_client_secret,
        "snowflake.oauth.refresh.token": credentials.oauth_refresh_token,
        "snowflake.oauth.token.endpoint": credentials.oauth_token_endpoint,
    }
    connector = create_connector(v4_config=config)
    driver.wait_for_connector_running(connector.name)

    RecordProducer(driver, topic).send(RECORD_COUNT)
    wait_for_rows(table_name, RECORD_COUNT, connector_name=connector.name)
