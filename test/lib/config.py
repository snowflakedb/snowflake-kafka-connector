from dataclasses import MISSING, asdict, dataclass, field
import json
import logging
from pathlib import Path
import re

from lib.crypto import parse_private_key


def _secret(default=MISSING):
    """Mark a secret field. repr=False keeps it out of the dataclass's
    auto-generated __repr__, so credentials can never reach logs, exception
    messages, or f-strings. Every new secret-bearing field MUST use this.
    Pass a default (e.g. None) for optional fields; omit it for required ones.
    """
    if default is MISSING:
        return field(repr=False)
    return field(default=default, repr=False)


@dataclass
class Profile:
    """Represents the SNOWFLAKE_CREDENTIAL_FILE (profile.json) content.

    Fields are consumed in two places in end-to-end tests:
    - Snowflake Python connector, used locally for validating data
    - Kafka Connect connector config

    The latter is filled from test-specific templates and sent to Kafka Connect.
    """

    protocol: str = None
    host: str = None
    port: int = None

    account: str = None
    user: str = None
    role: str = None
    private_key: str = _secret(None)

    database: str = None
    schema: str = None
    warehouse: str = None

    # Unused in end-to-end tests
    password: str = _secret(None)
    encrypted_private_key: str = _secret(None)
    private_key_passphrase: str = _secret(None)
    oauth_client_id: str = None
    oauth_client_secret: str = _secret(None)
    oauth_refresh_token: str = _secret(None)
    oauth_token_endpoint: str = None
    des_rsa_key: str = _secret(None)

    @staticmethod
    def load(path: Path) -> "Profile":
        with open(path) as f:
            return Profile(**json.load(f))

    def get_or_infer_account(self) -> str:
        if self.account is not None:
            return self.account

        reg = "[^/]*snowflakecomputing"  # find the account name
        account = re.findall(reg, self.host)
        if len(account) != 1 or len(account[0]) < 20:
            logging.warning(
                "Format error in 'host' field at profile.json, expecting account.snowflakecomputing.com:443"
            )
        return account[0][:-19]

    def make_url(self) -> str:
        protocol_prefix = f"{self.protocol}://" if self.protocol else ""
        port_suffix = f":{self.port}" if self.port else ""
        return protocol_prefix + self.host + port_suffix


@dataclass
class SnowflakeConnectorConfig:
    """Configuration for the Snowflake Python Connector used by the test driver to execute verification queries."""

    protocol: str
    host: str
    port: int

    account: str
    user: str
    private_key: bytes = _secret()

    database: str
    schema: str
    warehouse: str

    @staticmethod
    def from_profile(profile: Profile) -> "SnowflakeConnectorConfig":
        return SnowflakeConnectorConfig(
            protocol=profile.protocol,
            host=profile.host.split(":")[0],
            port=profile.port,
            account=profile.get_or_infer_account(),
            user=profile.user,
            private_key=parse_private_key(profile.private_key),
            database=profile.database,
            schema=profile.schema,
            warehouse=profile.warehouse,
        )

    def to_dict(self) -> dict:
        return {k: v for k, v in asdict(self).items() if v is not None}
