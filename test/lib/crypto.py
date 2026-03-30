import base64
import hashlib
import re
import textwrap
import time

import jwt
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization


def normalize_private_key(private_key: str, is_encrypted) -> bytes:
    """Accepts a private key string and returns a normalized PEM-encoded private key."""

    # Remove header, footer, and line breaks.
    private_key = re.sub("-+[A-Za-z ]+-+", "", private_key)
    private_key = re.sub("\\s", "", private_key)

    if is_encrypted:
        header = "-----BEGIN ENCRYPTED PRIVATE KEY-----"
        footer = "-----END ENCRYPTED PRIVATE KEY-----"
    else:
        header = "-----BEGIN PRIVATE KEY-----"
        footer = "-----END PRIVATE KEY-----"

    # Group in lines of 64 characters, append header and footer.
    return "\n".join([header, *textwrap.wrap(private_key, 64), footer]).encode()


def parse_private_key(private_key_str: str, password_str: str | None = None) -> bytes:
    password: bytes | None = password_str.encode("ascii") if password_str else None

    private_key_normalized: bytes = normalize_private_key(
        private_key_str, password is not None
    )

    private_key = serialization.load_pem_private_key(
        private_key_normalized, password=password, backend=default_backend()
    )

    return private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def make_snowflake_jwt(account: str, user: str, private_key_str: str) -> str:
    """Build a Snowflake-compatible JWT for the streaming ingest API.

    Args:
        account: Snowflake account identifier (e.g. "myaccount").
        user: Snowflake user name.
        private_key_str: Raw private key string (PEM content without headers).

    Returns:
        Signed JWT string suitable for Authorization: Bearer header.
    """
    pem_bytes = normalize_private_key(private_key_str, is_encrypted=False)
    private_key = serialization.load_pem_private_key(
        pem_bytes, password=None, backend=default_backend()
    )

    # Snowflake JWT issuer requires SHA256 fingerprint of the public key DER
    public_key_der = private_key.public_key().public_bytes(
        serialization.Encoding.DER,
        serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    fp = base64.standard_b64encode(hashlib.sha256(public_key_der).digest()).decode()

    account_upper = account.upper()
    user_upper = user.upper()
    now = int(time.time())

    payload = {
        "iss": f"{account_upper}.{user_upper}.SHA256:{fp}",
        "sub": f"{account_upper}.{user_upper}",
        "iat": now,
        "exp": now + 3600,
    }
    return jwt.encode(payload, private_key, algorithm="RS256")
