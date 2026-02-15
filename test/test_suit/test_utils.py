import re
import textwrap
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization


class Error(Exception):
    """Base class for other exceptions"""
    pass


class ResetAndRetry(Error):
    """Raised when we want to reset the retry count"""

    def __init__(self, msg=""):
        self.msg = msg


class RetryableError(Error):
    """Raised when we can retry"""

    def __init__(self, msg=""):
        self.msg = msg


class NonRetryableError(Error):
    """Raised when we cannot retry"""

    def __init__(self, msg=""):
        self.msg = msg


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
    password: bytes | None = password_str.encode('ascii') if password_str else None

    private_key_normalized: bytes = normalize_private_key(private_key_str, password is not None)

    private_key = serialization.load_pem_private_key(
        private_key_normalized,
        password=password,
        backend=default_backend()
    )

    return private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption())
