import re
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


def parsePrivateKey(pk, pk_passphrase):
    pkpass = None
    is_encrypted = len(pk_passphrase) != 0
    if is_encrypted:
        pkpass = pk_passphrase.encode()

    # remove header, footer, and line breaks
    pk = re.sub("-+[A-Za-z ]+-+", "", pk)
    pk = re.sub("\\s", "", pk)

    pkBuilder = ""
    if is_encrypted:
        pkBuilder += "-----BEGIN ENCRYPTED PRIVATE KEY-----"
    else:
        pkBuilder += "-----BEGIN PRIVATE KEY-----"
    for i, c in enumerate(pk):
        if i % 64 == 0:
            pkBuilder += "\n"
        pkBuilder += c
    if is_encrypted:
        pkBuilder += "\n-----END ENCRYPTED PRIVATE KEY-----"
    else:
        pkBuilder += "\n-----END PRIVATE KEY-----"

    p_key = serialization.load_pem_private_key(
        pkBuilder.encode(),
        password=pkpass,
        backend=default_backend()
    )

    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption())

    return pkb
