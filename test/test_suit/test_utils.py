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
    if len(pk_passphrase) != 0:
        pkpass = pk_passphrase.encode()

    # remove header, footer, and line breaks
    pk = re.sub("-+[A-Za-z ]+-+", "", pk)
    pk = re.sub("\\s", "", pk)

    pkBuilder = ""
    pkBuilder += "-----BEGIN ENCRYPTED PRIVATE KEY-----"
    for i, c in enumerate(pk):
        if i % 64 == 0:
            pkBuilder += "\n"
        pkBuilder += c
    pkBuilder += "\n-----END ENCRYPTED PRIVATE KEY-----"

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
