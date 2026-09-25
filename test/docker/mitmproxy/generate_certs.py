#!/usr/bin/env python3
"""Generate the test-only mitmproxy CA and leaf into /certs.

The leaf SAN covers both Docker aliases (mitmproxy, mitmproxy-subdomain).
mitmdump --certs wants the leaf cert and private key concatenated as PEM.
The Connect worker trusts the CA via SSL_CERT_FILE on the shared volume.
"""

from datetime import datetime, timedelta, timezone
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

CERTS_DIR = Path("/certs")
CA_CERT_PATH = CERTS_DIR / "mitmproxy-ca.crt"
LEAF_PEM_PATH = CERTS_DIR / "mitmproxy.pem"
VALIDITY = timedelta(days=3650)


def _key_usage(**kwargs):
    defaults = dict(
        digital_signature=False,
        content_commitment=False,
        key_encipherment=False,
        data_encipherment=False,
        key_agreement=False,
        key_cert_sign=False,
        crl_sign=False,
        encipher_only=False,
        decipher_only=False,
    )
    defaults.update(kwargs)
    return x509.KeyUsage(**defaults)


def main():
    CERTS_DIR.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc)

    ca_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "mitmproxy-e2e-ca")])
    ca_cert = (
        x509.CertificateBuilder()
        .subject_name(ca_name)
        .issuer_name(ca_name)
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now)
        .not_valid_after(now + VALIDITY)
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .add_extension(_key_usage(key_cert_sign=True, crl_sign=True), critical=True)
        .sign(ca_key, hashes.SHA256())
    )

    leaf_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    leaf_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "mitmproxy-e2e")])
    leaf_cert = (
        x509.CertificateBuilder()
        .subject_name(leaf_name)
        .issuer_name(ca_name)
        .public_key(leaf_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now)
        .not_valid_after(now + VALIDITY)
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(_key_usage(digital_signature=True, key_encipherment=True), critical=True)
        .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), critical=False)
        .add_extension(
            x509.SubjectAlternativeName(
                [x509.DNSName("mitmproxy"), x509.DNSName("mitmproxy-subdomain")]
            ),
            critical=False,
        )
        .sign(ca_key, hashes.SHA256())
    )

    CA_CERT_PATH.write_bytes(ca_cert.public_bytes(serialization.Encoding.PEM))
    CA_CERT_PATH.chmod(0o644)
    LEAF_PEM_PATH.write_bytes(
        leaf_cert.public_bytes(serialization.Encoding.PEM)
        + leaf_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption(),
        )
    )
    LEAF_PEM_PATH.chmod(0o644)
    print(f"generated test CA {CA_CERT_PATH} and leaf {LEAF_PEM_PATH}")


if __name__ == "__main__":
    main()
