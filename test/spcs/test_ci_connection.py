"""TEST ONLY: offline tests for ci_connection.py with a generated key."""

import base64
from contextlib import redirect_stdout
import io
import json
import os
from pathlib import Path
import shutil
import stat
import sys
import tempfile
import tomllib
import unittest
from unittest.mock import patch

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

sys.path.insert(0, str(Path(__file__).resolve().parent))
import ci_connection  # noqa: E402


def pem_body():
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode()
    return "".join(line for line in pem.splitlines() if "-----" not in line)


class CiConnectionTests(unittest.TestCase):
    def setUp(self):
        self.directory = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.directory)
        self.profile = {
            "host": "sfctest0.snowflakecomputing.com:443",
            "user": "KAFKA_CI_USER",
            "role": "testrole_kafka",
            "database": "testdb_kafka",
            "warehouse": "KAFKA_CI_WH",
            "private_key": pem_body(),
        }

    def run_main(self, **overrides):
        profile = self.directory / "profile.json"
        profile.write_text(json.dumps({**self.profile, **overrides}))
        key = self.directory / "scratch/key.p8"
        config = self.directory / "home/config.toml"
        stdout = io.StringIO()
        with patch.dict(os.environ, {"SNOWFLAKE_CREDENTIAL_FILE": str(profile)}):
            with redirect_stdout(stdout):
                code = ci_connection.main(["--key", str(key), "--config", str(config)])
        return code, stdout.getvalue(), key, config

    def test_writes_private_connection_and_prints_only_warehouse(self):
        code, stdout, key, config = self.run_main()
        self.assertEqual(code, 0)
        self.assertEqual(stdout, "KAFKA_CI_WH\n")
        for path in (key, config):
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
        self.assertTrue(key.read_bytes().startswith(b"-----BEGIN PRIVATE KEY-----"))
        written = serialization.load_pem_private_key(key.read_bytes(), password=None)
        profile_key = serialization.load_der_private_key(
            base64.b64decode(self.profile["private_key"]), password=None
        )
        self.assertEqual(written.private_numbers(), profile_key.private_numbers())
        connection = tomllib.loads(config.read_text())["connections"]["spcs"]
        self.assertEqual(
            connection,
            {
                "account": "sfctest0",
                "host": "sfctest0.snowflakecomputing.com",
                "user": "KAFKA_CI_USER",
                "role": "TESTROLE_KAFKA",
                "authenticator": "SNOWFLAKE_JWT",
                "private_key_file": str(key.resolve()),
            },
        )
        self.assertNotIn(self.profile["private_key"][:40], config.read_text())

    def test_rejects_profiles_that_do_not_own_the_fixture(self):
        for overrides in (
            {"role": "SYSADMIN"},
            {"role": None},
            {"database": "KC_TEST"},
            {"warehouse": None},
            {"host": None},
            {"user": None},
            {"private_key": None},
        ):
            with self.subTest(overrides=overrides):
                with self.assertRaises(SystemExit) as raised:
                    self.run_main(**overrides)
                self.assertNotIn(
                    self.profile["private_key"][:40], str(raised.exception)
                )
                self.assertFalse((self.directory / "scratch/key.p8").exists())


if __name__ == "__main__":
    unittest.main()
