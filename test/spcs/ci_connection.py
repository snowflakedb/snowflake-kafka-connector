"""Write the snow CLI connection for spcs-release from the existing CI profile.

The SPCS fixture (setup_ci.py) belongs to the CI role, so the smoke test uses the
same credential instead of a separate user. Prints only the warehouse name.
"""

import argparse
import json
import os
from pathlib import Path
import sys

from cryptography.hazmat.primitives import serialization

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from lib.config import Profile  # noqa: E402
from lib.crypto import parse_private_key  # noqa: E402
import setup_ci  # noqa: E402


def write_private(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, "wb") as handle:
        handle.write(data)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--key", required=True, type=Path)
    parser.add_argument(
        "--config", type=Path, default=Path.home() / ".snowflake/config.toml"
    )
    args = parser.parse_args(argv)
    profile = Profile.load(Path(os.environ["SNOWFLAKE_CREDENTIAL_FILE"]))
    if (profile.role or "").upper() != setup_ci.ROLE:
        raise SystemExit("CI profile role does not own the SPCS fixture")
    if (profile.database or "").upper() != setup_ci.DATABASE:
        raise SystemExit("CI profile database does not hold the SPCS fixture")
    required = (profile.host, profile.user, profile.private_key, profile.warehouse)
    if not all(required):
        raise SystemExit(
            "CI profile must specify host, user, private key and warehouse"
        )
    key = serialization.load_der_private_key(
        parse_private_key(profile.private_key), password=None
    )
    write_private(
        args.key,
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        ),
    )
    # json.dumps yields valid TOML basic strings for these values.
    fields = {
        "account": profile.get_or_infer_account(),
        "host": profile.host.split(":")[0],
        "user": profile.user,
        "role": setup_ci.ROLE,
        "authenticator": "SNOWFLAKE_JWT",
        "private_key_file": str(args.key.resolve()),
    }
    lines = ["[connections.spcs]"]
    lines += [f"{name} = {json.dumps(value)}" for name, value in fields.items()]
    write_private(args.config, ("\n".join(lines) + "\n").encode())
    print(profile.warehouse)
    return 0


if __name__ == "__main__":
    sys.exit(main())
