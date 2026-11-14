"""Reject stale/ambiguous connector bundles before starting a Connect worker."""

import hashlib
import json
from pathlib import Path
import re
import sys
import zipfile

V4 = "com/snowflake/kafka/connector/SnowflakeStreamingSinkConnector.class"
V3 = "com/snowflake/kafka/connector/SnowflakeSinkConnector.class"
METADATA = "META-INF/maven/com.snowflake/snowflake-kafka-connector/pom.properties"


def inspect(directory):
    connectors = []
    for jar in sorted(Path(directory).rglob("*.jar")):
        with zipfile.ZipFile(jar) as archive:
            names = set(archive.namelist())
            if V3 in names:
                raise ValueError("Legacy connector class in plugin bundle")
            if V4 not in names:
                continue
            properties = {}
            for line in archive.read(METADATA).decode("utf-8").splitlines():
                if "=" in line and not line.startswith("#"):
                    key, value = line.split("=", 1)
                    properties[key.strip()] = value.strip()
            version = properties.get("version", "")
            if not re.fullmatch(r"4\.\d+\.\d+(?:[-+][A-Za-z0-9.-]+)?", version):
                raise ValueError("Connector artifact must have v4 Maven metadata")
            connectors.append(
                {
                    "version": version,
                    "sha256": hashlib.sha256(jar.read_bytes()).hexdigest(),
                }
            )
    if len(connectors) != 1:
        raise ValueError("Expected exactly one KC v4 artifact in plugin bundle")
    return connectors[0]


if __name__ == "__main__":
    print(json.dumps(inspect(sys.argv[1]), sort_keys=True))
