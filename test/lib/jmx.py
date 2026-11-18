"""Reads connector JMX metrics through the Jolokia agent that run_tests.sh --jmx mounts.

Jolokia runs inside the Kafka Connect JVM: on apache in the `kafka` container, on confluent in
`kafka-connect`. KAFKA_CONNECT_HOST is set accordingly by the docker-compose overrides.
"""

import json
import logging
import os
import urllib.error
import urllib.parse
import urllib.request

logger = logging.getLogger(__name__)

JMX_DOMAIN = "snowflake.kafka.connector"

# Gauge names in the evolve-schema category; see SnowflakeTelemetryChannelStatus.
EVOLVE_SCHEMA_COUNTERS = (
    "call-count",
    "rejected-count",
    "redelivered-count",
    "not-enabled-count",
)


class Jolokia:
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")

    def _get(self, path: str) -> dict:
        url = f"{self.base_url}/{path}"
        with urllib.request.urlopen(url, timeout=10) as resp:
            return json.loads(resp.read())

    def is_reachable(self) -> bool:
        try:
            return self._get("version").get("status") == 200
        except (urllib.error.URLError, OSError, ValueError):
            return False

    def read_pattern(self, mbean_pattern: str) -> dict:
        """Returns {mbean_name: attributes} for every MBean matching the pattern.

        An empty dict means no MBean matches yet, for example before the channel opens.
        """
        data = self._get("read/" + urllib.parse.quote(mbean_pattern, safe=":=,*"))
        if data.get("status") == 404:
            return {}
        if data.get("status") != 200:
            raise RuntimeError(f"Jolokia error reading {mbean_pattern}: {data}")
        value = data["value"]
        if "*" not in mbean_pattern:
            return {mbean_pattern: value}
        return value

    def evolve_schema_counters(self, connector_name: str) -> dict:
        """The connector's evolve-schema counters, summed over its channels.

        Channels register their gauges when they open and remove them when they close, so a
        reopened channel starts again from zero.
        """
        # MetricsJmxReporter registers MBeans under the uppercased connector name.
        pattern = (
            f"{JMX_DOMAIN}:connector={connector_name.upper()},channel=*,"
            f"category=evolve-schema,name=*"
        )
        totals = dict.fromkeys(EVOLVE_SCHEMA_COUNTERS, 0)
        for mbean, attributes in self.read_pattern(pattern).items():
            name = dict(
                part.split("=", 1) for part in mbean.split(":", 1)[1].split(",")
            )["name"]
            totals[name] = totals.get(name, 0) + int(attributes["Value"])
        return totals


def from_env() -> Jolokia | None:
    """A client for this run's Jolokia agent, or None if run_tests.sh wasn't given --jmx."""
    host = os.environ.get("KAFKA_CONNECT_HOST", "kafka-connect")
    client = Jolokia(f"http://{host}:8778/jolokia")
    if not client.is_reachable():
        logger.info(
            "Jolokia isn't reachable at %s; JMX checks are skipped", client.base_url
        )
        return None
    return client
