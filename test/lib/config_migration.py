"""Config migration between KC v3 and v4 connector configurations."""

import copy
from typing import Dict

V4_CONNECTOR_CLASS = "com.snowflake.kafka.connector.SnowflakeStreamingSinkConnector"
V3_CONNECTOR_CLASS = "com.snowflake.kafka.connector.SnowflakeSinkConnector"


def v4_config_to_v3(config: Dict[str, str]) -> Dict[str, str]:
    """Convert a v4 connector config to v3 equivalent."""
    v3 = copy.deepcopy(config)
    v3["connector.class"] = V3_CONNECTOR_CLASS
    v3["snowflake.ingestion.method"] = "SNOWPIPE_STREAMING"
    # v3 defaults to schematization off; v4 defaults to on.
    # Preserve v4's default by setting it explicitly for v3 when unspecified.
    v3.setdefault("snowflake.enable.schematization", "true")
    v3.setdefault("buffer.flush.time", "1")
    v3.setdefault("snowflake.streaming.max.client.lag", "1")
    return v3


def v3_config_to_v4(config: Dict[str, str]) -> Dict[str, str]:
    """Convert a v3 connector config to v4 equivalent."""
    v4 = copy.deepcopy(config)
    v4["connector.class"] = V4_CONNECTOR_CLASS
    v4.pop("snowflake.ingestion.method", None)
    # v4 defaults to schematization on; v3 defaults to off.
    # Preserve v3's default by setting it explicitly for v4 when unspecified.
    v4.setdefault("snowflake.enable.schematization", "false")
    return v4
