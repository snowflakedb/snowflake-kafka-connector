from lib.config_migration import V4_CONFIG_TEMPLATE


def json_connector_config(topic: str, schematization: bool, validation: bool) -> dict:
    """Build a v4 connector config for JSON ingestion into an iceberg table."""
    config = {
        **V4_CONFIG_TEMPLATE,
        "tasks.max": "1",
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
        "snowflake.enable.schematization": str(schematization).lower(),
        "snowflake.validation": "client_side" if validation else "server_side",
        "topics": topic,
        "jmx": "true",
    }
    if schematization:
        # JSON field names are lowercase; Snowflake column names are uppercase.
        # Normalization uppercases the field names so the row validator and SSv2
        # can match them to the pre-declared columns (ID, BODY_TEMPERATURE, etc.).
        config["snowflake.compatibility.enable.column.identifier.normalization"] = (
            "true"
        )
    return config
