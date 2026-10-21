from lib.config_migration import V4_CONFIG_TEMPLATE


def interactive_json_config(topic: str, create_table_options: str = None) -> dict:
    """Build a v4 JSON connector config that auto-creates an interactive table.

    Server-side schema evolution only: the connector creates a metadata-only interactive
    table and the default streaming pipe adds payload columns as records arrive.
    ``create_table_options`` maps to ``snowflake.interactive.create.table.options``.
    """
    config = {
        **V4_CONFIG_TEMPLATE,
        "tasks.max": "1",
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
        "snowflake.enable.schematization": "true",
        "snowflake.validation": "server_side",
        "snowflake.autocreate.table.type": "interactive",
        "snowflake.compatibility.enable.column.identifier.normalization": "true",
        "topics": topic,
        "jmx": "true",
    }
    if create_table_options:
        config["snowflake.interactive.create.table.options"] = create_table_options
    return config
