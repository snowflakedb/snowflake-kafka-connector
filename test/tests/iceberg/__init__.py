from lib.config_migration import V4_CONFIG_TEMPLATE

# Structured RECORD_METADATA OBJECT schema for managed Iceberg, mirroring the
# connector's IcebergDDLTypes.ICEBERG_METADATA_OBJECT_SCHEMA. Used to pre-create
# v2 Iceberg tables, where a VARIANT metadata column is not allowed (VARIANT in
# Iceberg requires format v3).
ICEBERG_METADATA_OBJECT = (
    "OBJECT(offset LONG, topic STRING, partition INTEGER, key STRING, "
    "CreateTime BIGINT, SnowflakeConnectorPushTime BIGINT, headers MAP(VARCHAR, VARCHAR))"
)


def record_metadata_column(iceberg_version: int) -> str:
    """RECORD_METADATA column DDL for a given Iceberg format version.

    v3 can use VARIANT; v2 must use the structured OBJECT (VARIANT-in-Iceberg
    requires v3). Using OBJECT on v2 also exercises the connector's strict
    typed-OBJECT cast / metadata-conform path.
    """
    return (
        "RECORD_METADATA VARIANT"
        if iceberg_version == 3
        else f"RECORD_METADATA {ICEBERG_METADATA_OBJECT}"
    )


def json_connector_config(
    topic: str,
    schematization: bool,
    validation: bool,
    table_type: str = "snowflake",
    iceberg_create_table_options: str = None,
) -> dict:
    """Build a v4 connector config for JSON ingestion into an iceberg table.

    ``table_type`` selects the auto-creation behavior (snowflake | iceberg | none).
    ``iceberg_create_table_options`` are SQL clauses spliced into the auto-created Iceberg
    table's CREATE after the column list (e.g. "EXTERNAL_VOLUME='v' ICEBERG_VERSION=3"); only
    valid when ``table_type='iceberg'``. Leave None to use account/schema/db defaults.
    """
    config = {
        **V4_CONFIG_TEMPLATE,
        "tasks.max": "1",
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
        "snowflake.enable.schematization": str(schematization).lower(),
        "snowflake.validation": "client_side" if validation else "server_side",
        "snowflake.autocreate.table.type": table_type,
        "topics": topic,
        "jmx": "true",
    }
    if table_type == "iceberg" and iceberg_create_table_options:
        config["snowflake.iceberg.create.table.options"] = iceberg_create_table_options
    if schematization:
        # JSON field names are lowercase; Snowflake column names are uppercase.
        # Normalization uppercases the field names so the row validator and SSv2
        # can match them to the pre-declared columns (ID, BODY_TEMPERATURE, etc.).
        config["snowflake.compatibility.enable.column.identifier.normalization"] = (
            "true"
        )
    return config
