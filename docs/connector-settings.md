# Connector Settings Reference

## Required Settings

These are validated at startup in `DefaultConnectorConfigValidator.validateConfig()`. The connector will fail to start if any are missing.

| Setting | Type | Default | Notes |
|---------|------|---------|-------|
| `name` | STRING | *(none)* | Connector instance name. Must match Snowflake object identifier syntax. |
| `topics` | STRING | *(none)* | Kafka Connect framework requirement. |
| `snowflake.url.name` | STRING | *(none)* | Snowflake account URL (e.g. `https://myaccount.snowflakecomputing.com:443`). |
| `snowflake.user.name` | STRING | *(none)* | Snowflake user name for authentication. |
| `snowflake.private.key` | PASSWORD | *(none)* | PEM private key for key-pair auth. ConfigDef default is `""` but validator rejects empty. |
| `snowflake.private.key.passphrase` | PASSWORD | `""` | Passphrase for encrypted private key. Only required if the key is encrypted. |
| `snowflake.database.name` | STRING | *(none)* | Target Snowflake database. |
| `snowflake.schema.name` | STRING | *(none)* | Target Snowflake schema. |
| `snowflake.role.name` | STRING | *(none)* | Snowflake role used by the connector. |
| `key.converter` | STRING | *(none)* | Kafka Connect key deserializer class. Not defined in our ConfigDef but required by the framework. |
| `value.converter` | STRING | *(none)* | Kafka Connect value deserializer class. Not defined in our ConfigDef but required by the framework. |
| `value.converter.schema.registry.url` | STRING | *(none)* | Schema Registry URL. Only required when using Avro/Protobuf/JSON Schema converters. Referenced in Constants but not in our ConfigDef. |

## Behavioral Settings

These control connector behavior: data handling, error handling, metadata, monitoring, logging, and networking.

| Setting | Type | Default | Notes |
|---------|------|---------|-------|
| **Data routing / mapping** | | | |
| `snowflake.topic2table.map` | STRING | `""` | Optional topic-to-table mapping. Format: `topic1:table1,topic2:table2`. When empty, topic name = table name. |
| `behavior.on.null.values` | STRING | `"default"` | How to handle Kafka tombstones. `"default"` inserts empty JSON; `"ignore"` drops them. |
| `value.converter.schemas.enable` | STRING | *(not defined)* | Referenced in Constants; passed through to the Kafka Connect value converter. |
| `snowflake.jdbc.map` | STRING | *(not defined)* | Additional JDBC connection parameters. Referenced in Constants but not in ConfigDef. |
| **Metadata flags** | | | |
| `snowflake.metadata.all` | BOOLEAN | `true` | Master switch for all metadata collection. If `false`, all metadata is dropped. |
| `snowflake.metadata.createtime` | BOOLEAN | `true` | Include record create-time in `RECORD_METADATA`. |
| `snowflake.metadata.topic` | BOOLEAN | `true` | Include Kafka topic name in `RECORD_METADATA`. |
| `snowflake.metadata.offset.and.partition` | BOOLEAN | `true` | Include Kafka offset and partition in `RECORD_METADATA`. |
| `snowflake.streaming.metadata.connectorPushTime` | BOOLEAN | `true` | Include `ConnectorPushTime` timestamp in `RECORD_METADATA`. |
| **Error handling** | | | |
| `errors.tolerance` | STRING | `"none"` | `"none"` = fail fast on any error. `"all"` = skip problematic records. |
| `errors.log.enable` | BOOLEAN | `false` | Log each tolerated error with details of the failed record. |
| `errors.deadletterqueue.topic.name` | STRING | `""` | DLQ topic for failed records. Requires `errors.tolerance=all`. Empty = no DLQ. |
| `enable.task.fail.on.authorization.errors` | BOOLEAN | `false` | If `true`, fail the task on Snowflake authorization errors (rather than retrying). |
| **Monitoring / logging** | | | |
| `jmx` | BOOLEAN | `true` | Enable JMX MBeans for custom Snowflake metrics. |
| `enable.mdc.logging` | BOOLEAN | `false` | Prepend MDC context to log messages. Requires Apache Kafka 2.3+. |
| **Proxy** | | | |
| `jvm.proxy.host` | STRING | `""` | HTTPS proxy host. |
| `jvm.proxy.port` | STRING | `""` | HTTPS proxy port. |
| `jvm.nonProxy.hosts` | STRING | `""` | Hosts that bypass the proxy. |
| `jvm.proxy.username` | STRING | `""` | Proxy authentication username. |
| `jvm.proxy.password` | PASSWORD | `""` | Proxy authentication password. |

## Performance Settings

These control flush frequency and caching to reduce network round-trips to Snowflake.

| Setting | Type | Default | Min | Notes |
|---------|------|---------|-----|-------|
| **Streaming client flush** | | | | |
| `snowflake.streaming.max.client.lag` | LONG | `1` (second) | `1` | How often the Ingest SDK flushes its internal buffer. Applied via `setDefaultValues()` with default 1s; ConfigDef minimum is also 1s. |
| `snowflake.streaming.client.provider.override.map` | STRING | `""` | - | Key-value overrides for SDK client properties (e.g. `MAX_CLIENT_LAG:5000`). **Use only after consulting Snowflake Support.** |
| **Existence-check caching** | | | | |
| `snowflake.cache.table.exists` | BOOLEAN | `true` | - | Cache `tableExist()` results to avoid repeated `DESC TABLE` queries. |
| `snowflake.cache.table.exists.expire.ms` | LONG | `300000` (5 min) | `1` | TTL for table-existence cache entries. |
| `snowflake.cache.pipe.exists` | BOOLEAN | `true` | - | Cache `pipeExist()` results to avoid repeated `SHOW PIPES` queries. |
| `snowflake.cache.pipe.exists.expire.ms` | LONG | `300000` (5 min) | `1` | TTL for pipe-existence cache entries. |

## Source Files

- **Constants & defaults**: `Constants.KafkaConnectorConfigParams`
- **Kafka ConfigDef registration**: `config/ConnectorConfigDefinition`
- **Defaults backfill**: `ConnectorConfigTools.setDefaultValues()`
- **Required-field validation**: `DefaultConnectorConfigValidator.validateConfig()`
