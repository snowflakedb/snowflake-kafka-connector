package com.snowflake.kafka.connector.internal.streaming.v2;

import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import com.snowflake.kafka.connector.internal.streaming.StreamingClientProperties;
import java.util.Map;

public interface IngestClientSupplier {

  SnowflakeStreamingIngestClient get(
      String clientName,
      String dbName,
      String schemaName,
      String pipeName,
      Map<String, String> connectorConfig,
      StreamingClientProperties streamingClientProperties);
}
