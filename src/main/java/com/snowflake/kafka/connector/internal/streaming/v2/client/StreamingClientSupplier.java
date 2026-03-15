package com.snowflake.kafka.connector.internal.streaming.v2.client;

import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import com.snowflake.kafka.connector.config.SinkTaskConfig;
import com.snowflake.kafka.connector.internal.streaming.StreamingClientProperties;

public interface StreamingClientSupplier {
  SnowflakeStreamingIngestClient get(
      String clientName,
      String dbName,
      String schemaName,
      String pipeName,
      SinkTaskConfig config,
      StreamingClientProperties streamingClientProperties);
}
