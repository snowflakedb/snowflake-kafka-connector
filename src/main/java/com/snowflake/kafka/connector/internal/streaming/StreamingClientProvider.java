package com.snowflake.kafka.connector.internal.streaming;

import java.util.Map;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;

public interface StreamingClientProvider {
  SnowflakeStreamingIngestClient getClient(Map<String, String> connectorConfig);

  void closeClient(Map<String, String> connectorConfig, SnowflakeStreamingIngestClient client);
}
