package com.snowflake.kafka.connector.internal.streaming.v2;

import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import com.snowflake.kafka.connector.internal.streaming.StreamingClientProperties;
import java.util.Map;

public interface StreamingIngestClientV2Provider {
  SnowflakeStreamingIngestClient getClient(
      Map<String, String> connectorConfig,
      String pipeName,
      StreamingClientProperties streamingClientProperties);

  void close(String pipeName);

  void closeAll();
}
