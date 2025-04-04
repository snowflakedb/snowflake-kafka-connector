package com.snowflake.kafka.connector.internal.streaming;

import java.util.Map;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;

public class SimpleStreamingClientProvider implements StreamingClientProvider {

  private final StreamingClientHandler streamingClientHandler = new DirectStreamingClientHandler();

  @Override
  public SnowflakeStreamingIngestClient getClient(Map<String, String> connectorConfig) {
    return streamingClientHandler.createClient(new StreamingClientProperties(connectorConfig));
  }

  @Override
  public void closeClient(
      Map<String, String> connectorConfig, SnowflakeStreamingIngestClient client) {
    streamingClientHandler.closeClient(client);
  }
}
