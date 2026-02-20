package com.snowflake.kafka.connector.internal.streaming;

import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import com.snowflake.kafka.connector.internal.streaming.v2.client.StreamingClientSupplier;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class FakeIngestClientSupplier implements StreamingClientSupplier {

  private final ConcurrentHashMap<String, FakeSnowflakeStreamingIngestClient>
      pipeToIngestClientMap = new ConcurrentHashMap<>();

  private long preExistingErrorCount = 0;

  @Override
  public SnowflakeStreamingIngestClient get(
      final String clientName,
      final String dbName,
      final String schemaName,
      final String pipeName,
      final Map<String, String> connectorConfig,
      final StreamingClientProperties streamingClientProperties) {
    return pipeToIngestClientMap.computeIfAbsent(
        pipeName,
        (key) -> {
          final FakeSnowflakeStreamingIngestClient client =
              new FakeSnowflakeStreamingIngestClient(pipeName, connectorConfig.get("name"));
          client.setDefaultErrorCount(preExistingErrorCount);
          return client;
        });
  }

  public Collection<FakeSnowflakeStreamingIngestClient> getFakeIngestClients() {
    return pipeToIngestClientMap.values();
  }

  /**
   * Sets the pre-existing error count that will be applied to all channels when they are opened.
   * This simulates the cumulative error count that persists in Snowflake across connector restarts.
   */
  public void setPreExistingErrorCount(final long errorCount) {
    this.preExistingErrorCount = errorCount;
    // Also update existing clients
    for (final FakeSnowflakeStreamingIngestClient client : pipeToIngestClientMap.values()) {
      client.setDefaultErrorCount(errorCount);
    }
  }
}
