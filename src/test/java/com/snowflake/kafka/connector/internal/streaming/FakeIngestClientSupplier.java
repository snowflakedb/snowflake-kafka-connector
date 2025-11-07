package com.snowflake.kafka.connector.internal.streaming;

import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import com.snowflake.kafka.connector.internal.streaming.v2.IngestClientSupplier;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class FakeIngestClientSupplier implements IngestClientSupplier {

  private final ConcurrentHashMap<String, FakeSnowflakeStreamingIngestClient>
      pipeToIngestClientMap = new ConcurrentHashMap<>();

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
        (key) -> new FakeSnowflakeStreamingIngestClient(pipeName, connectorConfig.get("name")));
  }

  public Collection<FakeSnowflakeStreamingIngestClient> getFakeIngestClients() {
    return pipeToIngestClientMap.values();
  }
}
