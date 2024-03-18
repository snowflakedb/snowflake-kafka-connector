package com.snowflake.kafka.connector.internal.streaming;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import net.snowflake.ingest.streaming.FakeSnowflakeStreamingIngestClient;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;

public class FakeStreamingClientHandler implements StreamingClientHandler {

  private AtomicInteger createClientCalls = new AtomicInteger(0);
  private AtomicInteger closeClientCalls = new AtomicInteger(0);

  @Override
  public SnowflakeStreamingIngestClient createClient(
      StreamingClientProperties streamingClientProperties) {
    createClientCalls.incrementAndGet();
    return new FakeSnowflakeStreamingIngestClient(
        streamingClientProperties.clientName + "_" + UUID.randomUUID());
  }

  @Override
  public void closeClient(SnowflakeStreamingIngestClient client) {
    closeClientCalls.incrementAndGet();
    try {
      client.close();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public Integer getCreateClientCalls() {
    return createClientCalls.get();
  }

  public Integer getCloseClientCalls() {
    return closeClientCalls.get();
  }
}
