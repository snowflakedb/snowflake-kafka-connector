package com.snowflake.kafka.connector.internal.streaming;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import net.snowflake.ingest.streaming.FakeSnowflakeStreamingIngestClient;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;

public class FakeStreamingClientHandler implements StreamingClientHandler {

  private final ConcurrentLinkedQueue<FakeSnowflakeStreamingIngestClient> clients =
      new ConcurrentLinkedQueue<>();
  private final AtomicInteger createClientCalls = new AtomicInteger(0);
  private final AtomicInteger closeClientCalls = new AtomicInteger(0);

  @Override
  public SnowflakeStreamingIngestClient createClient(
      StreamingClientProperties streamingClientProperties) {
    createClientCalls.incrementAndGet();
    FakeSnowflakeStreamingIngestClient ingestClient =
        new FakeSnowflakeStreamingIngestClient(
            streamingClientProperties.clientName + "_" + UUID.randomUUID());
    clients.add(ingestClient);
    return ingestClient;
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

  public Set<Map<String, Object>> ingestedRows() {
    return clients.stream()
        .map(FakeSnowflakeStreamingIngestClient::ingestedRecords)
        .flatMap(Collection::stream)
        .collect(Collectors.toSet());
  }

  public Map<String, String> getLatestCommittedOffsetTokensPerChannel() {
    return this.clients.stream()
        .map(FakeSnowflakeStreamingIngestClient::getLatestCommittedOffsetTokensPerChannel)
        .map(Map::entrySet)
        .flatMap(Collection::stream)
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  public long countChannels(Predicate<SnowflakeStreamingIngestChannel> predicate) {
    return this.clients.stream().mapToLong(channel -> channel.countChannels(predicate)).sum();
  }
}
