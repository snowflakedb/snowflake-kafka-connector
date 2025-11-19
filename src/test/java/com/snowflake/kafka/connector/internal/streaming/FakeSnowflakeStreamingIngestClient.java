package com.snowflake.kafka.connector.internal.streaming;

import com.snowflake.ingest.streaming.ChannelStatus;
import com.snowflake.ingest.streaming.ChannelStatusBatch;
import com.snowflake.ingest.streaming.OpenChannelResult;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class FakeSnowflakeStreamingIngestClient implements SnowflakeStreamingIngestClient {

  private final String pipeName;
  private final String connectorName;
  private final List<FakeSnowflakeStreamingIngestChannel> openedChannels =
      Collections.synchronizedList(new ArrayList<>());
  private final Map<String, String> channelNameToOffsetTokens = new ConcurrentHashMap<>();
  private boolean closed = false;

  public FakeSnowflakeStreamingIngestClient(final String pipeName, final String connectorName) {
    this.pipeName = pipeName;
    this.connectorName = connectorName;
  }

  @Override
  public void close() {
    this.closed = true;
  }

  @Override
  public CompletableFuture<Void> close(final boolean waitForFlush, final Duration timeoutDuration) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void initiateFlush() {
    throw new UnsupportedOperationException();
  }

  @Override
  public OpenChannelResult openChannel(final String channelName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OpenChannelResult openChannel(final String channelName, final String offsetToken) {
    if (offsetToken != null) {
      channelNameToOffsetTokens.put(channelName, offsetToken);
    }
    final ChannelStatus channelStatus =
        new ChannelStatus(
            "db",
            "schema",
            pipeName,
            channelName,
            "SUCCESS",
            offsetToken,
            Instant.now(),
            1,
            1,
            1,
            null,
            null,
            null,
            null,
            Instant.now());
    final FakeSnowflakeStreamingIngestChannel channel =
        new FakeSnowflakeStreamingIngestChannel(this, pipeName, channelName);
    openedChannels.add(channel);
    return new OpenChannelResult(channel, channelStatus);
  }

  @Override
  public void dropChannel(final String channelName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<String, String> getLatestCommittedOffsetTokens(final List<String> channelNames) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ChannelStatusBatch getChannelStatus(final List<String> channelNames) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isClosed() {
    throw new UnsupportedOperationException();
  }

  @Override
  public CompletableFuture<Void> waitForFlush(final Duration timeoutDuration) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getDBName() {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getSchemaName() {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getPipeName() {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getClientName() {
    throw new UnsupportedOperationException();
  }

  public List<FakeSnowflakeStreamingIngestChannel> getOpenedChannels() {
    return openedChannels;
  }

  public String getConnectorName() {
    return connectorName;
  }

  public long countClosedChannels() {
    return openedChannels.stream().filter((channel) -> channel.isClosed()).count();
  }

  public int getAppendedRowCount() {
    return openedChannels.stream()
        .mapToInt(FakeSnowflakeStreamingIngestChannel::getAppendedRowsCount)
        .sum();
  }
}
