package com.snowflake.kafka.connector.internal.streaming;

import com.snowflake.ingest.streaming.ChannelStatus;
import com.snowflake.ingest.streaming.ChannelStatusBatch;
import com.snowflake.ingest.streaming.OpenChannelResult;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class FakeSnowflakeStreamingIngestClient implements SnowflakeStreamingIngestClient {

  private final String pipeName;
  private final String connectorName;
  private final Map<String, FakeSnowflakeStreamingIngestChannel> openedChannels =
      new ConcurrentHashMap<>();
  private final Map<String, String> channelNameToOffsetTokens = new ConcurrentHashMap<>();
  // Shared error counts per channel name - persists across channel reopens like real Snowflake
  private final Map<String, Long> channelNameToErrorCount = new ConcurrentHashMap<>();
  // Default error count to use when no channel-specific count is set
  private long defaultErrorCount = 0;
  private boolean closed = false;

  public FakeSnowflakeStreamingIngestClient(final String pipeName, final String connectorName) {
    this.pipeName = pipeName;
    this.connectorName = connectorName;
  }

  public void setDefaultErrorCount(final long errorCount) {
    this.defaultErrorCount = errorCount;
  }

  public void setInitialErrorCountForChannel(final String channelName, final long errorCount) {
    channelNameToErrorCount.put(channelName, errorCount);
  }

  public long getErrorCountForChannel(final String channelName) {
    return channelNameToErrorCount.getOrDefault(channelName, defaultErrorCount);
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
    // Error counts persist across channel reopens, like real Snowflake.
    // Use the existing channel's count if present, otherwise fall back to pre-seeded or default.
    FakeSnowflakeStreamingIngestChannel previous = openedChannels.get(channelName);
    final long errorCount =
        previous != null
            ? previous.getChannelStatus().getRowsErrorCount()
            : channelNameToErrorCount.getOrDefault(channelName, defaultErrorCount);
    final FakeSnowflakeStreamingIngestChannel channel =
        new FakeSnowflakeStreamingIngestChannel("db", "schema", pipeName, channelName);
    channel.setOffsetToken(offsetToken);
    channel.setErrorCount(errorCount);
    openedChannels.put(channel.getFullyQualifiedChannelName(), channel);
    return new OpenChannelResult(channel, channel.getChannelStatus());
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
    Map<String, ChannelStatus> statusMap = new HashMap<>();
    for (String name : channelNames) {
      FakeSnowflakeStreamingIngestChannel channel = openedChannels.get(name);
      if (channel != null) {
        statusMap.put(name, channel.getChannelStatus());
      }
    }
    return new ChannelStatusBatch(statusMap);
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
    return new ArrayList<>(openedChannels.values());
  }

  public String getConnectorName() {
    return connectorName;
  }

  public long countClosedChannels() {
    return openedChannels.values().stream()
        .filter(FakeSnowflakeStreamingIngestChannel::isClosed)
        .count();
  }

  public int getAppendedRowCount() {
    return openedChannels.values().stream()
        .mapToInt(FakeSnowflakeStreamingIngestChannel::getAppendedRowsCount)
        .sum();
  }
}
