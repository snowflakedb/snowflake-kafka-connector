package com.snowflake.kafka.connector.internal.streaming;

import static java.util.List.copyOf;

import com.snowflake.ingest.streaming.ChannelStatus;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

public class FakeSnowflakeStreamingIngestChannel
    implements SnowflakeStreamingIngestChannel, Comparable<FakeSnowflakeStreamingIngestChannel> {

  private final String pipeName;
  private final String channelName;
  /** Collection of all rows appended to this channel */
  private final List<Map<String, Object>> appendedRows;

  private volatile boolean closed;
  private String offsetToken;
  private ChannelStatus channelStatus;

  public FakeSnowflakeStreamingIngestChannel(String pipeName, String channelName) {
    this.pipeName = pipeName;
    this.channelName = channelName;
    this.appendedRows = new ArrayList<>();
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
    return pipeName;
  }

  @Override
  public String getFullyQualifiedPipeName() {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getFullyQualifiedChannelName() {
    return channelName;
  }

  @Override
  public boolean isClosed() {
    return closed;
  }

  @Override
  public String getChannelName() {
    return channelName;
  }

  @Override
  public void close() {
    this.closed = true;
  }

  @Override
  public void close(final boolean waitForFlush, final Duration timeoutDuration)
      throws TimeoutException {
    this.close();
  }

  @Override
  public synchronized void appendRow(final Map<String, Object> row, final String offsetToken) {
    this.appendedRows.add(row);
    this.offsetToken = offsetToken;
  }

  @Override
  public synchronized void appendRows(
      final Iterable<Map<String, Object>> rows,
      final String startOffsetToken,
      final String endOffsetToken) {

    for (Map<String, Object> row : rows) {
      this.appendedRows.add(row);
    }
    this.offsetToken = endOffsetToken;
  }

  @Override
  public synchronized String getLatestCommittedOffsetToken() {
    return offsetToken;
  }

  @Override
  public ChannelStatus getChannelStatus() {
    if (channelStatus == null) {
      throw new UnsupportedOperationException("ChannelStatus not configured for test");
    }
    return channelStatus;
  }

  public void setChannelStatus(final ChannelStatus channelStatus) {
    this.channelStatus = channelStatus;
  }

  @Override
  public CompletableFuture<Void> waitForCommit(
      final Predicate<String> tokenChecker, final Duration timeoutDuration) {
    throw new UnsupportedOperationException();
  }

  @Override
  public CompletableFuture<Void> waitForFlush(final Duration timeoutDuration) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void initiateFlush() {
    throw new UnsupportedOperationException();
  }

  public synchronized int getAppendedRowsCount() {
    return this.appendedRows.size();
  }

  public synchronized List<Map<String, Object>> getAppendedRows() {
    return copyOf(appendedRows);
  }

  @Override
  public int compareTo(final FakeSnowflakeStreamingIngestChannel o) {
    return this.channelName.compareTo(o.getChannelName());
  }
}
