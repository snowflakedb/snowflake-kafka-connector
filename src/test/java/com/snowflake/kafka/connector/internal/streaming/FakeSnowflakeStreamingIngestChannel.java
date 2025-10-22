package com.snowflake.kafka.connector.internal.streaming;

import com.snowflake.ingest.streaming.ChannelStatus;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;

public class FakeSnowflakeStreamingIngestChannel
    implements SnowflakeStreamingIngestChannel, Comparable<FakeSnowflakeStreamingIngestChannel> {

  private final String pipeName;
  private final String channelName;
  /** Reference to the client that owns this channel */
  private final SnowflakeStreamingIngestClient owningClient;
  /** Lock used to protect the buffers from concurrent read/write */
  private final Lock bufferLock;

  private boolean closed;
  private String offsetToken;
  private int appendedRowsCount;

  public FakeSnowflakeStreamingIngestChannel(
      SnowflakeStreamingIngestClient owningClient, String pipeName, String channelName) {
    this.owningClient = owningClient;
    this.pipeName = pipeName;
    this.channelName = channelName;
    this.bufferLock = new ReentrantLock();
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
  public void appendRow(final Map<String, Object> row, final String offsetToken) {
    // fake client don't care about the data appended we need to keep track of the offset
    this.offsetToken = offsetToken;
    this.appendedRowsCount++;
  }

  @Override
  public void appendRows(
      final Iterable<Map<String, Object>> rows,
      final String startOffsetToken,
      final String endOffsetToken) {
    // fake client don't care about the data appended we need to keep track of the offset
    this.offsetToken = endOffsetToken;
  }

  @Override
  public String getLatestCommittedOffsetToken() {
    bufferLock.lock();
    try {
      return offsetToken;
    } finally {
      bufferLock.unlock();
    }
  }

  @Override
  public ChannelStatus getChannelStatus() {
    throw new UnsupportedOperationException();
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

  public int getAppendedRowsCount() {
    return appendedRowsCount;
  }

  @Override
  public int compareTo(final FakeSnowflakeStreamingIngestChannel o) {
    return this.channelName.compareTo(o.getChannelName());
  }
}
