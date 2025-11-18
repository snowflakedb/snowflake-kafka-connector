package com.snowflake.kafka.connector.internal.streaming;

import static java.util.List.copyOf;

import com.snowflake.ingest.streaming.ChannelStatus;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
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
  /** Collection of all rows appended to this channel */
  private final List<Map<String, Object>> appendedRows;

  private volatile boolean closed;
  private String offsetToken;

  public FakeSnowflakeStreamingIngestChannel(
      SnowflakeStreamingIngestClient owningClient, String pipeName, String channelName) {
    this.owningClient = owningClient;
    this.pipeName = pipeName;
    this.channelName = channelName;
    this.bufferLock = new ReentrantLock();
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
  public void appendRow(final Map<String, Object> row, final String offsetToken) {
    bufferLock.lock();
    try {
      this.appendedRows.add(row);
      this.offsetToken = offsetToken;
    } finally {
      bufferLock.unlock();
    }
  }

  @Override
  public void appendRows(
      final Iterable<Map<String, Object>> rows,
      final String startOffsetToken,
      final String endOffsetToken) {

    bufferLock.lock();
    try {
      for (Map<String, Object> row : rows) {
        this.appendedRows.add(row);
      }
      this.offsetToken = endOffsetToken;
    } finally {
      bufferLock.unlock();
    }
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
    bufferLock.lock();
    try {
      return this.appendedRows.size();
    } finally {
      bufferLock.unlock();
    }
  }

  public List<Map<String, Object>> getAppendedRows() {
    bufferLock.lock();
    try {
      return copyOf(appendedRows);
    } finally {
      bufferLock.unlock();
    }
  }

  @Override
  public int compareTo(final FakeSnowflakeStreamingIngestChannel o) {
    return this.channelName.compareTo(o.getChannelName());
  }
}
