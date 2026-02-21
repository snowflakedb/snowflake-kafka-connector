package com.snowflake.kafka.connector.internal.streaming;

import static java.util.List.copyOf;

import com.snowflake.ingest.streaming.ChannelStatus;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import java.time.Duration;
import java.time.Instant;
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

  /** Reference to parent client for sharing error counts across channel reopens */
  private final FakeSnowflakeStreamingIngestClient parentClient;

  private volatile boolean closed;
  private String offsetToken;
  private ChannelStatus channelStatus;

  public FakeSnowflakeStreamingIngestChannel(
      final String pipeName,
      final String channelName,
      final FakeSnowflakeStreamingIngestClient parentClient) {
    this.pipeName = pipeName;
    this.channelName = channelName;
    this.appendedRows = new ArrayList<>();
    this.parentClient = parentClient;
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
    // Build a fresh status reflecting the current offset token so the batch path sees up-to-date
    // committed offsets.
    long errorCount =
        parentClient != null
            ? parentClient.getErrorCountForChannel(channelName)
            : channelStatus.getRowsErrorCount();
    return new ChannelStatus(
        channelStatus.getDatabaseName(),
        channelStatus.getSchemaName(),
        channelStatus.getPipeName(),
        channelStatus.getChannelName(),
        channelStatus.getStatusCode(),
        offsetToken,
        channelStatus.getCreatedOn(),
        channelStatus.getRowsInsertedCount(),
        channelStatus.getRowsParsedCount(),
        errorCount,
        channelStatus.getLastErrorOffsetTokenUpperBound(),
        channelStatus.getLastErrorMessage(),
        channelStatus.getLastErrorTimestamp(),
        channelStatus.getServerAvgProcessingLatency(),
        Instant.now());
  }

  public void setChannelStatus(final ChannelStatus channelStatus) {
    this.channelStatus = channelStatus;
    // Update the shared error count in the parent client so it persists across channel reopens
    if (parentClient != null && channelStatus.getRowsErrorCount() > 0) {
      parentClient.setInitialErrorCountForChannel(channelName, channelStatus.getRowsErrorCount());
    }
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
