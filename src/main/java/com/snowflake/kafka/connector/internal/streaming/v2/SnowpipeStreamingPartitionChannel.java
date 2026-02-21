package com.snowflake.kafka.connector.internal.streaming.v2;

import static com.snowflake.kafka.connector.internal.SnowflakeErrors.ERROR_5027;
import static com.snowflake.kafka.connector.internal.SnowflakeErrors.ERROR_5028;
import static com.snowflake.kafka.connector.internal.SnowflakeErrors.ERROR_5030;

import com.google.common.annotations.VisibleForTesting;
import com.snowflake.ingest.streaming.ChannelStatus;
import com.snowflake.ingest.streaming.OpenChannelResult;
import com.snowflake.ingest.streaming.SFException;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import com.snowflake.kafka.connector.dlq.KafkaRecordErrorReporter;
import com.snowflake.kafka.connector.internal.KCLogger;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.metrics.MetricsJmxReporter;
import com.snowflake.kafka.connector.internal.streaming.LatestCommitedOffsetTokenExecutor;
import com.snowflake.kafka.connector.internal.streaming.StreamingClientProperties;
import com.snowflake.kafka.connector.internal.streaming.StreamingErrorHandler;
import com.snowflake.kafka.connector.internal.streaming.TopicPartitionChannelInsertionException;
import com.snowflake.kafka.connector.internal.streaming.channel.TopicPartitionChannel;
import com.snowflake.kafka.connector.internal.streaming.telemetry.SnowflakeTelemetryChannelCreation;
import com.snowflake.kafka.connector.internal.streaming.telemetry.SnowflakeTelemetryChannelStatus;
import com.snowflake.kafka.connector.internal.streaming.v2.channel.PartitionOffsetTracker;
import com.snowflake.kafka.connector.internal.streaming.v2.client.StreamingClientPools;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryService;
import com.snowflake.kafka.connector.records.SnowflakeMetadataConfig;
import com.snowflake.kafka.connector.records.SnowflakeSinkRecord;
import dev.failsafe.FailsafeExecutor;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;

public class SnowpipeStreamingPartitionChannel implements TopicPartitionChannel {
  private static final KCLogger LOGGER =
      new KCLogger(SnowpipeStreamingPartitionChannel.class.getName());
  private final StreamingClientProperties streamingClientProperties;
  private final String connectorName;
  private final String taskId;

  private SnowflakeStreamingIngestChannel channel;

  private final PartitionOffsetTracker offsetTracker;

  // Tracks the initial error count when the channel was opened.
  // Used to detect NEW errors (current error count > initial error count) since error counts
  // are cumulative and don't reset when a channel is reopened.
  private long initialErrorCount = 0;

  private final String channelName;

  private final SnowflakeTelemetryChannelStatus snowflakeTelemetryChannelStatus;

  private final KafkaRecordErrorReporter kafkaRecordErrorReporter;
  private final SnowflakeMetadataConfig metadataConfig;

  /**
   * Used to send telemetry to Snowflake. Currently, TelemetryClient created from a Snowflake
   * Connection Object, i.e. not a session-less Client
   */
  private final SnowflakeTelemetryService telemetryService;

  private final FailsafeExecutor<Long> offsetTokenExecutor;

  private final String pipeName;

  private final Map<String, String> connectorConfig;

  private final StreamingErrorHandler streamingErrorHandler;

  public SnowpipeStreamingPartitionChannel(
      String tableName,
      String channelName,
      String pipeName,
      TopicPartition topicPartition,
      SnowflakeConnectionService conn,
      Map<String, String> connectorConfig,
      KafkaRecordErrorReporter kafkaRecordErrorReporter,
      SnowflakeMetadataConfig metadataConfig,
      SinkTaskContext sinkTaskContext,
      boolean enableCustomJMXMonitoring,
      MetricsJmxReporter metricsJmxReporter,
      String connectorName,
      String taskId,
      StreamingErrorHandler streamingErrorHandler) {
    this.channelName = channelName;
    this.connectorConfig = connectorConfig;
    this.kafkaRecordErrorReporter = kafkaRecordErrorReporter;
    this.metadataConfig = metadataConfig;
    this.connectorName = connectorName;
    this.taskId = taskId;
    this.streamingErrorHandler = streamingErrorHandler;

    this.telemetryService = conn.getTelemetryClient();
    this.pipeName = pipeName;
    this.streamingClientProperties = new StreamingClientProperties(connectorConfig);

    this.offsetTracker = new PartitionOffsetTracker(topicPartition, sinkTaskContext, channelName);

    LOGGER.info(
        "Initializing SnowpipeStreamingPartitionChannel for table: {}, channel: {}, pipe: {},"
            + " topic: {}, partition: {}",
        tableName,
        channelName,
        pipeName,
        topicPartition.topic(),
        topicPartition.partition());

    this.channel = openChannelForTable(channelName);
    this.offsetTokenExecutor =
        LatestCommitedOffsetTokenExecutor.getExecutor(
            this.getChannelNameFormatV1(),
            Exception.class,
            () ->
                streamingApiFallbackSupplier(
                    StreamingApiFallbackInvoker.GET_OFFSET_TOKEN_FALLBACK));

    final long lastCommittedOffsetToken = fetchOffsetTokenWithRetry();
    offsetTracker.initializeFromSnowflake(lastCommittedOffsetToken);

    final long startTime = System.currentTimeMillis();
    this.snowflakeTelemetryChannelStatus =
        new SnowflakeTelemetryChannelStatus(
            tableName,
            this.connectorName,
            channelName,
            startTime,
            enableCustomJMXMonitoring,
            metricsJmxReporter,
            offsetTracker.persistedOffsetRef(),
            offsetTracker.processedOffsetRef(),
            offsetTracker.consumerGroupOffsetRef());

    this.telemetryService.reportKafkaPartitionStart(
        new SnowflakeTelemetryChannelCreation(tableName, channelName, startTime));
  }

  @Override
  public void insertRecord(SinkRecord kafkaSinkRecord, boolean isFirstRowPerPartitionInBatch) {
    if (offsetTracker.shouldProcess(kafkaSinkRecord.kafkaOffset(), isFirstRowPerPartitionInBatch)) {
      transformAndSend(kafkaSinkRecord);
    }
  }

  private void transformAndSend(SinkRecord kafkaSinkRecord) {
    try {
      final long kafkaOffset = kafkaSinkRecord.kafkaOffset();
      final SnowflakeSinkRecord record = SnowflakeSinkRecord.from(kafkaSinkRecord, metadataConfig);

      if (record.isBroken()) {
        LOGGER.debug("Broken record offset:{}, topic:{}", kafkaOffset, kafkaSinkRecord.topic());
        kafkaRecordErrorReporter.reportError(kafkaSinkRecord, new DataException("Broken Record"));
      } else {
        // If we reach here, it means we should ingest a record (possibly empty for tombstones)
        final Map<String, Object> transformedRecord =
            record.getContentWithMetadata(metadataConfig.shouldIncludeAllMetadata());
        if (!transformedRecord.isEmpty()) {
          insertRowWithFallback(transformedRecord, kafkaOffset);
        }
      }
      // Always update processedOffset after processing, even for broken records
      offsetTracker.recordProcessed(kafkaOffset);
    } catch (TopicPartitionChannelInsertionException ex) {
      // Suppressing the exception because other channels might still continue to ingest
      LOGGER.warn(
          "Failed to insert row for channel:{}. Will be retried by Kafka. Exception: {}",
          this.getChannelNameFormatV1(),
          ex);
    }
  }

  @Override
  public CompletableFuture<Void> waitForLastProcessedRecordCommitted() {
    if (offsetTracker.getLastAppendRowsOffset() == NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE) {
      return CompletableFuture.completedFuture(null);
    }

    return CompletableFuture.runAsync(
        () -> {
          LOGGER.info("Starting flush for channel: {}", this.getChannelNameFormatV1());

          StreamingClientPools.getClient(
                  connectorName, taskId, pipeName, connectorConfig, streamingClientProperties)
              .initiateFlush();

          final long targetOffset = offsetTracker.getLastAppendRowsOffset();
          WaitForLastOffsetCommittedPolicy.getPolicy(
              () -> {
                long offsetCommittedToBackend = fetchLatestCommittedOffsetFromSnowflake();
                if (offsetCommittedToBackend == targetOffset) {
                  return true;
                }
                throw ERROR_5027.getException();
              });

          LOGGER.info("Completed flush for channel: {}", this.getChannelNameFormatV1());
        });
  }

  private static Map<String, Object> unquoteIdentifiers(Map<String, Object> transformedRecord) {
    Map<String, Object> unquotedMap = new HashMap<>();
    transformedRecord.forEach(
        (originalKey, originalValue) -> {
          if (originalKey.startsWith("\"") && originalKey.endsWith("\"")) {
            unquotedMap.put(originalKey.substring(1, originalKey.length() - 1), originalValue);
          } else {
            unquotedMap.put(originalKey, originalValue);
          }
        });

    return unquotedMap;
  }

  /**
   * Uses {@link AppendRowWithRetryAndFallbackPolicy} to reopen the channel if insertRows throws
   * {@link SFException}.
   *
   * <p>We have deliberately not performed retries on insertRows because it might slow down overall
   * ingestion and introduce lags in committing offsets to Kafka.
   *
   * <p>Note that insertRows API does perform channel validation which might throw SFException if
   * channel is invalidated.
   */
  private void insertRowWithFallback(Map<String, Object> transformedRecord, long offset) {
    AppendRowWithRetryAndFallbackPolicy.executeWithRetryAndFallback(
        () -> {
          LOGGER.trace("Inserting transformed record: {}, offset: {}", transformedRecord, offset);
          this.channel.appendRow(transformedRecord, Long.toString(offset));
          offsetTracker.recordAppended(offset);
        },
        this::insertRowFallbackSupplier,
        this.getChannelNameFormatV1());
  }

  /**
   * Fallback supplier used by {@link AppendRowWithRetryAndFallbackPolicy} to handle channel
   * recovery.
   *
   * <p>We will reopen the channel on {@link SFException} and reset offset in kafka. But, we will
   * throw a custom exception to show that records were not added into Snowflake.
   *
   * @param ex the original exception that caused the fallback to be triggered
   * @throws TopicPartitionChannelInsertionException exception is thrown after channel reopen has
   *     been successful and offsetToken was fetched from Snowflake
   */
  private void insertRowFallbackSupplier(Throwable ex)
      throws TopicPartitionChannelInsertionException {
    final long offsetRecoveredFromSnowflake =
        streamingApiFallbackSupplier(StreamingApiFallbackInvoker.APPEND_ROW_FALLBACK);
    throw new TopicPartitionChannelInsertionException(
        String.format(
            "%s Failed to insert rows for channel:%s. Recovered offset from Snowflake is:%s",
            StreamingApiFallbackInvoker.APPEND_ROW_FALLBACK,
            this.getChannelNameFormatV1(),
            offsetRecoveredFromSnowflake),
        ex);
  }

  @Override
  @VisibleForTesting
  public long fetchOffsetTokenWithRetry() {
    return offsetTokenExecutor.get(this::fetchLatestCommittedOffsetFromSnowflake);
  }

  private void closeChannelWithoutFlushing() {
    try {
      channel.close(false /* waitForFlush */, Duration.ZERO);
    } catch (TimeoutException e) {
      // This should never happen since we are not waiting for the channel to flush.
      throw new RuntimeException(
          String.format(
              "Error closing channel %s: %s",
              channel.getFullyQualifiedChannelName(), e.getMessage()));
    }
  }

  /**
   * Fallback function to be executed when either of insertRows API or getOffsetToken sends
   * SFException.
   *
   * <p>Or, in other words, if streaming channel is invalidated, we will reopen the channel and
   * reset the kafka offset to last committed offset in Snowflake.
   *
   * <p>If a valid offset is found from snowflake, we will reset the topicPartition with
   * (offsetReturnedFromSnowflake + 1).
   *
   * @param streamingApiFallbackInvoker Streaming API which is using this fallback function. Used
   *     for logging mainly.
   * @return offset which was last present in Snowflake
   */
  private long streamingApiFallbackSupplier(
      final StreamingApiFallbackInvoker streamingApiFallbackInvoker) {
    LOGGER.info(
        "{} Channel recovery initiated for channel: {}",
        streamingApiFallbackInvoker,
        this.getChannelNameFormatV1());

    // Close old channel before reopening a new one. We don't want to wait for the channel to flush
    // since it will be reopened right away and the in-progress data will be lost.
    if (!channel.isClosed()) {
      closeChannelWithoutFlushing();
    }
    SnowflakeStreamingIngestChannel newChannel = openChannelForTable(channelName);

    LOGGER.warn(
        "{} Fetching offsetToken after re-opening the channel:{}",
        streamingApiFallbackInvoker,
        this.getChannelNameFormatV1());
    long offsetRecoveredFromSnowflake = fetchLatestOffsetFromChannel(newChannel);

    if (offsetRecoveredFromSnowflake == NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE) {
      LOGGER.info(
          "{} Channel:{}, offset token is NULL, will attempt to use offset managed by the"
              + " connector, consumer offset: {}",
          streamingApiFallbackInvoker,
          this.getChannelNameFormatV1(),
          offsetTracker.consumerGroupOffsetRef().get());
    }

    offsetTracker.resetAfterRecovery(offsetRecoveredFromSnowflake);
    this.channel = newChannel;

    LOGGER.warn(
        "{} Channel:{}, recovery complete, offsetRecoveredFromSnowflake={}",
        streamingApiFallbackInvoker,
        this.getChannelNameFormatV1(),
        offsetRecoveredFromSnowflake);

    return offsetRecoveredFromSnowflake;
  }

  /**
   * Parses an offset token string into a long value.
   *
   * @param offsetToken the offset token string (may be null)
   * @param channelNameForLogging used in error messages
   * @return the parsed long, or {@link TopicPartitionChannel#NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE} if null
   * @throws ConnectException if the token is non-null but not parsable as long
   */
  @VisibleForTesting
  static long parseOffsetToken(String offsetToken, String channelNameForLogging) {
    if (offsetToken == null) {
      return NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE;
    }
    try {
      return Long.parseLong(offsetToken);
    } catch (NumberFormatException ex) {
      LOGGER.error(
          "The offsetToken string does not contain a parsable long:{} for channel:{}",
          offsetToken,
          channelNameForLogging);
      throw new ConnectException(ex);
    }
  }

  /**
   * Returns the offset Token persisted into snowflake.
   *
   * <p>OffsetToken from Snowflake returns a String and we will convert it into long.
   *
   * <p>If it is not long parsable, we will throw {@link ConnectException}
   *
   * @return -1 if no offset is found in snowflake, else the long value of committedOffset in
   *     snowflake.
   */
  private long fetchLatestCommittedOffsetFromSnowflake() {
    return fetchLatestOffsetFromChannel(this.channel);
  }

  private long fetchLatestOffsetFromChannel(SnowflakeStreamingIngestChannel channel) {
    String offsetToken = channel.getLatestCommittedOffsetToken();
    LOGGER.info(
        "Fetched offsetToken for channelName:{}, offset:{}",
        this.getChannelNameFormatV1(),
        offsetToken);
    return parseOffsetToken(offsetToken, this.getChannelNameFormatV1());
  }

  /**
   * Open a channel for Table with given channel name and tableName.
   *
   * <p>Open channels happens at:
   *
   * <p>Constructor of TopicPartitionChannel -> which means we will wipe of all states and it will
   * call precomputeOffsetTokenForChannel
   *
   * <p>Failure handling which will call reopen, replace instance variable with new channel and call
   * offsetToken/insertRows.
   *
   * @return new channel which was fetched after open/reopen
   */
  private SnowflakeStreamingIngestChannel openChannelForTable(final String channelName) {
    final SnowflakeStreamingIngestClient streamingIngestClient =
        StreamingClientPools.getClient(
            connectorName, taskId, pipeName, connectorConfig, streamingClientProperties);
    // Close old channel before reopening a new one. We don't want to wait for the channel to flush
    // since it will be reopened right away and the in-progress data will be lost.
    if (channelIsOpen()) {
      closeChannelWithoutFlushing();
    }

    final OpenChannelResult result = streamingIngestClient.openChannel(channelName, null);
    final ChannelStatus channelStatus = result.getChannelStatus();
    if (channelStatus.getStatusCode().equals("SUCCESS")) {
      // Capture the initial error count - errors are cumulative and don't reset on channel reopen.
      // We only want to fail on NEW errors that occur after the channel was opened.
      this.initialErrorCount = channelStatus.getRowsErrorCount();
      LOGGER.info(
          "Successfully opened streaming channel: {}, initialErrorCount: {}",
          channelName,
          this.initialErrorCount);
      return result.getChannel();
    } else {
      LOGGER.error(
          "Failed to open channel: {}, error code: {}", channelName, channelStatus.getStatusCode());
      throw ERROR_5028.getException(
          String.format(
              "Failed to open channel %s. Error code %s",
              channelName, channelStatus.getStatusCode()));
    }
  }

  @Override
  public CompletableFuture<Void> closeChannelAsync() {
    LOGGER.info("Closing streaming channel: {}", this.getChannelNameFormatV1());
    return closeChannelWrapped()
        .thenAccept(__ -> onCloseChannelSuccess())
        .exceptionally(this::tryRecoverFromCloseChannelError);
  }

  private CompletableFuture<Void> closeChannelWrapped() {
    try {
      return CompletableFuture.runAsync(() -> closeChannelWithoutFlushing());
    } catch (SFException e) {
      // Calling channel.close() can throw an SFException if the channel has been invalidated
      // already. Wrapping the exception into a CompletableFuture to keep a consistent method chain.
      CompletableFuture<Void> future = new CompletableFuture<>();
      future.completeExceptionally(e);
      return future;
    }
  }

  private boolean channelIsOpen() {
    return this.channel != null && !this.channel.isClosed();
  }

  private void onCloseChannelSuccess() {
    LOGGER.info("Successfully closed streaming channel: {}", this.getChannelNameFormatV1());
    this.telemetryService.reportKafkaPartitionUsage(this.snowflakeTelemetryChannelStatus, true);
    this.snowflakeTelemetryChannelStatus.tryUnregisterChannelJMXMetrics();
  }

  private Void tryRecoverFromCloseChannelError(Throwable e) {
    // CompletableFuture wraps errors into CompletionException.
    Throwable cause = e instanceof CompletionException ? e.getCause() : e;

    String errMsg =
        String.format(
            "Failure closing Streaming Channel name:%s msg:%s",
            this.getChannelNameFormatV1(), cause.getMessage());
    this.telemetryService.reportKafkaConnectFatalError(errMsg);

    // Only SFExceptions are swallowed. If a channel-related error occurs, it shouldn't fail a
    // connector task. The channel is going to be reopened after a rebalance, so the failed channel
    // will be invalidated anyway.
    if (cause instanceof SFException) {
      LOGGER.warn(
          "Closing Streaming Channel={} encountered an exception {}: {} {}",
          this.getChannelNameFormatV1(),
          cause.getClass(),
          cause.getMessage(),
          Arrays.toString(cause.getStackTrace()));
      return null;
    } else {
      throw new CompletionException(cause);
    }
  }

  @Override
  public boolean isChannelClosed() {
    return this.channel.isClosed();
  }

  @Override
  public String getChannelNameFormatV1() {
    return channel.getFullyQualifiedChannelName();
  }

  @VisibleForTesting
  public SnowflakeStreamingIngestChannel getChannel() {
    return this.channel;
  }

  @Override
  @VisibleForTesting
  public SnowflakeTelemetryChannelStatus getSnowflakeTelemetryChannelStatus() {
    return this.snowflakeTelemetryChannelStatus;
  }

  @Override
  public void setLatestConsumerGroupOffset(long consumerOffset) {
    offsetTracker.setLatestConsumerGroupOffset(consumerOffset);
  }

  @Override
  public long processChannelStatus(final ChannelStatus status, final boolean tolerateErrors) {
    logChannelStatus(status);
    handleChannelErrors(status, tolerateErrors);

    long committedOffset =
        parseOffsetToken(status.getLatestCommittedOffsetToken(), this.getChannelNameFormatV1());
    offsetTracker.updatePersistedOffset(committedOffset);

    if (committedOffset == NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE) {
      return NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE;
    }
    long offsetSafeToCommit = committedOffset + 1;
    setLatestConsumerGroupOffset(offsetSafeToCommit);
    return offsetSafeToCommit;
  }

  @Override
  public String getPipeName() {
    return pipeName;
  }

  @Override
  public void checkChannelStatusAndLogErrors(final boolean tolerateErrors) {
    final ChannelStatus status = channel.getChannelStatus();
    logChannelStatus(status);
    handleChannelErrors(status, tolerateErrors);
  }

  private void logChannelStatus(final ChannelStatus status) {
    LOGGER.info(
        "Channel status for channel=[{}]: databaseName=[{}], schemaName=[{}], pipeName=[{}],"
            + " channelName=[{}], statusCode=[{}], latestCommittedOffsetToken=[{}],"
            + " createdOn=[{}], rowsInsertedCount=[{}], rowsParsedCount=[{}],"
            + " rowsErrorCount=[{}], lastErrorOffsetTokenUpperBound=[{}],"
            + " lastErrorMessage=[{}], lastErrorTimestamp=[{}],"
            + " serverAvgProcessingLatency=[{}], lastRefreshedOn=[{}]",
        this.getChannelNameFormatV1(),
        status.getDatabaseName(),
        status.getSchemaName(),
        status.getPipeName(),
        status.getChannelName(),
        status.getStatusCode(),
        status.getLatestCommittedOffsetToken(),
        status.getCreatedOn(),
        status.getRowsInsertedCount(),
        status.getRowsParsedCount(),
        status.getRowsErrorCount(),
        status.getLastErrorOffsetTokenUpperBound(),
        status.getLastErrorMessage(),
        status.getLastErrorTimestamp(),
        status.getServerAvgProcessingLatency(),
        status.getLastRefreshedOn());
  }

  private void handleChannelErrors(final ChannelStatus status, final boolean tolerateErrors) {
    final long currentErrorCount = status.getRowsErrorCount();
    // Error counts are cumulative and don't reset when a channel is reopened.
    // Only fail if there are NEW errors that occurred after the channel was opened.
    final long newErrorCount = currentErrorCount - this.initialErrorCount;

    if (newErrorCount > 0) {
      final String errorMessage =
          String.format(
              "Channel [%s] has %d new errors (total: %d, initial: %d). Last error message: %s,"
                  + " last error timestamp: %s, last error offset token upper bound: %s",
              this.getChannelNameFormatV1(),
              newErrorCount,
              currentErrorCount,
              this.initialErrorCount,
              status.getLastErrorMessage(),
              status.getLastErrorTimestamp(),
              status.getLastErrorOffsetTokenUpperBound());

      this.initialErrorCount = currentErrorCount;
      if (tolerateErrors) {
        LOGGER.warn(errorMessage);
      } else {
        this.telemetryService.reportKafkaConnectFatalError(errorMessage);
        throw ERROR_5030.getException(errorMessage);
      }
    } else if (currentErrorCount > 0) {
      LOGGER.debug(
          "Channel [{}] has {} pre-existing errors from before connector startup (no new errors)",
          this.getChannelNameFormatV1(),
          currentErrorCount);
    }
  }

  /**
   * Enum representing which Streaming API is invoking the fallback supplier. ({@link
   * #streamingApiFallbackSupplier(StreamingApiFallbackInvoker)})
   *
   * <p>Fallback supplier is essentially reopening the channel and resetting the kafka offset to
   * offset found in Snowflake.
   */
  private enum StreamingApiFallbackInvoker {
    /**
     * Fallback invoked when {@link SnowflakeStreamingIngestChannel#appendRow(Map, String)} has
     * failures.
     */
    APPEND_ROW_FALLBACK,

    /**
     * Fallback invoked when {@link SnowflakeStreamingIngestChannel#getLatestCommittedOffsetToken()}
     * has failures.
     */
    GET_OFFSET_TOKEN_FALLBACK;

    /**
     * @return Used to LOG which API tried to invoke fallback function.
     */
    @Override
    public String toString() {
      return "[" + this.name() + "]";
    }
  }
}
