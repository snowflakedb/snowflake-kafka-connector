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
import com.snowflake.kafka.connector.internal.DescribeTableRow;
import com.snowflake.kafka.connector.internal.KCLogger;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException;
import com.snowflake.kafka.connector.internal.metrics.TaskMetrics;
import com.snowflake.kafka.connector.internal.schemaevolution.SchemaEvolutionTargetItems;
import com.snowflake.kafka.connector.internal.schemaevolution.SnowflakeSchemaEvolutionService;
import com.snowflake.kafka.connector.internal.schemaevolution.ValidationResultMapper;
import com.snowflake.kafka.connector.internal.streaming.StreamingErrorHandler;
import com.snowflake.kafka.connector.internal.streaming.TopicPartitionChannelInsertionException;
import com.snowflake.kafka.connector.internal.streaming.channel.TopicPartitionChannel;
import com.snowflake.kafka.connector.internal.streaming.telemetry.SnowflakeTelemetryChannelCreation;
import com.snowflake.kafka.connector.internal.streaming.telemetry.SnowflakeTelemetryChannelStatus;
import com.snowflake.kafka.connector.internal.streaming.v2.channel.PartitionOffsetTracker;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryService;
import com.snowflake.kafka.connector.internal.validation.ColumnSchema;
import com.snowflake.kafka.connector.internal.validation.RowValidator;
import com.snowflake.kafka.connector.internal.validation.ValidationResult;
import com.snowflake.kafka.connector.records.SnowflakeMetadataConfig;
import com.snowflake.kafka.connector.records.SnowflakeSinkRecord;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

public class SnowpipeStreamingPartitionChannel implements TopicPartitionChannel {
  private static final KCLogger LOGGER =
      new KCLogger(SnowpipeStreamingPartitionChannel.class.getName());

  private SnowflakeStreamingIngestChannel channel;

  private final PartitionOffsetTracker offsetTracker;

  // Tracks the initial error count when the channel was opened.
  // Used to detect NEW errors (current error count > initial error count) since error counts
  // are cumulative and don't reset when a channel is reopened.
  private long initialErrorCount = 0;

  private final String channelName;

  private final SnowflakeTelemetryChannelStatus snowflakeTelemetryChannelStatus;

  private final SnowflakeMetadataConfig metadataConfig;
  private final boolean enableSchematization;

  /**
   * Used to send telemetry to Snowflake. Currently, TelemetryClient created from a Snowflake
   * Connection Object, i.e. not a session-less Client
   */
  private final SnowflakeTelemetryService telemetryService;

  private final String pipeName;

  private final SnowflakeStreamingIngestClient streamingClient;
  private final ExecutorService openChannelIoExecutor;

  private final StreamingErrorHandler streamingErrorHandler;

  private final TaskMetrics taskMetrics;

  // Client-side validation fields
  private final boolean clientValidationEnabled;
  private final SnowflakeConnectionService conn;
  private final String tableName;
  private volatile RowValidator rowValidator;
  private volatile SnowflakeSchemaEvolutionService schemaEvolutionService;
  private volatile Map<String, ColumnSchema> tableSchema;
  private final boolean hasSchemaEvolutionPermission;

  public SnowpipeStreamingPartitionChannel(
      String tableName,
      String channelName,
      String pipeName,
      SnowflakeStreamingIngestClient streamingClient,
      ExecutorService openChannelIoExecutor,
      SnowflakeTelemetryService telemetryService,
      SnowflakeTelemetryChannelStatus snowflakeTelemetryChannelStatus,
      PartitionOffsetTracker offsetTracker,
      SnowflakeMetadataConfig metadataConfig,
      boolean enableSchematization,
      StreamingErrorHandler streamingErrorHandler,
      TaskMetrics taskMetrics,
      boolean clientValidationEnabled,
      boolean hasSchemaEvolutionPermission,
      SnowflakeConnectionService conn) {
    this.channelName = channelName;
    this.pipeName = pipeName;
    this.streamingClient = streamingClient;
    this.openChannelIoExecutor = openChannelIoExecutor;
    this.metadataConfig = metadataConfig;
    this.enableSchematization = enableSchematization;
    this.streamingErrorHandler = streamingErrorHandler;
    this.taskMetrics = taskMetrics;
    this.telemetryService = telemetryService;
    this.snowflakeTelemetryChannelStatus = snowflakeTelemetryChannelStatus;
    this.offsetTracker = offsetTracker;
    this.clientValidationEnabled = clientValidationEnabled && enableSchematization;
    this.hasSchemaEvolutionPermission = hasSchemaEvolutionPermission;
    this.conn = conn;
    this.tableName = tableName;

    LOGGER.info(
        "Initializing SnowpipeStreamingPartitionChannel channel: {}, pipe: {}",
        channelName,
        pipeName);

    OpenChannelResult openChannelResult = openChannelForTable(channelName);
    final long lastCommittedOffsetToken =
        parseOffsetToken(
            openChannelResult.getChannelStatus().getLatestCommittedOffsetToken(), channelName);
    LOGGER.info("New channel {} has offset token {}", channelName, lastCommittedOffsetToken);
    offsetTracker.initializeFromSnowflake(lastCommittedOffsetToken);
    this.channel = openChannelResult.getChannel();

    if (this.clientValidationEnabled) {
      initializeValidation();
    } else {
      LOGGER.info("Client-side validation disabled for channel {}", channelName);
    }

    this.telemetryService.reportKafkaPartitionStart(
        new SnowflakeTelemetryChannelCreation(tableName, channelName, System.currentTimeMillis()));
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
      final SnowflakeSinkRecord record =
          SnowflakeSinkRecord.from(kafkaSinkRecord, metadataConfig, enableSchematization);

      if (record.isBroken()) {
        LOGGER.debug("Broken record offset:{}, topic:{}", kafkaOffset, kafkaSinkRecord.topic());
        streamingErrorHandler.handleError(record.getBrokenReason(), kafkaSinkRecord);
      } else {
        // If we reach here, it means we should ingest a record (possibly empty for tombstones)
        final Map<String, Object> transformedRecord =
            record.getContentWithMetadata(metadataConfig.shouldIncludeAllMetadata());
        if (!transformedRecord.isEmpty()) {
          if (clientValidationEnabled && rowValidator != null) {
            ValidationResult validationResult = rowValidator.validateRow(transformedRecord);

            if (!validationResult.isValid()) {
              if (validationResult.hasStructuralError()) {
                handleStructuralError(validationResult, kafkaSinkRecord, transformedRecord);
              } else {
                handleValidationError(validationResult, kafkaSinkRecord);
              }
              offsetTracker.recordProcessed(kafkaOffset);
              return;
            }
          }

          insertRowWithFallback(transformedRecord, kafkaOffset);
        }
      }
      // Always update processedOffset after processing, even for broken records
      offsetTracker.recordProcessed(kafkaOffset);
    } catch (TopicPartitionChannelInsertionException ex) {
      // Suppressing the exception because other channels might still continue to ingest
      LOGGER.warn(
          "Failed to insert row for channel:{}. Will be retried by Kafka. Exception: {}",
          this.channelName,
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
          LOGGER.info("Starting flush for channel: {}", this.channelName);

          streamingClient.initiateFlush();

          final long targetOffset = offsetTracker.getLastAppendRowsOffset();
          WaitForLastOffsetCommittedPolicy.getPolicy(
              () -> {
                long offsetCommittedToBackend = fetchLatestCommittedOffsetFromSnowflake();
                if (offsetCommittedToBackend == targetOffset) {
                  return true;
                }
                throw ERROR_5027.getException();
              });

          LOGGER.info("Completed flush for channel: {}", this.channelName);
        });
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
          getChannel().appendRow(transformedRecord, Long.toString(offset));
          offsetTracker.recordAppended(offset);
        },
        (Throwable ex) -> {
          reopenChannel("APPEND_ROW_FALLBACK");
          throw new TopicPartitionChannelInsertionException(
              String.format("Failed to insert rows into channel %s.", this.channelName), ex);
        },
        this.channelName);
  }

  private static void closeChannelWithoutFlushing(SnowflakeStreamingIngestChannel channel) {
    try {
      channel.close(false /* waitForFlush */, Duration.ZERO);
    } catch (TimeoutException e) {
      // This should never happen since we are not waiting for the channel to flush.
      throw new RuntimeException(
          String.format("Error closing channel %s: %s", channel.getChannelName(), e.getMessage()));
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
   * @param reason Reason for the channel recovery. Used for logging.
   * @return offset which was last present in Snowflake
   */
  private void reopenChannel(final String reason) {
    LOGGER.warn("{} Channel {} recovery initiated", reason, this.channelName);

    if (this.snowflakeTelemetryChannelStatus != null
        && this.snowflakeTelemetryChannelStatus.getRecoveryCount() != null) {
      this.snowflakeTelemetryChannelStatus.getRecoveryCount().inc();
    }

    // Close old channel before reopening a new one. We don't want to wait for the channel to flush
    // since it will be reopened right away and the in-progress data will be lost.
    if (!channel.isClosed()) {
      closeChannelWithoutFlushing(channel);
    }
    OpenChannelResult openChannelResult = openChannelForTable(channelName);

    final long offsetRecoveredFromSnowflake =
        parseOffsetToken(
            openChannelResult.getChannelStatus().getLatestCommittedOffsetToken(), channelName);

    if (offsetRecoveredFromSnowflake == NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE) {
      LOGGER.info(
          "{} Channel {} has no offset token. Will use consumer group offset, currently {}",
          reason,
          this.channelName,
          offsetTracker.consumerGroupOffsetRef().get());
    }

    offsetTracker.resetAfterRecovery(offsetRecoveredFromSnowflake);
    this.channel = openChannelResult.getChannel();

    LOGGER.info(
        "{} Channel {} recovery complete, offsetRecoveredFromSnowflake={}",
        reason,
        this.channelName,
        offsetRecoveredFromSnowflake);
  }

  /**
   * Parses an offset token string into a long value.
   *
   * @param offsetToken the offset token string (may be null)
   * @param channelNameForLogging used in error messages
   * @return the parsed long, or {@link
   *     TopicPartitionChannel#NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE} if null
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
    return fetchLatestOffsetFromChannel(this.getChannel());
  }

  private static long fetchLatestOffsetFromChannel(SnowflakeStreamingIngestChannel channel) {
    String offsetToken = channel.getLatestCommittedOffsetToken();
    LOGGER.info(
        "Fetched offsetToken for channelName:{}, offset:{}", channel.getChannelName(), offsetToken);
    return parseOffsetToken(offsetToken, channel.getChannelName());
  }

  private void initializeValidation() {
    try {
      Optional<List<DescribeTableRow>> describeResult = conn.describeTable(tableName);
      if (!describeResult.isPresent()) {
        LOGGER.warn(
            "Table {} not found during validation initialization. "
                + "Client-side validation will be disabled for channel {}",
            tableName,
            channelName);
        return;
      }

      this.tableSchema = new HashMap<>();
      for (DescribeTableRow row : describeResult.get()) {
        ColumnSchema colSchema =
            ColumnSchema.fromDescribeTableFields(row.getColumn(), row.getType(), row.getNullable());
        this.tableSchema.put(row.getColumn(), colSchema);
      }

      RowValidator.validateSchema(this.tableSchema);

      this.rowValidator = new RowValidator(this.tableSchema);
      this.schemaEvolutionService = new SnowflakeSchemaEvolutionService(conn);

      LOGGER.info(
          "Client-side validation enabled for channel {}. Table {} has {} columns",
          channelName,
          tableName,
          this.tableSchema.size());
    } catch (Exception e) {
      LOGGER.warn(
          "Failed to initialize client-side validation for channel {}. "
              + "Validation will be disabled. Error: {}",
          channelName,
          e.getMessage());
      this.rowValidator = null;
    }
  }

  private void refreshTableSchema() {
    initializeValidation();
  }

  private void handleValidationError(ValidationResult result, SinkRecord record) {
    String errorMsg =
        String.format(
            "Validation failed for column %s: %s", result.getColumnName(), result.getValueError());
    streamingErrorHandler.handleError(new DataException(errorMsg), record);
  }

  private void handleStructuralError(
      ValidationResult result, SinkRecord record, Map<String, Object> transformedRecord) {
    LOGGER.info(
        "handleStructuralError for channel {}: hasSchemaEvolutionPermission={}, extraCols={},"
            + " missingNotNull={}",
        channelName,
        hasSchemaEvolutionPermission,
        result.getExtraColNames(),
        result.getMissingNotNullColNames());
    if (!hasSchemaEvolutionPermission) {
      String errorMsg =
          String.format(
              "Structural validation error (schema evolution disabled): extraCols=%s,"
                  + " missingNotNull=%s",
              result.getExtraColNames(), result.getMissingNotNullColNames());
      LOGGER.info("Routing to DLQ for channel {}: {}", channelName, errorMsg);
      streamingErrorHandler.handleError(new DataException(errorMsg), record);
      return;
    }

    try {
      LOGGER.info("Attempting schema evolution for channel {}, table {}", channelName, tableName);
      SchemaEvolutionTargetItems items =
          ValidationResultMapper.mapToSchemaEvolutionItems(result, tableName);
      schemaEvolutionService.evolveSchemaIfNeeded(items, record);

      refreshTableSchema();

      ValidationResult retryResult = result;
      if (rowValidator != null) {
        retryResult = rowValidator.validateRow(transformedRecord);
        if (retryResult.isValid()) {
          insertRowWithFallback(transformedRecord, record.kafkaOffset());
          return;
        }
      }

      String errorMsg =
          String.format(
              "Schema mismatch after evolution attempt: extraCols=%s, missingNotNull=%s",
              retryResult.getExtraColNames(), retryResult.getMissingNotNullColNames());
      streamingErrorHandler.handleError(new DataException(errorMsg), record);
    } catch (SnowflakeKafkaConnectorException e) {
      LOGGER.error("Schema evolution failed for table {}", tableName, e);
      throw e;
    }
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
  private OpenChannelResult openChannelForTable(final String channelName) {
    final OpenChannelResult result;
    try (TaskMetrics.TimingContext ignored = taskMetrics.timeChannelOpen()) {
      result = streamingClient.openChannel(channelName, null);
    }

    taskMetrics.incChannelOpenCount();

    final ChannelStatus channelStatus = result.getChannelStatus();
    if (channelStatus.getStatusCode().equals("SUCCESS")) {
      // Capture the initial error count - errors are cumulative and don't reset on channel reopen.
      // We only want to fail on NEW errors that occur after the channel was opened.
      this.initialErrorCount = channelStatus.getRowsErrorCount();
      LOGGER.info(
          "Successfully opened streaming channel: {}, initialErrorCount: {}",
          channelName,
          this.initialErrorCount);
      return result;
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
    return CompletableFuture.runAsync(
        () -> {
          LOGGER.info("Closing streaming channel {}", this.channelName);
          try {
            if (!channel.isClosed()) {
              closeChannelWithoutFlushing(channel);
            }
            LOGGER.info("Successfully closed streaming channel {}", this.channelName);
          } catch (Exception e) {
            tryRecoverFromCloseChannelError(e);
          } finally {
            this.telemetryService.reportKafkaPartitionUsage(
                this.snowflakeTelemetryChannelStatus, true);
            this.snowflakeTelemetryChannelStatus.tryUnregisterChannelJMXMetrics();
          }
        });
  }

  private void tryRecoverFromCloseChannelError(Throwable e) {
    String errMsg =
        String.format(
            "Failure closing streaming channel %s, error: %s", this.channelName, e.getMessage());
    this.telemetryService.reportKafkaConnectFatalError(errMsg);

    // Only SFExceptions are swallowed.
    // If a channel-related error occurs, it shouldn't fail a connector task.
    // The channel is going to be reopened after a rebalance, so the failed channel
    // will be invalidated anyway.
    if (e instanceof SFException) {
      LOGGER.warn(
          "Encountered {} when closing streaming channel {}: {}. Stack trace: {}",
          e.getClass(),
          this.channelName,
          e.getMessage(),
          Arrays.toString(e.getStackTrace()));
    } else {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean isChannelClosed() {
    return this.getChannel().isClosed();
  }

  @Override
  public String getChannelNameFormatV1() {
    return getChannel().getFullyQualifiedChannelName();
  }

  @Override
  public String getChannelName() {
    return channelName;
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
        parseOffsetToken(status.getLatestCommittedOffsetToken(), this.channelName);
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

  private void logChannelStatus(final ChannelStatus status) {
    LOGGER.info(
        "Channel status for channel=[{}]: databaseName=[{}], schemaName=[{}], pipeName=[{}],"
            + " channelName=[{}], statusCode=[{}], latestCommittedOffsetToken=[{}],"
            + " createdOn=[{}], rowsInsertedCount=[{}], rowsParsedCount=[{}],"
            + " rowsErrorCount=[{}], lastErrorOffsetTokenUpperBound=[{}],"
            + " lastErrorMessage=[{}], lastErrorTimestamp=[{}],"
            + " serverAvgProcessingLatency=[{}], lastRefreshedOn=[{}]",
        this.channelName,
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
              this.channelName,
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
          this.channelName,
          currentErrorCount);
    }
  }
}
