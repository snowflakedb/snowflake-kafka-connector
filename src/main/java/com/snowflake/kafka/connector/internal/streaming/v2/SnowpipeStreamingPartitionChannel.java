package com.snowflake.kafka.connector.internal.streaming.v2;

import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_SSV1_OFFSET_MIGRATION;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_SSV1_OFFSET_MIGRATION_INCLUDE_CONNECTOR_NAME;
import static com.snowflake.kafka.connector.internal.SnowflakeErrors.ERROR_5027;
import static com.snowflake.kafka.connector.internal.SnowflakeErrors.ERROR_5028;
import static com.snowflake.kafka.connector.internal.SnowflakeErrors.ERROR_5030;

import com.google.common.annotations.VisibleForTesting;
import com.snowflake.ingest.streaming.ChannelStatus;
import com.snowflake.ingest.streaming.OpenChannelResult;
import com.snowflake.ingest.streaming.SFException;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import com.snowflake.kafka.connector.config.SinkTaskConfig;
import com.snowflake.kafka.connector.config.SnowflakeValidation;
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
import com.snowflake.kafka.connector.internal.streaming.channel.InsertResult;
import com.snowflake.kafka.connector.internal.streaming.channel.TopicPartitionChannel;
import com.snowflake.kafka.connector.internal.streaming.telemetry.SnowflakeTelemetryChannelCreation;
import com.snowflake.kafka.connector.internal.streaming.telemetry.SnowflakeTelemetryChannelStatus;
import com.snowflake.kafka.connector.internal.streaming.telemetry.SnowflakeTelemetrySsv1Migration;
import com.snowflake.kafka.connector.internal.streaming.v2.channel.PartitionOffsetTracker;
import com.snowflake.kafka.connector.internal.streaming.v2.migration.Ssv1MigrationMode;
import com.snowflake.kafka.connector.internal.streaming.v2.migration.Ssv1MigrationResponse;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryService;
import com.snowflake.kafka.connector.internal.validation.ColumnSchema;
import com.snowflake.kafka.connector.internal.validation.RowValidator;
import com.snowflake.kafka.connector.internal.validation.ValidationResult;
import com.snowflake.kafka.connector.records.SnowflakeSinkRecord;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

public class SnowpipeStreamingPartitionChannel implements TopicPartitionChannel {
  private static final KCLogger LOGGER =
      new KCLogger(SnowpipeStreamingPartitionChannel.class.getName());

  private volatile CompletableFuture<SnowflakeStreamingIngestChannel> channel;
  private final AtomicBoolean cancelled = new AtomicBoolean(false);

  private final PartitionOffsetTracker offsetTracker;

  // Tracks the initial error count when the channel was opened.
  // Used to detect NEW errors (current error count > initial error count) since error counts
  // are cumulative and don't reset when a channel is reopened.
  private long initialErrorCount = 0;

  /**
   * Maximum wall-clock duration we keep retrying recovery before giving up. Sized for real SSv2
   * pipe failover propagation (observed 2-4 min). Package-private + non-final so tests can
   * override.
   */
  @VisibleForTesting static Duration MAX_RECOVERY_DURATION = Duration.ofMinutes(6);

  /**
   * Consecutive recovery counter. Incremented each time the fallback reopens the channel, reset to
   * zero on every successful appendRow. Drives the exponential backoff in {@link
   * AppendRowWithFallbackPolicy#computeBackoffDelay(int)} — it is no longer used as the giving-up
   * threshold; that is now {@link #MAX_RECOVERY_DURATION}.
   */
  private int consecutiveRecoveryCount = 0;

  /**
   * Wall-clock timestamp (nanos) of the first failure in the current recovery streak; -1 when no
   * recovery is in progress. Reset on every successful appendRow.
   */
  private long firstFailureNanos = -1L;

  private final String channelName;

  private final SnowflakeTelemetryChannelStatus snowflakeTelemetryChannelStatus;

  private final SinkTaskConfig taskConfig;

  /**
   * Used to send telemetry to Snowflake. Currently, TelemetryClient created from a Snowflake
   * Connection Object, i.e. not a session-less Client
   */
  private final SnowflakeTelemetryService telemetryService;

  private final String pipeName;

  private volatile SnowflakeStreamingIngestClient streamingClient;
  private final ClientRecreator clientRecreator;
  private final ExecutorService openChannelIoExecutor;

  private final StreamingErrorHandler streamingErrorHandler;

  private final TaskMetrics taskMetrics;

  // SSv1 offset migration
  private final Optional<String> ssv1ChannelName;

  // Client-side validation fields
  private final SnowflakeConnectionService conn;
  private final String tableName;
  private volatile RowValidator rowValidator;
  private volatile SnowflakeSchemaEvolutionService schemaEvolutionService;
  private volatile Map<String, ColumnSchema> tableSchema;
  private final boolean shouldEvolveSchema;

  public SnowpipeStreamingPartitionChannel(
      String tableName,
      String channelName,
      String pipeName,
      SnowflakeStreamingIngestClient streamingClient,
      ClientRecreator clientRecreator,
      ExecutorService openChannelIoExecutor,
      SnowflakeTelemetryService telemetryService,
      SnowflakeTelemetryChannelStatus snowflakeTelemetryChannelStatus,
      PartitionOffsetTracker offsetTracker,
      SinkTaskConfig taskConfig,
      StreamingErrorHandler streamingErrorHandler,
      TaskMetrics taskMetrics,
      boolean shouldEvolveSchema,
      SnowflakeConnectionService conn,
      Optional<String> ssv1ChannelName) {
    this.channelName = channelName;
    this.pipeName = pipeName;
    this.streamingClient = streamingClient;
    this.clientRecreator = clientRecreator;
    this.openChannelIoExecutor = openChannelIoExecutor;
    this.taskConfig = taskConfig;
    this.streamingErrorHandler = streamingErrorHandler;
    this.taskMetrics = taskMetrics;
    this.telemetryService = telemetryService;
    this.snowflakeTelemetryChannelStatus = snowflakeTelemetryChannelStatus;
    this.offsetTracker = offsetTracker;
    this.shouldEvolveSchema = shouldEvolveSchema;
    this.conn = conn;
    this.tableName = tableName;
    this.ssv1ChannelName = ssv1ChannelName;

    LOGGER.info(
        "Initializing SnowpipeStreamingPartitionChannel channel: {}, pipe: {}",
        channelName,
        pipeName);

    this.channel =
        CompletableFuture.supplyAsync(
            () -> {
              OpenChannelResult openChannelResult = openChannelForTable(channelName);
              long offsetRecoveredFromSnowflake = parseOrMigrateOffsetToken(openChannelResult);
              offsetTracker.initializeFromSnowflake(offsetRecoveredFromSnowflake);
              return openChannelResult.getChannel();
            },
            openChannelIoExecutor);

    if (taskConfig.getValidation() == SnowflakeValidation.CLIENT_SIDE) {
      initializeValidation();
    } else {
      LOGGER.info("Client-side validation disabled for channel {}", channelName);
    }

    this.telemetryService.reportKafkaPartitionStart(
        new SnowflakeTelemetryChannelCreation(tableName, channelName, System.currentTimeMillis()));
  }

  @Override
  public InsertResult insertRecord(
      SinkRecord kafkaSinkRecord, boolean isFirstRowPerPartitionInBatch) {
    if (offsetTracker.shouldProcess(kafkaSinkRecord.kafkaOffset(), isFirstRowPerPartitionInBatch)) {
      return transformAndSend(kafkaSinkRecord);
    }
    // shouldProcess returned false — the record is either a duplicate (already processed) or
    // skipped because needToSkipCurrentBatch is set after a mid-batch recovery. Nothing to do.
    return InsertResult.PROCESSED;
  }

  private InsertResult transformAndSend(SinkRecord kafkaSinkRecord) {
    try {
      final long kafkaOffset = kafkaSinkRecord.kafkaOffset();
      final SnowflakeSinkRecord record =
          SnowflakeSinkRecord.from(
              kafkaSinkRecord,
              taskConfig.getMetadataConfig(),
              taskConfig.isEnableSchematization(),
              taskConfig.isEnableColumnIdentifierNormalization());

      if (record.isBroken()) {
        LOGGER.debug("Broken record offset:{}, topic:{}", kafkaOffset, kafkaSinkRecord.topic());
        streamingErrorHandler.handleError(record.getBrokenReason(), kafkaSinkRecord);
        // If we reach here, the error was tolerated (errors.tolerance=all)
        snowflakeTelemetryChannelStatus.incErrorToleratedCount();
      } else {
        // If we reach here, it means we should ingest a record (possibly empty for tombstones)
        final Map<String, Object> row =
            record.getContentWithMetadata(
                taskConfig.getMetadataConfig().shouldIncludeAllMetadata());
        if (!row.isEmpty()) {
          if (taskConfig.getValidation() == SnowflakeValidation.CLIENT_SIDE
              && rowValidator != null) {
            ValidationResult validationResult = rowValidator.validateRow(row);

            if (!validationResult.isValid()) {
              if (validationResult.hasStructuralError()) {
                handleStructuralError(validationResult, kafkaSinkRecord, record, row);
              } else {
                handleValidationError(validationResult, kafkaSinkRecord);
              }
              offsetTracker.recordProcessed(kafkaOffset);
              return InsertResult.PROCESSED;
            }
          }

          if (!insertRowWithFallback(row, kafkaOffset)) {
            // Fallback fired: the record was NOT inserted. The fallback's resetAfterRecovery
            // already issued the authoritative Kafka seek to (committed + 1). Do NOT call
            // recordProcessed() here — that would advance processedOffset past the recovery
            // point and cause replayed offsets to be skipped. See SNOW-3344243. The batch
            // loop receives RECOVERY_IN_FLIGHT and skips its own end-of-batch rewind for
            // this partition, so the authoritative seek survives.
            return InsertResult.RECOVERY_IN_FLIGHT;
          }
        }
      }
      // Always update processedOffset after processing, even for broken records
      offsetTracker.recordProcessed(kafkaOffset);
      return InsertResult.PROCESSED;
    } catch (BackpressureException ex) {
      snowflakeTelemetryChannelStatus.incBackpressureRetryCount();
      return InsertResult.BACKPRESSURE_TRIGGERED;
    } catch (TopicPartitionChannelInsertionException ex) {
      // Suppressing the exception because other channels might still continue to ingest
      LOGGER.warn(
          "Failed to insert row for channel:{}. Will be retried by Kafka. Exception: {}",
          this.channelName,
          ex);
      return InsertResult.PROCESSED;
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

          try {
            streamingClient.initiateFlush();
          } catch (SFException e) {
            if (ClientRecreationException.isClientInvalidError(e)) {
              // Called from stop(); next task instance will get a fresh client and Kafka
              // will redeliver uncommitted rows.
              LOGGER.warn(
                  "Skipping flush for channel {}: client is invalid ({}). "
                      + "Uncommitted data will be replayed on task restart.",
                  this.channelName,
                  e.getErrorCodeName());
              return;
            }
            throw e;
          }

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
   * Uses {@link AppendRowWithFallbackPolicy} to reopen the channel if insertRows throws {@link
   * SFException}.
   *
   * <p>We have deliberately not performed retries on insertRows because it might slow down overall
   * ingestion and introduce lags in committing offsets to Kafka.
   *
   * <p>Note that insertRows API does perform channel validation which might throw SFException if
   * channel is invalidated.
   */
  /**
   * @return true if the record was inserted successfully, false if the fallback fired (record was
   *     NOT inserted)
   */
  private boolean insertRowWithFallback(Map<String, Object> row, long offset) {
    return AppendRowWithFallbackPolicy.executeWithFallback(
        () -> {
          LOGGER.trace("Inserting transformed record: {}, offset: {}", row, offset);
          getChannel().appendRow(row, Long.toString(offset));
          offsetTracker.recordAppended(offset);
          consecutiveRecoveryCount = 0;
          firstFailureNanos = -1L;
        },
        (Throwable ex) -> {
          consecutiveRecoveryCount++;
          if (firstFailureNanos == -1L) {
            firstFailureNanos = System.nanoTime();
          }
          Duration elapsed = Duration.ofNanos(System.nanoTime() - firstFailureNanos);
          if (elapsed.compareTo(MAX_RECOVERY_DURATION) > 0) {
            // Recovery is stuck. Throw ConnectException (NOT
            // TopicPartitionChannelInsertionException — transformAndSend's catch swallows that
            // and commits the offset without calling recordProcessed, silently losing the
            // record). ConnectException fails the Kafka Connect task so offsets are not
            // committed; the task retries recovery on restart.
            LOGGER.error(
                "Channel {} exceeded max recovery duration ({}s) after {} attempts, giving up",
                this.channelName,
                MAX_RECOVERY_DURATION.getSeconds(),
                consecutiveRecoveryCount);
            throw new ConnectException(
                String.format(
                    "Channel %s could not be recovered within %ds (%d attempts)."
                        + " Check Snowflake service health.",
                    this.channelName,
                    MAX_RECOVERY_DURATION.getSeconds(),
                    consecutiveRecoveryCount),
                ex);
          }
          Duration backoff = AppendRowWithFallbackPolicy.computeBackoffDelay(
              consecutiveRecoveryCount);
          LOGGER.warn(
              "Channel {} recovery attempt {} (elapsed {}s / {}s budget), backoff {}ms",
              this.channelName,
              consecutiveRecoveryCount,
              elapsed.getSeconds(),
              MAX_RECOVERY_DURATION.getSeconds(),
              backoff.toMillis());
          try {
            Thread.sleep(backoff.toMillis());
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            return;
          }
          boolean clientInvalid = ClientRecreationException.isClientInvalidError(ex);
          reopenChannel(clientInvalid ? "CLIENT_RECREATION" : "APPEND_ROW_FALLBACK", clientInvalid);
          snowflakeTelemetryChannelStatus.incAppendRowFallbackCount();
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
  private void reopenChannel(final String reason, boolean recreateClient) {
    LOGGER.warn("{} Channel {} recovery initiated", reason, this.channelName);

    if (this.snowflakeTelemetryChannelStatus != null) {
      this.snowflakeTelemetryChannelStatus.incRecoveryCount();
    }

    if (recreateClient) {
      // Client recreation is a higher-level recovery than channel reopen — the fresh client can
      // unblock a pipe that channel-reopen alone couldn't. The pool's CAS ensures only one new
      // client is created even if multiple channels call this concurrently. Reset the retry
      // budget so subsequent channel-reopen attempts on the new client get their full window.
      try {
        this.streamingClient = clientRecreator.recreate(this.streamingClient);
        snowflakeTelemetryChannelStatus.incClientRecreationCount();
        consecutiveRecoveryCount = 0;
        firstFailureNanos = -1L;
      } catch (RuntimeException e) {
        LOGGER.warn(
            "{} Channel {} client recreation failed (failover may still be propagating): {}",
            reason,
            this.channelName,
            e.getMessage());
      }
    }

    this.channel =
        this.channel
            // Close old channel before reopening a new one. We don't want to wait for the channel
            // to flush since it will be reopened right away and the in-progress data will be lost.
            .thenAccept(
                oldChannel -> {
                  if (!oldChannel.isClosed()) {
                    LOGGER.info(
                        "{} Channel {} is not closed before reopening", reason, this.channelName);
                    closeChannelWithoutFlushing(oldChannel);
                  }
                })
            // If the previous init failed, there is no old channel to close.
            .exceptionally(
                initFailure -> {
                  LOGGER.warn(
                      "{} Channel {} had a failed initialization, skipping close: {}",
                      reason,
                      this.channelName,
                      initFailure.getMessage());
                  return null;
                })
            .thenApply(
                ignored -> {
                  OpenChannelResult openChannelResult = openChannelForTable(channelName);
                  final long offsetRecoveredFromSnowflake =
                      parseOrMigrateOffsetToken(openChannelResult);

                  if (offsetRecoveredFromSnowflake == NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE) {
                    LOGGER.info(
                        "{} Channel {} has no offset token. Will use consumer group offset,"
                            + " currently {}",
                        reason,
                        this.channelName,
                        offsetTracker.consumerGroupOffsetRef().get());
                  }

                  offsetTracker.resetAfterRecovery(offsetRecoveredFromSnowflake);

                  LOGGER.info(
                      "{} Channel {} recovery complete, offsetRecoveredFromSnowflake={}",
                      reason,
                      this.channelName,
                      offsetRecoveredFromSnowflake);

                  return openChannelResult.getChannel();
                });
  }

  /**
   * Parses the SSv2 offset from the open-channel result, and if SSv2 has no committed offset yet,
   * attempts SSv1 offset migration based on the configured {@link Ssv1MigrationMode}.
   *
   * <p>Used by both the initial channel open (constructor) and {@link #reopenChannel} so that
   * migration behavior is consistent regardless of whether the first open succeeded or failed.
   */
  private long parseOrMigrateOffsetToken(OpenChannelResult openChannelResult) {
    final long ssv2Offset =
        parseOffsetToken(
            openChannelResult.getChannelStatus().getLatestCommittedOffsetToken(), channelName);
    LOGGER.info("Channel {} has SSv2 offset token {}", channelName, ssv2Offset);

    long effectiveOffset = ssv2Offset;

    // Only consult SSv1 when SSv2 has no committed offset yet (first-time migration).
    // Once SSv2 has its own offset, it is authoritative.
    if (ssv2Offset == NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE
        && taskConfig.getSsv1MigrationMode() != Ssv1MigrationMode.SKIP) {
      // migrateSsv1ChannelOffset calls SYSTEM$MIGRATE_SSV1_CHANNEL_OFFSET which:
      //   - returns ssv1ChannelFound=false if the SSv1 channel doesn't exist
      //   - returns ssv1ChannelFound=true, migratedOffset=null if found but no committed offset
      //   - returns ssv1ChannelFound=true, migratedOffset=N on success (also writes to SSv2 in FDB)
      //   - THROWS for SQL/network errors (must not silently proceed --
      //     falling through to consumer group offset could cause duplicates)
      String ssv1Channel =
          ssv1ChannelName.orElseThrow(
              () ->
                  new IllegalStateException(
                      "ssv1ChannelName must be present when migration mode is "
                          + taskConfig.getSsv1MigrationMode()));
      Ssv1MigrationResponse response =
          conn.migrateSsv1ChannelOffset(tableName, ssv1Channel, channelName, pipeName);
      Long migrated = response.getMigratedOffset();
      if (migrated != null) {
        effectiveOffset = migrated;
        LOGGER.info(
            "SSv2 channel {} has no offset yet, migrating SSv1 offset for {}: {}",
            channelName,
            ssv1Channel,
            effectiveOffset);
      } else if (!response.isSsv1ChannelFound()) {
        LOGGER.info("SSv1 channel {} not found for SSv2 channel {}", ssv1Channel, channelName);
      } else {
        LOGGER.info(
            "SSv1 channel {} exists but has no committed offset for SSv2 channel {}",
            ssv1Channel,
            channelName);
      }
      telemetryService.reportSsv1Migration(
          new SnowflakeTelemetrySsv1Migration(
              tableName, channelName, ssv1Channel, taskConfig.getSsv1MigrationMode(), response));
      if (!response.isSsv1ChannelFound()
          && taskConfig.getSsv1MigrationMode() == Ssv1MigrationMode.STRICT) {
        throw new ConnectException(
            "Snowpipe Streaming Classic channel "
                + ssv1Channel
                + " not found but the offset token migration mode is set to 'strict'. This can"
                + " happen if new topics are added after migrating from version 3 of the"
                + " connector or if an incorrect value is provided for "
                + SNOWFLAKE_SSV1_OFFSET_MIGRATION_INCLUDE_CONNECTOR_NAME
                + " or the connector name. Validate your settings or set "
                + SNOWFLAKE_SSV1_OFFSET_MIGRATION
                + " to 'best_effort' or 'skip' to fall through to the Kafka consumer group"
                + " offset.");
      }
    }

    return effectiveOffset;
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
        this.snowflakeTelemetryChannelStatus.setValidationDisabled();
        return;
      }

      this.tableSchema = new HashMap<>();
      for (DescribeTableRow row : describeResult.get()) {
        ColumnSchema colSchema =
            ColumnSchema.fromDescribeTableFields(
                row.getColumn(),
                row.getType(),
                row.getNullable(),
                row.hasDefault(),
                row.isAutoincrement());
        this.tableSchema.put(row.getColumn(), colSchema);
      }

      RowValidator.validateSchema(this.tableSchema);

      this.rowValidator = new RowValidator(this.tableSchema);
      this.schemaEvolutionService = new SnowflakeSchemaEvolutionService(conn);

      LOGGER.info(
          "Client-side validation enabled for channel {}. Table {} has {} columns,"
              + " enableSchematization={}",
          channelName,
          tableName,
          this.tableSchema.size(),
          taskConfig.isEnableSchematization());
    } catch (Exception e) {
      LOGGER.warn(
          "Failed to initialize client-side validation for channel {}. "
              + "Validation will be disabled. Error: {}",
          channelName,
          e.getMessage());
      this.snowflakeTelemetryChannelStatus.setValidationDisabled();
      this.rowValidator = null;
    }
  }

  private void refreshTableSchema() {
    initializeValidation();
  }

  private void handleValidationError(
      ValidationResult result, SinkRecord originalRecordForReporting) {
    if (streamingErrorHandler.isLogErrors()) {
      LOGGER.warn(
          "Client-side validation failure [{}] channel={}, column={}, error={}, offset={}",
          result.getErrorType(),
          channelName,
          result.getColumnName(),
          result.getValueError(),
          originalRecordForReporting.kafkaOffset());
    }

    snowflakeTelemetryChannelStatus.incValidationFailureCount();

    String errorMsg =
        String.format(
            "Validation failed for column %s: %s", result.getColumnName(), result.getValueError());
    streamingErrorHandler.handleError(new DataException(errorMsg), originalRecordForReporting);
    snowflakeTelemetryChannelStatus.incErrorToleratedCount();
  }

  private void handleStructuralError(
      ValidationResult result,
      SinkRecord originalRecordForReporting,
      SnowflakeSinkRecord snowflakeRecord,
      Map<String, Object> row) {
    if (streamingErrorHandler.isLogErrors()) {
      LOGGER.warn(
          "Client-side structural validation failure [{}] channel={}, "
              + "hasSchemaEvolutionPermission={}, extraCols={}, missingNotNull={}, "
              + "nullNotNull={}, offset={}",
          result.getErrorType(),
          channelName,
          shouldEvolveSchema,
          result.getExtraColNames(),
          result.getMissingNotNullColNames(),
          result.getNullValueForNotNullColNames(),
          originalRecordForReporting.kafkaOffset());
    }

    if (!shouldEvolveSchema) {
      snowflakeTelemetryChannelStatus.incValidationFailureCount();

      String errorMsg =
          String.format(
              "Structural validation error (schema evolution disabled): extraCols=%s,"
                  + " missingNotNull=%s",
              result.getExtraColNames(), result.getMissingNotNullColNames());
      LOGGER.info("Routing to DLQ for channel {}: {}", channelName, errorMsg);
      streamingErrorHandler.handleError(new DataException(errorMsg), originalRecordForReporting);
      snowflakeTelemetryChannelStatus.incErrorToleratedCount();
      return;
    }

    try {
      LOGGER.info("Attempting schema evolution for channel {}, table {}", channelName, tableName);
      SchemaEvolutionTargetItems items =
          ValidationResultMapper.mapToSchemaEvolutionItems(result, tableName);
      schemaEvolutionService.evolveSchemaIfNeeded(items, snowflakeRecord);

      refreshTableSchema();

      ValidationResult retryResult = result;
      if (rowValidator != null) {
        retryResult = rowValidator.validateRow(row);
        if (retryResult.isValid()) {
          insertRowWithFallback(row, originalRecordForReporting.kafkaOffset());
          return;
        }
      }

      snowflakeTelemetryChannelStatus.incValidationFailureCount();
      snowflakeTelemetryChannelStatus.incSchemaEvolutionFailureCount();

      String errorMsg =
          String.format(
              "Schema mismatch after evolution attempt: extraCols=%s, missingNotNull=%s",
              retryResult.getExtraColNames(), retryResult.getMissingNotNullColNames());
      streamingErrorHandler.handleError(new DataException(errorMsg), originalRecordForReporting);
      snowflakeTelemetryChannelStatus.incErrorToleratedCount();
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
    if (cancelled.get()) {
      throw new CancellationException("Channel " + channelName + " was cancelled before opening");
    }

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
    LOGGER.info("Closing streaming channel {}", this.channelName);
    cancelled.set(true);
    return channel
        .thenAccept(
            c -> {
              try {
                if (!c.isClosed()) {
                  closeChannelWithoutFlushing(c);
                }
                LOGGER.info("Successfully closed streaming channel {}", this.channelName);
              } catch (RuntimeException e) {
                tryRecoverFromCloseChannelError(e);
              } finally {
                this.telemetryService.reportKafkaPartitionUsage(
                    this.snowflakeTelemetryChannelStatus, true);
                this.snowflakeTelemetryChannelStatus.tryUnregisterChannelJMXMetrics();
              }
            })
        .exceptionally(
            e -> {
              Throwable cause = e.getCause() != null ? e.getCause() : e;
              if (cause instanceof java.util.concurrent.CancellationException) {
                LOGGER.info(
                    "Channel {} was cancelled before opening, nothing to close", this.channelName);
              } else {
                LOGGER.warn(
                    "Channel {} failed during initialization, skipping close: {}",
                    this.channelName,
                    cause.getMessage());
              }
              this.snowflakeTelemetryChannelStatus.tryUnregisterChannelJMXMetrics();
              return null;
            });
  }

  private void tryRecoverFromCloseChannelError(RuntimeException e) {
    String errMsg =
        String.format(
            "Failure closing streaming channel %s, error: %s", this.channelName, e.getMessage());
    this.telemetryService.reportKafkaConnectFatalError(
        errMsg, this.channelName, this.tableName, this.pipeName);

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
      throw e;
    }
  }

  @Override
  public boolean isInitializing() {
    return !channel.isDone();
  }

  @Override
  public void awaitInitialization() {
    channel.join();
  }

  @Override
  public boolean isChannelClosed() {
    try {
      return this.getChannel().isClosed();
    } catch (RuntimeException e) {
      // If the channel failed to initialize, we consider it closed.
      LOGGER.warn(
          "Channel {} failed to initialize, treating as closed: {}", channelName, e.getMessage());
      return true;
    }
  }

  @Override
  public String getChannelNameFormatV1() {
    return getChannel().getFullyQualifiedChannelName();
  }

  @Override
  public String getChannelName() {
    return channelName;
  }

  /**
   * Blocks until the channel initialization future completes and returns the underlying SDK
   * channel.
   *
   * <p><b>Warning:</b> Do not call this from the channel construction future body (the lambda
   * passed to {@code CompletableFuture.supplyAsync} in the constructor). That future is what
   * populates {@code this.channel}; calling {@code join()} on it from within itself will deadlock.
   */
  @VisibleForTesting
  public SnowflakeStreamingIngestChannel getChannel() {
    try {
      return this.channel.join();
    } catch (CompletionException e) {
      if (e.getCause() instanceof RuntimeException) {
        throw (RuntimeException) e.getCause();
      }
      throw new RuntimeException(e.getCause());
    }
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

    this.snowflakeTelemetryChannelStatus.updateFromChannelStatus(status);

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
        this.telemetryService.reportKafkaConnectFatalError(
            errorMessage, this.channelName, this.tableName, this.pipeName);
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
