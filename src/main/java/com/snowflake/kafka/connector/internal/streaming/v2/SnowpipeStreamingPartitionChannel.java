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
import com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams;
import com.snowflake.kafka.connector.dlq.KafkaRecordErrorReporter;
import com.snowflake.kafka.connector.internal.DescribeTableRow;
import com.snowflake.kafka.connector.internal.KCLogger;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.metrics.MetricsJmxReporter;
import com.snowflake.kafka.connector.internal.metrics.TaskMetrics;
import com.snowflake.kafka.connector.internal.schemaevolution.ColumnTypeMapper;
import com.snowflake.kafka.connector.internal.schemaevolution.SchemaEvolutionTargetItems;
import com.snowflake.kafka.connector.internal.schemaevolution.SnowflakeColumnTypeMapper;
import com.snowflake.kafka.connector.internal.schemaevolution.SnowflakeSchemaEvolutionService;
import com.snowflake.kafka.connector.internal.schemaevolution.TableSchemaResolver;
import com.snowflake.kafka.connector.internal.schemaevolution.ValidationResultMapper;
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
import com.snowflake.kafka.connector.internal.validation.ColumnSchema;
import com.snowflake.kafka.connector.internal.validation.RowValidator;
import com.snowflake.kafka.connector.internal.validation.ValidationResult;
import com.snowflake.kafka.connector.internal.validation.metrics.ValidationMetrics;
import com.snowflake.kafka.connector.records.SnowflakeMetadataConfig;
import com.snowflake.kafka.connector.records.SnowflakeSinkRecord;
import dev.failsafe.FailsafeExecutor;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
  private static final int MAX_SCHEMA_EVOLUTION_RETRIES = 1;
  // Allows: initial attempt (depth=0) + 1 retry (depth=1) = 2 total attempts
  // Note: KC v3 and SSv1 have 0 retries, so this adds resilience beyond legacy behavior

  /**
   * Immutable container for all validation-related state.
   * This ensures atomic updates across all validation fields to prevent race conditions.
   * Thread-safe via volatile reference in the parent class.
   */
  private static class ValidationState {
    final RowValidator rowValidator;
    final Map<String, ColumnSchema> tableSchema;
    final boolean schemaEvolutionEnabled;
    final SnowflakeSchemaEvolutionService schemaEvolutionService;

    ValidationState(
        RowValidator rowValidator,
        Map<String, ColumnSchema> tableSchema,
        boolean schemaEvolutionEnabled,
        SnowflakeSchemaEvolutionService schemaEvolutionService) {
      this.rowValidator = rowValidator;
      this.tableSchema = tableSchema;
      this.schemaEvolutionEnabled = schemaEvolutionEnabled;
      this.schemaEvolutionService = schemaEvolutionService;
    }
  }
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
  private final boolean enableSchematization;

  /**
   * Used to send telemetry to Snowflake. Currently, TelemetryClient created from a Snowflake
   * Connection Object, i.e. not a session-less Client
   */
  private final SnowflakeTelemetryService telemetryService;

  private final FailsafeExecutor<Long> offsetTokenExecutor;

  private final String pipeName;

  private final Map<String, String> connectorConfig;

  private final StreamingErrorHandler streamingErrorHandler;

  private final TaskMetrics taskMetrics;

  // Client-side validation state (atomically updated for thread safety)
  private final boolean clientValidationEnabled;
  private final SnowflakeConnectionService conn;
  private final String tableName;
  private volatile ValidationState validationState;
  private final ValidationMetrics validationMetrics = new ValidationMetrics();

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
      Optional<MetricsJmxReporter> metricsJmxReporter,
      String connectorName,
      String taskId,
      StreamingErrorHandler streamingErrorHandler,
      TaskMetrics taskMetrics) {
    this.channelName = channelName;
    this.connectorConfig = connectorConfig;
    this.kafkaRecordErrorReporter = kafkaRecordErrorReporter;
    this.metadataConfig = metadataConfig;
    this.enableSchematization =
        Boolean.parseBoolean(
            connectorConfig.getOrDefault(
                KafkaConnectorConfigParams.SNOWFLAKE_ENABLE_SCHEMATIZATION,
                String.valueOf(
                    KafkaConnectorConfigParams.SNOWFLAKE_ENABLE_SCHEMATIZATION_DEFAULT)));
    this.connectorName = connectorName;
    this.taskId = taskId;
    this.streamingErrorHandler = streamingErrorHandler;
    this.taskMetrics = taskMetrics;
    this.conn = conn;
    this.tableName = tableName;

    // Initialize client-side validation
    this.clientValidationEnabled =
        Boolean.parseBoolean(
            connectorConfig.getOrDefault(
                KafkaConnectorConfigParams.SNOWFLAKE_CLIENT_VALIDATION_ENABLED,
                String.valueOf(
                    KafkaConnectorConfigParams.SNOWFLAKE_CLIENT_VALIDATION_ENABLED_DEFAULT)));

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

    // Initialize validation after channel is open
    if (clientValidationEnabled) {
      initializeValidation();
    } else {
      LOGGER.info("Client-side validation disabled for channel {}", channelName);
    }

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
    transformAndSend(kafkaSinkRecord, 0);
  }

  private void transformAndSend(SinkRecord kafkaSinkRecord, int retryDepth) {
    // Prevent infinite recursion from schema evolution retries
    if (retryDepth > MAX_SCHEMA_EVOLUTION_RETRIES) {
      LOGGER.error(
          "Max schema evolution retry depth {} exceeded for record offset {}. Routing to error handler.",
          MAX_SCHEMA_EVOLUTION_RETRIES,
          kafkaSinkRecord.kafkaOffset());
      streamingErrorHandler.handleError(
          new DataException("Max schema evolution retries exceeded after " + retryDepth + " attempts"),
          kafkaSinkRecord);
      offsetTracker.recordProcessed(kafkaSinkRecord.kafkaOffset());
      return;
    }

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
          // Client-side validation before insert
          ValidationState currentState = validationState;
          if (clientValidationEnabled && currentState != null && currentState.rowValidator != null) {
            long validationStart = System.nanoTime();
            ValidationResult validationResult = currentState.rowValidator.validateRow(transformedRecord);
            long validationDuration = (System.nanoTime() - validationStart) / 1_000_000; // Convert to ms

            if (!validationResult.isValid()) {
              validationMetrics.recordValidationFailure(
                  validationDuration, validationResult.getErrorType());
              LOGGER.warn(
                  "Validation failed at retryDepth={}. ErrorType={}, HasStructuralError={}, Column={}, Message={}",
                  retryDepth,
                  validationResult.getErrorType(),
                  validationResult.hasStructuralError(),
                  validationResult.getColumnName(),
                  validationResult.getValueError());
              if (validationResult.hasStructuralError()) {
                handleStructuralError(validationResult, kafkaSinkRecord, transformedRecord, retryDepth);
              } else {
                handleValidationError(validationResult, kafkaSinkRecord);
              }
              offsetTracker.recordProcessed(kafkaOffset);
              return;
            }
            validationMetrics.recordValidationSuccess(validationDuration);
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
                  connectorName,
                  taskId,
                  pipeName,
                  connectorConfig,
                  streamingClientProperties,
                  taskMetrics)
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

    if (this.snowflakeTelemetryChannelStatus != null
        && this.snowflakeTelemetryChannelStatus.getRecoveryCount() != null) {
      this.snowflakeTelemetryChannelStatus.getRecoveryCount().inc();
    }

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
   * Initialize client-side validation by fetching table schema using JDBC DESCRIBE TABLE.
   *
   * <p>Steps:
   * 1. Fetch schema via conn.describeTable(tableName)
   * 2. Build Map<String, ColumnSchema> from the result
   * 3. Call RowValidator.validateSchema(columns) to fail fast on unsupported types
   * 4. Instantiate RowValidator
   * 5. Determine schemaEvolutionEnabled (defaults to true for KC v4 created tables)
   * 6. Instantiate schema evolution service
   * 7. Atomically update validation state
   */
  private void initializeValidation() {
    if (!clientValidationEnabled) {
      return;
    }

    try {
      LOGGER.info(
          "Initializing client-side validation for channel {} using DESCRIBE TABLE", channelName);

      // Step 1: Fetch schema via JDBC DESCRIBE TABLE
      List<DescribeTableRow> schemaRows =
          conn.describeTable(tableName)
              .orElseThrow(() -> new RuntimeException("Table not found: " + tableName));

      // Step 2: Build column schema map
      Map<String, ColumnSchema> newTableSchema = new HashMap<>();
      for (DescribeTableRow row : schemaRows) {
        ColumnSchema col = ColumnSchema.fromDescribeTableFields(row.getColumn(), row.getType(), row.getNullable());
        // Normalize column names using same logic as validation (unquoted names are uppercased)
        String normalizedName = RowValidator.normalizeColumnName(col.getName());
        newTableSchema.put(normalizedName, col);
      }

      // Step 3: Validate schema for unsupported types
      RowValidator.validateSchema(newTableSchema);

      // Step 4: Instantiate row validator
      RowValidator newRowValidator = new RowValidator(newTableSchema);

      // Step 5: Check if schema evolution is enabled
      // For now, default to true since KC v4 sets enable_schema_evolution=true on table creation
      // TODO: Query table property when system function is available
      boolean newSchemaEvolutionEnabled = checkTableSchemaEvolutionEnabled();

      // Step 6: Instantiate schema evolution service
      SnowflakeSchemaEvolutionService newSchemaEvolutionService = new SnowflakeSchemaEvolutionService(conn);

      // Step 7: Atomically update validation state (thread-safe)
      this.validationState = new ValidationState(
          newRowValidator,
          newTableSchema,
          newSchemaEvolutionEnabled,
          newSchemaEvolutionService);

      LOGGER.info(
          "Client-side validation initialized for channel {}. Schema columns: {}, schemaEvolution: {}",
          channelName,
          newTableSchema.size(),
          newSchemaEvolutionEnabled);
    } catch (Exception e) {
      LOGGER.error("Failed to initialize client-side validation for channel {}", channelName, e);
      throw new RuntimeException("Validation initialization failed", e);
    }
  }

  /**
   * Check if schema evolution is enabled on the table by querying table parameters.
   *
   * @return true if schema evolution is enabled
   */
  private boolean checkTableSchemaEvolutionEnabled() {
    try {
      return conn.isSchemaEvolutionEnabled(tableName);
    } catch (Exception e) {
      LOGGER.warn("Failed to check schema evolution setting for table {}, defaulting to true: {}",
          tableName, e.getMessage());
      // Default to true for KC v4 created tables (they have enable_schema_evolution=true by default)
      return true;
    }
  }

  /**
   * Handle validation type error (invalid data type for column).
   * Routes to DLQ if error tolerance is enabled, otherwise throws.
   */
  private void handleValidationError(ValidationResult result, SinkRecord record) {
    String errorMsg =
        String.format(
            "Validation failed for column %s: %s",
            result.getColumnName(), result.getValueError());
    streamingErrorHandler.handleError(new DataException(errorMsg), record);
  }

  /**
   * Handle structural validation error (extra columns, NOT NULL violations).
   *
   * <p>If schema evolution is enabled, triggers client-side DDL to evolve the schema, refreshes
   * the cached schema, and retries the record.
   *
   * <p>If schema evolution is disabled, routes the error to DLQ.
   */
  private void handleStructuralError(
      ValidationResult result, SinkRecord record, Map<String, Object> transformedRecord, int retryDepth) {
    ValidationState currentState = validationState;
    if (currentState != null && currentState.schemaEvolutionEnabled) {
      // Save current working state before attempting schema evolution (issue #8 fix - matches KC v3)
      ValidationState previousState = currentState;

      try {
        LOGGER.info(
            "Schema mismatch detected for channel {}. Attempting schema evolution. "
                + "ExtraCols={}, MissingNotNull={}, NullNotNull={}",
            channelName,
            result.getExtraColNames(),
            result.getMissingNotNullColNames(),
            result.getNullValueForNotNullColNames());

        // Map validation result to schema evolution target
        SchemaEvolutionTargetItems items =
            ValidationResultMapper.mapToSchemaEvolutionItems(result, tableName);

        // Evolve schema via client-side DDL
        currentState.schemaEvolutionService.evolveSchemaIfNeeded(items, record);

        // Refresh schema by closing and reopening channel
        LOGGER.info("Refreshing channel {} after schema evolution", channelName);
        closeChannelWithoutFlushing();
        this.channel = openChannelForTable(channelName);

        // Note: initializeValidation() already called by openChannelForTable() at line 666

        // Record metrics BEFORE retry (issue #4 fix - ensures metric is recorded even if retry fails)
        validationMetrics.recordSchemaEvolution(true);

        // Retry the record with fresh schema (increment retry depth)
        transformAndSend(record, retryDepth + 1);
        return; // CRITICAL: Prevent parent from recording offset again (issue #1 fix)

        // Note: Logging moved to prevent unreachable code warning
        // Schema evolution success is confirmed when transformAndSend completes without exception
      } catch (Exception e) {
        LOGGER.error("Schema evolution failed for channel {}, restoring previous channel state", channelName, e);

        // Restore previous working state (issue #8 fix)
        this.validationState = previousState;

        LOGGER.info("Restored previous validation state. Channel {} remains operational for subsequent records.", channelName);

        // Record schema evolution failure on exception (issue #5 fix)
        validationMetrics.recordSchemaEvolution(false);
        streamingErrorHandler.handleError(e, record);
        // Track offset to prevent infinite retry loop (issue #2 fix - matches KC v3 behavior)
        offsetTracker.recordProcessed(record.kafkaOffset());
      }
    } else {
      // Schema evolution disabled - route to DLQ
      String errorMsg =
          String.format(
              "Schema mismatch (evolution disabled): extraCols=%s, missingNotNull=%s, nullNotNull=%s",
              result.getExtraColNames(),
              result.getMissingNotNullColNames(),
              result.getNullValueForNotNullColNames());
      streamingErrorHandler.handleError(new DataException(errorMsg), record);
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
  private SnowflakeStreamingIngestChannel openChannelForTable(final String channelName) {
    final SnowflakeStreamingIngestClient streamingIngestClient =
        StreamingClientPools.getClient(
            connectorName,
            taskId,
            pipeName,
            connectorConfig,
            streamingClientProperties,
            taskMetrics);

    // Close old channel before reopening a new one. We don't want to wait for the channel to flush
    // since it will be reopened right away and the in-progress data will be lost.
    if (channelIsOpen()) {
      closeChannelWithoutFlushing();
    }

    final OpenChannelResult result;
    try (TaskMetrics.TimingContext ignored = taskMetrics.timeChannelOpen()) {
      result = streamingIngestClient.openChannel(channelName, null);
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

      // Initialize client-side validation after channel is opened
      initializeValidation();

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

  @Override
  public String getChannelName() {
    return channel.getChannelName();
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

  /**
   * Get validation metrics for this channel.
   * Used for aggregation and export at the task/connector level.
   *
   * @return ValidationMetrics instance
   */
  public ValidationMetrics getValidationMetrics() {
    return validationMetrics;
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
