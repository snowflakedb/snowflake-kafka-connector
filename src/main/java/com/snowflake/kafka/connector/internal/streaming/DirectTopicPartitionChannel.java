package com.snowflake.kafka.connector.internal.streaming;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.ENABLE_CHANNEL_OFFSET_TOKEN_MIGRATION_CONFIG;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.ENABLE_CHANNEL_OFFSET_TOKEN_MIGRATION_DEFAULT;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.ERRORS_TOLERANCE_CONFIG;
import static com.snowflake.kafka.connector.internal.streaming.StreamingUtils.DURATION_BETWEEN_GET_OFFSET_TOKEN_RETRY;
import static com.snowflake.kafka.connector.internal.streaming.StreamingUtils.MAX_GET_OFFSET_TOKEN_RETRIES;
import static java.time.temporal.ChronoUnit.SECONDS;
import static org.apache.kafka.common.record.TimestampType.NO_TIMESTAMP_TYPE;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.dlq.KafkaRecordErrorReporter;
import com.snowflake.kafka.connector.internal.KCLogger;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException;
import com.snowflake.kafka.connector.internal.metrics.MetricsJmxReporter;
import com.snowflake.kafka.connector.internal.streaming.channel.TopicPartitionChannel;
import com.snowflake.kafka.connector.internal.streaming.schemaevolution.InsertErrorMapper;
import com.snowflake.kafka.connector.internal.streaming.schemaevolution.SchemaEvolutionService;
import com.snowflake.kafka.connector.internal.streaming.schemaevolution.SchemaEvolutionTargetItems;
import com.snowflake.kafka.connector.internal.streaming.telemetry.SnowflakeTelemetryChannelCreation;
import com.snowflake.kafka.connector.internal.streaming.telemetry.SnowflakeTelemetryChannelStatus;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryService;
import com.snowflake.kafka.connector.records.RecordService;
import com.snowflake.kafka.connector.records.RecordServiceFactory;
import com.snowflake.kafka.connector.records.SnowflakeJsonSchema;
import com.snowflake.kafka.connector.records.SnowflakeRecordContent;
import dev.failsafe.Failsafe;
import dev.failsafe.Fallback;
import dev.failsafe.RetryPolicy;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import net.snowflake.ingest.streaming.*;
import net.snowflake.ingest.utils.SFException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;

public class DirectTopicPartitionChannel implements TopicPartitionChannel {
  private static final KCLogger LOGGER = new KCLogger(DirectTopicPartitionChannel.class.getName());

  // used to communicate to the streaming ingest's insertRows API
  // This is non final because we might decide to get the new instance of Channel
  private SnowflakeStreamingIngestChannel channel;

  // -------- private final fields -------- //

  // This offset represents the data persisted in Snowflake. More specifically it is the Snowflake
  // offset determined from the insertRows API call. It is set after calling the fetchOffsetToken
  // API for this channel
  private final AtomicLong offsetPersistedInSnowflake =
      new AtomicLong(NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE);

  // This offset represents the data buffered in KC. More specifically it is the KC offset to ensure
  // exactly once functionality. On the creation it is set to the latest committed token in
  // Snowflake (see offsetPersistedInSnowflake) and updated on each new row from KC.
  private final AtomicLong processedOffset =
      new AtomicLong(NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE);

  // This offset is would not be required for buffer-less channel, but we add it to keep buffered
  // and non-buffered
  // channel versions compatible.
  private final AtomicLong currentConsumerGroupOffset =
      new AtomicLong(NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE);

  // Indicates whether we need to skip and discard any leftover rows in the current batch, this
  // could happen when the channel gets invalidated and reset, then anything left in the buffer
  // should be skipped
  private boolean needToSkipCurrentBatch = false;

  private final SnowflakeStreamingIngestClient streamingIngestClient;

  // Topic partition Object from connect consisting of topic and partition
  private final TopicPartition topicPartition;

  /* Channel Name is computed from topic and partition */
  private final String channelNameFormatV1;

  /* table is required for opening the channel */
  private final String tableName;

  /* Error handling, DB, schema, Snowflake URL and other snowflake specific connector properties are defined here. */
  private final Map<String, String> sfConnectorConfig;

  /* Responsible for converting records to Json */
  private final RecordService recordService;

  /* Responsible for returning errors to DLQ if records have failed to be ingested. */
  private final KafkaRecordErrorReporter kafkaRecordErrorReporter;

  private final SchemaEvolutionService schemaEvolutionService;

  /**
   * Available from {@link org.apache.kafka.connect.sink.SinkTask} which has access to various
   * utility methods.
   */
  private final SinkTaskContext sinkTaskContext;

  /* Error related properties */

  // If set to true, we will send records to DLQ provided DLQ name is valid.
  private final boolean errorTolerance;

  // Whether to log errors to log file.
  private final boolean logErrors;

  // Set to false if DLQ topic is null or empty. True if it is a valid string in config
  private final boolean isDLQTopicSet;

  // Whether schematization has been enabled.
  private final boolean enableSchematization;

  // Whether schema evolution could be done on this channel
  private final boolean enableSchemaEvolution;

  // Reference to the Snowflake connection service
  private final SnowflakeConnectionService conn;

  private final SnowflakeTelemetryChannelStatus snowflakeTelemetryChannelStatus;

  private final InsertErrorMapper insertErrorMapper;

  private final ChannelOffsetTokenMigrator channelOffsetTokenMigrator;

  /**
   * Used to send telemetry to Snowflake. Currently, TelemetryClient created from a Snowflake
   * Connection Object, i.e. not a session-less Client
   */
  private final SnowflakeTelemetryService telemetryServiceV2;

  /** Testing only, initialize TopicPartitionChannel without the connection service */
  @VisibleForTesting
  public DirectTopicPartitionChannel(
      SnowflakeStreamingIngestClient streamingIngestClient,
      TopicPartition topicPartition,
      final String channelNameFormatV1,
      final String tableName,
      final Map<String, String> sfConnectorConfig,
      KafkaRecordErrorReporter kafkaRecordErrorReporter,
      SinkTaskContext sinkTaskContext,
      SnowflakeConnectionService conn,
      SnowflakeTelemetryService telemetryService,
      SchemaEvolutionService schemaEvolutionService,
      InsertErrorMapper insertErrorMapper) {
    this(
        streamingIngestClient,
        topicPartition,
        channelNameFormatV1,
        tableName,
        false, /* No schema evolution permission */
        sfConnectorConfig,
        kafkaRecordErrorReporter,
        sinkTaskContext,
        conn,
        RecordServiceFactory.createRecordService(
            false, Utils.isSchematizationEnabled(sfConnectorConfig)),
        telemetryService,
        false,
        null,
        schemaEvolutionService,
        insertErrorMapper);
  }

  /**
   * @param streamingIngestClient client created specifically for this task
   * @param topicPartition topic partition corresponding to this Streaming Channel
   *     (TopicPartitionChannel)
   * @param channelNameFormatV1 channel Name which is deterministic for topic and partition
   * @param tableName table to ingest in snowflake
   * @param hasSchemaEvolutionPermission if the role has permission to perform schema evolution on
   *     the table
   * @param sfConnectorConfig configuration set for snowflake connector
   * @param kafkaRecordErrorReporter kafka errpr reporter for sending records to DLQ
   * @param sinkTaskContext context on Kafka Connect's runtime
   * @param conn the snowflake connection service
   * @param recordService record service for processing incoming offsets from Kafka
   * @param telemetryService Telemetry Service which includes the Telemetry Client, sends Json data
   *     to Snowflake
   * @param insertErrorMapper Mapper to map insert errors to schema evolution items
   */
  public DirectTopicPartitionChannel(
      SnowflakeStreamingIngestClient streamingIngestClient,
      TopicPartition topicPartition,
      final String channelNameFormatV1,
      final String tableName,
      boolean hasSchemaEvolutionPermission,
      final Map<String, String> sfConnectorConfig,
      KafkaRecordErrorReporter kafkaRecordErrorReporter,
      SinkTaskContext sinkTaskContext,
      SnowflakeConnectionService conn,
      RecordService recordService,
      SnowflakeTelemetryService telemetryService,
      boolean enableCustomJMXMonitoring,
      MetricsJmxReporter metricsJmxReporter,
      SchemaEvolutionService schemaEvolutionService,
      InsertErrorMapper insertErrorMapper) {
    final long startTime = System.currentTimeMillis();

    this.streamingIngestClient = Preconditions.checkNotNull(streamingIngestClient);
    Preconditions.checkState(!streamingIngestClient.isClosed());
    this.topicPartition = Preconditions.checkNotNull(topicPartition);
    this.channelNameFormatV1 = Preconditions.checkNotNull(channelNameFormatV1);
    this.tableName = Preconditions.checkNotNull(tableName);
    this.sfConnectorConfig = Preconditions.checkNotNull(sfConnectorConfig);
    this.kafkaRecordErrorReporter = Preconditions.checkNotNull(kafkaRecordErrorReporter);
    this.sinkTaskContext = Preconditions.checkNotNull(sinkTaskContext);
    this.conn = conn;

    this.recordService = recordService;
    this.telemetryServiceV2 = Preconditions.checkNotNull(telemetryService);

    /* Error properties */
    this.errorTolerance = StreamingUtils.tolerateErrors(this.sfConnectorConfig);
    this.logErrors = StreamingUtils.logErrors(this.sfConnectorConfig);
    this.isDLQTopicSet =
        !Strings.isNullOrEmpty(StreamingUtils.getDlqTopicName(this.sfConnectorConfig));

    /* Schematization related properties */
    this.enableSchematization = Utils.isSchematizationEnabled(this.sfConnectorConfig);

    this.enableSchemaEvolution = this.enableSchematization && hasSchemaEvolutionPermission;
    this.schemaEvolutionService = schemaEvolutionService;

    this.channelOffsetTokenMigrator = new ChannelOffsetTokenMigrator(conn, telemetryService);

    if (isEnableChannelOffsetMigration(sfConnectorConfig)) {
      /* Channel Name format V2 is computed from connector name, topic and partition */
      final String channelNameFormatV2 =
          TopicPartitionChannel.generateChannelNameFormatV2(
              this.channelNameFormatV1, this.conn.getConnectorName());
      channelOffsetTokenMigrator.migrateChannelOffsetWithRetry(
          this.tableName, channelNameFormatV2, this.channelNameFormatV1);
    }

    // Open channel and reset the offset in kafka
    this.channel = Preconditions.checkNotNull(openChannelForTable(this.enableSchemaEvolution));
    final long lastCommittedOffsetToken = fetchOffsetTokenWithRetry();
    this.offsetPersistedInSnowflake.set(lastCommittedOffsetToken);
    this.processedOffset.set(lastCommittedOffsetToken);

    // setup telemetry and metrics
    String connectorName =
        conn == null || conn.getConnectorName() == null || conn.getConnectorName().isEmpty()
            ? "default_connector_name"
            : conn.getConnectorName();
    this.snowflakeTelemetryChannelStatus =
        new SnowflakeTelemetryChannelStatus(
            tableName,
            connectorName,
            channelNameFormatV1,
            startTime,
            enableCustomJMXMonitoring,
            metricsJmxReporter,
            this.offsetPersistedInSnowflake,
            this.processedOffset,
            this.currentConsumerGroupOffset);
    this.telemetryServiceV2.reportKafkaPartitionStart(
        new SnowflakeTelemetryChannelCreation(this.tableName, this.channelNameFormatV1, startTime));

    this.insertErrorMapper = insertErrorMapper;

    if (lastCommittedOffsetToken != NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE) {
      this.sinkTaskContext.offset(this.topicPartition, lastCommittedOffsetToken + 1L);
    } else {
      LOGGER.info(
          "TopicPartitionChannel:{}, offset token is NULL, will rely on Kafka to send us the"
              + " correct offset instead",
          this.getChannelNameFormatV1());
    }
  }

  /**
   * Checks if the configuration provided in Snowflake Kafka Connect has set {@link
   * SnowflakeSinkConnectorConfig#ENABLE_CHANNEL_OFFSET_TOKEN_MIGRATION_CONFIG} to any value. If not
   * set, it fetches the default value.
   *
   * <p>If the returned is false, system function for channel offset migration will not be called
   * and Channel name will use V1 format.
   *
   * @param sfConnectorConfig customer provided json config
   * @return true is enabled, false otherwise
   */
  private boolean isEnableChannelOffsetMigration(Map<String, String> sfConnectorConfig) {
    boolean isEnableChannelOffsetMigration =
        Boolean.parseBoolean(
            sfConnectorConfig.getOrDefault(
                SnowflakeSinkConnectorConfig.ENABLE_CHANNEL_OFFSET_TOKEN_MIGRATION_CONFIG,
                Boolean.toString(ENABLE_CHANNEL_OFFSET_TOKEN_MIGRATION_DEFAULT)));
    if (!isEnableChannelOffsetMigration) {
      LOGGER.info(
          "Config:{} is disabled for connector:{}",
          ENABLE_CHANNEL_OFFSET_TOKEN_MIGRATION_CONFIG,
          conn.getConnectorName());
    }
    return isEnableChannelOffsetMigration;
  }

  @Override
  public void insertRecord(SinkRecord kafkaSinkRecord, boolean isFirstRowPerPartitionInBatch) {
    final long currentOffsetPersistedInSnowflake = this.offsetPersistedInSnowflake.get();
    final long currentProcessedOffset = this.processedOffset.get();

    // for backwards compatibility - set the consumer offset to be the first one received from kafka
    if (currentConsumerGroupOffset.get() == NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE) {
      this.currentConsumerGroupOffset.set(kafkaSinkRecord.kafkaOffset());
    }

    // Reset the value if it's a new batch
    if (isFirstRowPerPartitionInBatch) {
      needToSkipCurrentBatch = false;
    }

    // Simply skip inserting into the buffer if the row should be ignored after channel reset
    if (needToSkipCurrentBatch) {
      LOGGER.info(
          "Ignore inserting offset:{} for channel:{} because we recently reset offset in"
              + " Kafka. currentProcessedOffset:{}",
          kafkaSinkRecord.kafkaOffset(),
          this.getChannelNameFormatV1(),
          currentProcessedOffset);
      return;
    }
    // Accept the incoming record only if we don't have a valid offset token at server side, or the
    // incoming record offset is 1 + the processed offset
    if (currentProcessedOffset == NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE
        || kafkaSinkRecord.kafkaOffset() >= currentProcessedOffset + 1) {
      transformAndSend(kafkaSinkRecord);
    } else {
      LOGGER.warn(
          "Channel {} - skipping current record - expected offset {} but received {}. The current"
              + " offset stored in Snowflake: {}",
          this.getChannelNameFormatV1(),
          currentProcessedOffset,
          kafkaSinkRecord.kafkaOffset(),
          currentOffsetPersistedInSnowflake);
    }
  }

  private boolean shouldConvertContent(final Object content) {
    return content != null && !(content instanceof SnowflakeRecordContent);
  }

  /**
   * This would always return false for streaming ingest use case since isBroken field is never set.
   * isBroken is set only when using Custom snowflake converters and the content was not json
   * serializable.
   *
   * <p>For Community converters, the kafka record will not be sent to Kafka connector if the record
   * is not serializable.
   */
  private boolean isRecordBroken(final SinkRecord record) {
    return isContentBroken(record.value()) || isContentBroken(record.key());
  }

  private boolean isContentBroken(final Object content) {
    return content != null && ((SnowflakeRecordContent) content).isBroken();
  }

  private SinkRecord handleNativeRecord(SinkRecord record, boolean isKey) {
    SnowflakeRecordContent newSFContent;
    Schema schema = isKey ? record.keySchema() : record.valueSchema();
    Object content = isKey ? record.key() : record.value();
    try {
      newSFContent = new SnowflakeRecordContent(schema, content, true);
    } catch (Exception e) {
      LOGGER.error("Native content parser error:\n{}", e.getMessage());
      try {
        // try to serialize this object and send that as broken record
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ObjectOutputStream os = new ObjectOutputStream(out);
        os.writeObject(content);
        newSFContent = new SnowflakeRecordContent(out.toByteArray());
      } catch (Exception serializeError) {
        LOGGER.error(
            "Failed to convert broken native record to byte data:\n{}",
            serializeError.getMessage());
        throw e;
      }
    }
    // create new sinkRecord
    Schema keySchema = isKey ? new SnowflakeJsonSchema() : record.keySchema();
    Object keyContent = isKey ? newSFContent : record.key();
    Schema valueSchema = isKey ? record.valueSchema() : new SnowflakeJsonSchema();
    Object valueContent = isKey ? record.value() : newSFContent;
    return new SinkRecord(
        record.topic(),
        record.kafkaPartition(),
        keySchema,
        keyContent,
        valueSchema,
        valueContent,
        record.kafkaOffset(),
        record.timestamp(),
        record.timestampType(),
        record.headers());
  }

  private void transformAndSend(SinkRecord kafkaSinkRecord) {
    try {
      Map<String, Object> transformedRecord = transformDataBeforeSending(kafkaSinkRecord);
      if (!transformedRecord.isEmpty()) {
        InsertValidationResponse response =
            insertRowWithFallback(transformedRecord, kafkaSinkRecord.kafkaOffset());
        this.processedOffset.set(kafkaSinkRecord.kafkaOffset());

        if (response.hasErrors()) {
          LOGGER.warn(
              "insertRow for channel:{} resulted in errors:{},",
              this.getChannelNameFormatV1(),
              response.hasErrors());

          handleInsertRowFailure(response.getInsertErrors(), kafkaSinkRecord);
        }
      }

    } catch (TopicPartitionChannelInsertionException ex) {
      // Suppressing the exception because other channels might still continue to ingest
      LOGGER.warn(
          String.format(
              "[INSERT_BUFFERED_RECORDS] Failure inserting rows for channel:%s",
              this.getChannelNameFormatV1()),
          ex);
    }
  }

  /**
   * Uses {@link Fallback} API to reopen the channel if insertRows throws {@link SFException}.
   *
   * <p>We have deliberately not performed retries on insertRows because it might slow down overall
   * ingestion and introduce lags in committing offsets to Kafka.
   *
   * <p>Note that insertRows API does perform channel validation which might throw SFException if
   * channel is invalidated.
   *
   * <p>It can also send errors {@link
   * net.snowflake.ingest.streaming.InsertValidationResponse.InsertError} in form of response inside
   * {@link InsertValidationResponse}
   *
   * @return InsertValidationResponse a response that wraps around InsertValidationResponse
   */
  private InsertValidationResponse insertRowWithFallback(
      Map<String, Object> transformedRecord, long offset) {
    Fallback<Object> reopenChannelFallbackExecutorForInsertRows =
        Fallback.builder(
                executionAttemptedEvent -> {
                  insertRowFallbackSupplier(executionAttemptedEvent.getLastException());
                })
            .handle(SFException.class)
            .onFailedAttempt(
                event ->
                    LOGGER.warn(
                        String.format(
                            "Failed Attempt to invoke the insertRows API for channel: %s",
                            getChannelNameFormatV1()),
                        event.getLastException()))
            .onFailure(
                event ->
                    LOGGER.error(
                        String.format(
                            "%s Failed to open Channel or fetching offsetToken for channel:%s",
                            StreamingApiFallbackInvoker.INSERT_ROWS_FALLBACK,
                            this.getChannelNameFormatV1()),
                        event.getException()))
            .build();

    return Failsafe.with(reopenChannelFallbackExecutorForInsertRows)
        .get(() -> this.channel.insertRow(transformedRecord, Long.toString(offset)));
  }

  /**
   * We will reopen the channel on {@link SFException} and reset offset in kafka. But, we will throw
   * a custom exception to show that the streamingBuffer was not added into Snowflake.
   *
   * @throws TopicPartitionChannelInsertionException exception is thrown after channel reopen has
   *     been successful and offsetToken was fetched from Snowflake
   */
  private void insertRowFallbackSupplier(Throwable ex)
      throws TopicPartitionChannelInsertionException {
    final long offsetRecoveredFromSnowflake =
        streamingApiFallbackSupplier(StreamingApiFallbackInvoker.INSERT_ROWS_FALLBACK);
    throw new TopicPartitionChannelInsertionException(
        String.format(
            "%s Failed to insert rows for channel:%s. Recovered offset from Snowflake is:%s",
            StreamingApiFallbackInvoker.INSERT_ROWS_FALLBACK,
            this.getChannelNameFormatV1(),
            offsetRecoveredFromSnowflake),
        ex);
  }

  /**
   * Invoked only when {@link InsertValidationResponse} has errors.
   *
   * <p>This function checks if we need to log errors, send it to DLQ or just ignore and throw
   * exception.
   *
   * @param insertErrors errors from validation response. (Only if it has errors)
   */
  private void handleInsertRowFailure(
      List<InsertValidationResponse.InsertError> insertErrors, SinkRecord kafkaSinkRecord) {
    if (enableSchemaEvolution) {
      InsertValidationResponse.InsertError insertError = insertErrors.get(0);
      SchemaEvolutionTargetItems schemaEvolutionTargetItems =
          insertErrorMapper.mapToSchemaEvolutionItems(insertError, this.channel.getTableName());
      if (schemaEvolutionTargetItems.hasDataForSchemaEvolution()) {
        try {
          schemaEvolutionService.evolveSchemaIfNeeded(
              schemaEvolutionTargetItems, kafkaSinkRecord, channel.getTableSchema());
          streamingApiFallbackSupplier(
              StreamingApiFallbackInvoker.INSERT_ROWS_SCHEMA_EVOLUTION_FALLBACK);
        } catch (SnowflakeKafkaConnectorException e) {
          LOGGER.error(
              "Error while performing schema evolution for channel:{}",
              this.getChannelNameFormatV1(),
              e);
          if (Objects.equals(e.getCode(), SnowflakeErrors.ERROR_5026.getCode())) {
            handleError(Collections.singletonList(e), kafkaSinkRecord);
          } else {
            throw e;
          }
        }

        return;
      }
    }

    handleError(
        insertErrors.stream()
            .map(InsertValidationResponse.InsertError::getException)
            .collect(Collectors.toList()),
        kafkaSinkRecord);
  }

  private void handleError(List<Exception> insertErrors, SinkRecord kafkaSinkRecord) {
    if (logErrors) {
      for (Exception insertError : insertErrors) {
        LOGGER.error("Insert Row Error message:{}", insertError.getMessage());
      }
    }
    if (errorTolerance) {
      if (!isDLQTopicSet) {
        LOGGER.warn(
            "{} is set, however {} is not. The message will not be added to the Dead Letter Queue"
                + " topic.",
            ERRORS_TOLERANCE_CONFIG,
            ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG);
      } else {
        LOGGER.warn(
            "Adding the message to Dead Letter Queue topic: {}",
            ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG);
        this.kafkaRecordErrorReporter.reportError(
            kafkaSinkRecord,
            insertErrors.stream()
                .findFirst()
                .orElseThrow(
                    () ->
                        new IllegalStateException(
                            "Reported record error, however exception list is empty.")));
      }
    } else {
      final String errMsg =
          String.format(
              "Error inserting Records using Streaming API with msg:%s",
              insertErrors.get(0).getMessage());
      this.telemetryServiceV2.reportKafkaConnectFatalError(errMsg);
      throw new DataException(errMsg, insertErrors.get(0));
    }
  }

  // TODO: SNOW-529755 POLL committed offsets in background thread

  @Override
  public long getOffsetSafeToCommitToKafka() {
    final long committedOffsetInSnowflake = fetchOffsetTokenWithRetry();
    if (committedOffsetInSnowflake == NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE) {
      return NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE;
    } else {
      // Return an offset which is + 1 of what was present in snowflake.
      // Idea of sending + 1 back to Kafka is that it should start sending offsets after task
      // restart from this offset
      return committedOffsetInSnowflake + 1;
    }
  }

  @Override
  @VisibleForTesting
  public long fetchOffsetTokenWithRetry() {
    final RetryPolicy<Long> offsetTokenRetryPolicy =
        RetryPolicy.<Long>builder()
            .handle(SFException.class)
            .withDelay(DURATION_BETWEEN_GET_OFFSET_TOKEN_RETRY)
            .withMaxAttempts(MAX_GET_OFFSET_TOKEN_RETRIES)
            .onRetry(
                event ->
                    LOGGER.warn(
                        "[OFFSET_TOKEN_RETRY_POLICY] retry for getLatestCommittedOffsetToken. Retry"
                            + " no:{}, message:{}",
                        event.getAttemptCount(),
                        event.getLastException().getMessage()))
            .build();

    /*
     * The fallback function to execute when all retries from getOffsetToken have exhausted.
     * Fallback is only attempted on SFException
     */
    Fallback<Long> offsetTokenFallbackExecutor =
        Fallback.builder(
                () ->
                    streamingApiFallbackSupplier(
                        StreamingApiFallbackInvoker.GET_OFFSET_TOKEN_FALLBACK))
            .handle(SFException.class)
            .onFailure(
                event ->
                    LOGGER.error(
                        "[OFFSET_TOKEN_FALLBACK] Failed to open Channel/fetch offsetToken for"
                            + " channel:{}, exception:{}",
                        this.getChannelNameFormatV1(),
                        event.getException().toString()))
            .build();

    // Read it from reverse order. Fetch offsetToken, apply retry policy and then fallback.
    return Failsafe.with(offsetTokenFallbackExecutor)
        .onFailure(
            event ->
                LOGGER.error(
                    "[OFFSET_TOKEN_RETRY_FAILSAFE] Failure to fetch offsetToken even after retry"
                        + " and fallback from snowflake for channel:{}, elapsedTimeSeconds:{}",
                    this.getChannelNameFormatV1(),
                    event.getElapsedTime().get(SECONDS),
                    event.getException()))
        .compose(offsetTokenRetryPolicy)
        .get(this::fetchLatestCommittedOffsetFromSnowflake);
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
    SnowflakeStreamingIngestChannel newChannel = reopenChannel(streamingApiFallbackInvoker);

    LOGGER.warn(
        "{} Fetching offsetToken after re-opening the channel:{}",
        streamingApiFallbackInvoker,
        this.getChannelNameFormatV1());
    long offsetRecoveredFromSnowflake = fetchLatestOffsetFromChannel(newChannel);

    resetChannelMetadataAfterRecovery(
        streamingApiFallbackInvoker, offsetRecoveredFromSnowflake, newChannel);

    return offsetRecoveredFromSnowflake;
  }

  /**
   * Resets the offset in kafka, resets metadata related to offsets and clears the buffer. If we
   * don't get a valid offset token (because of a table recreation or channel inactivity), we will
   * rely on kafka to send us the correct offset
   *
   * <p>Idea behind resetting offset (1 more than what we found in snowflake) is that Kafka should
   * send offsets from this offset number so as to not miss any data.
   *
   * @param streamingApiFallbackInvoker Streaming API which is using this fallback function. Used
   *     for logging mainly.
   * @param offsetRecoveredFromSnowflake offset number found in snowflake for this
   *     channel(partition)
   * @param newChannel a channel to assign to the current instance
   */
  private void resetChannelMetadataAfterRecovery(
      final StreamingApiFallbackInvoker streamingApiFallbackInvoker,
      final long offsetRecoveredFromSnowflake,
      SnowflakeStreamingIngestChannel newChannel) {
    if (offsetRecoveredFromSnowflake == NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE) {
      LOGGER.info(
          "{} Channel:{}, offset token is NULL, will attempt to use offset managed by the connector"
              + ", consumer offset: {}",
          streamingApiFallbackInvoker,
          this.getChannelNameFormatV1(),
          this.currentConsumerGroupOffset.get());
    }

    final long offsetToResetInKafka =
        offsetRecoveredFromSnowflake == NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE
            ? currentConsumerGroupOffset.get()
            : offsetRecoveredFromSnowflake + 1L;
    if (offsetToResetInKafka == NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE) {
      this.channel = newChannel;
      return;
    }

    // Reset Offset in kafka for this topic partition.
    this.sinkTaskContext.offset(this.topicPartition, offsetToResetInKafka);

    // Need to update the in memory processed offset otherwise if same offset is send again, it
    // might get rejected.
    this.offsetPersistedInSnowflake.set(offsetRecoveredFromSnowflake);
    this.processedOffset.set(offsetRecoveredFromSnowflake);

    // Set the flag so that any leftover rows in the buffer should be skipped, it will be
    // re-ingested since the offset in kafka was reset
    needToSkipCurrentBatch = true;
    this.channel = newChannel;

    LOGGER.warn(
        "{} Channel:{}, setting sinkTaskOffset to {}, offsetPersistedInSnowflake to {},"
            + " processedOffset = {}",
        streamingApiFallbackInvoker,
        this.getChannelNameFormatV1(),
        offsetToResetInKafka,
        offsetRecoveredFromSnowflake,
        offsetRecoveredFromSnowflake);
  }

  /**
   * {@link Fallback} executes below code if retries have failed on {@link SFException}.
   *
   * <p>It re-opens the channel and fetches the latestOffsetToken one more time after reopen was
   * successful.
   *
   * @param streamingApiFallbackInvoker Streaming API which invoked this function.
   * @return offset which was last present in Snowflake
   */
  private SnowflakeStreamingIngestChannel reopenChannel(
      final StreamingApiFallbackInvoker streamingApiFallbackInvoker) {
    LOGGER.warn(
        "{} Re-opening channel:{}", streamingApiFallbackInvoker, this.getChannelNameFormatV1());
    return Preconditions.checkNotNull(openChannelForTable(this.enableSchemaEvolution));
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
    LOGGER.debug(
        "Fetching last committed offset for partition channel:{}", this.getChannelNameFormatV1());
    SnowflakeStreamingIngestChannel channelToGetOffset = this.channel;
    return fetchLatestOffsetFromChannel(channelToGetOffset);
  }

  private long fetchLatestOffsetFromChannel(SnowflakeStreamingIngestChannel channel) {
    String offsetToken = null;
    try {
      offsetToken = channel.getLatestCommittedOffsetToken();
      LOGGER.info(
          "Fetched offsetToken for channelName:{}, offset:{}",
          this.getChannelNameFormatV1(),
          offsetToken);
      return offsetToken == null
          ? NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE
          : Long.parseLong(offsetToken);
    } catch (NumberFormatException ex) {
      LOGGER.error(
          "The offsetToken string does not contain a parsable long:{} for channel:{}",
          offsetToken,
          this.getChannelNameFormatV1());
      throw new ConnectException(ex);
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
  private SnowflakeStreamingIngestChannel openChannelForTable(boolean schemaEvolutionEnabled) {
    // SKIP_BATCH is necessary to avoid race condition in the schematization flow
    OpenChannelRequest.OnErrorOption onErrorOption =
        schemaEvolutionEnabled
            ? OpenChannelRequest.OnErrorOption.SKIP_BATCH
            : OpenChannelRequest.OnErrorOption.CONTINUE;

    OpenChannelRequest channelRequest =
        OpenChannelRequest.builder(this.channelNameFormatV1)
            .setDBName(this.sfConnectorConfig.get(Utils.SF_DATABASE))
            .setSchemaName(this.sfConnectorConfig.get(Utils.SF_SCHEMA))
            .setTableName(this.tableName)
            .setOnErrorOption(onErrorOption)
            .setOffsetTokenVerificationFunction(StreamingUtils.offsetTokenVerificationFunction)
            .build();

    LOGGER.info(
        "Opening a channel with name:{} for table name:{}",
        this.channelNameFormatV1,
        this.tableName);

    try {
      return OpenChannelRetryPolicy.executeWithRetry(
          () -> streamingIngestClient.openChannel(channelRequest), this.channelNameFormatV1);
    } catch (RuntimeException e) {
      LOGGER.error(
          "Failed to open channel {} after retries: {}",
          this.channelNameFormatV1,
          e.getMessage(),
          e);
      // rethrow the original exception when retry limit exceeded
      throw e;
    }
  }

  @Override
  public void closeChannel() {
    try {
      this.channel.close().get();

      // telemetry and metrics
      this.telemetryServiceV2.reportKafkaPartitionUsage(this.snowflakeTelemetryChannelStatus, true);
      this.snowflakeTelemetryChannelStatus.tryUnregisterChannelJMXMetrics();
    } catch (InterruptedException | ExecutionException | SFException e) {
      final String errMsg =
          String.format(
              "Failure closing Streaming Channel name:%s msg:%s",
              this.getChannelNameFormatV1(), e.getMessage());
      this.telemetryServiceV2.reportKafkaConnectFatalError(errMsg);
      LOGGER.error(
          "Closing Streaming Channel={} encountered an exception {}: {} {}",
          this.getChannelNameFormatV1(),
          e.getClass(),
          e.getMessage(),
          Arrays.toString(e.getStackTrace()));
    }
  }

  @Override
  public CompletableFuture<Void> closeChannelAsync() {
    return closeChannelWrapped()
        .thenAccept(__ -> onCloseChannelSuccess())
        .exceptionally(this::tryRecoverFromCloseChannelError);
  }

  private CompletableFuture<Void> closeChannelWrapped() {
    try {
      return this.channel.close();
    } catch (SFException e) {
      // Calling channel.close() can throw an SFException if the channel has been invalidated
      // already. Wrapping the exception into a CompletableFuture to keep a consistent method chain.
      CompletableFuture<Void> future = new CompletableFuture<>();
      future.completeExceptionally(e);
      return future;
    }
  }

  private void onCloseChannelSuccess() {
    this.telemetryServiceV2.reportKafkaPartitionUsage(this.snowflakeTelemetryChannelStatus, true);
    this.snowflakeTelemetryChannelStatus.tryUnregisterChannelJMXMetrics();
  }

  private Void tryRecoverFromCloseChannelError(Throwable e) {
    // CompletableFuture wraps errors into CompletionException.
    Throwable cause = e instanceof CompletionException ? e.getCause() : e;

    String errMsg =
        String.format(
            "Failure closing Streaming Channel name:%s msg:%s",
            this.getChannelNameFormatV1(), cause.getMessage());
    this.telemetryServiceV2.reportKafkaConnectFatalError(errMsg);

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

  /* Return true is channel is closed. Caller should handle the logic for reopening the channel if it is closed. */
  @Override
  public boolean isChannelClosed() {
    return this.channel.isClosed();
  }

  // ------ GETTERS ------ //

  @Override
  public String getChannelNameFormatV1() {
    return this.channel.getFullyQualifiedName();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("offsetPersistedInSnowflake", this.offsetPersistedInSnowflake)
        .add("channelName", this.getChannelNameFormatV1())
        .add("isStreamingIngestClientClosed", this.streamingIngestClient.isClosed())
        .toString();
  }

  @Override
  @VisibleForTesting
  public long getOffsetPersistedInSnowflake() {
    return this.offsetPersistedInSnowflake.get();
  }

  @Override
  @VisibleForTesting
  public long getProcessedOffset() {
    return this.processedOffset.get();
  }

  @Override
  @VisibleForTesting
  public long getLatestConsumerOffset() {
    return this.currentConsumerGroupOffset.get();
  }

  @Override
  @VisibleForTesting
  @Deprecated
  public boolean isPartitionBufferEmpty() {
    return true;
  }

  @Override
  @VisibleForTesting
  public SnowflakeStreamingIngestChannel getChannel() {
    return this.channel;
  }

  @Override
  @VisibleForTesting
  public SnowflakeTelemetryService getTelemetryServiceV2() {
    return this.telemetryServiceV2;
  }

  @Override
  @VisibleForTesting
  public SnowflakeTelemetryChannelStatus getSnowflakeTelemetryChannelStatus() {
    return this.snowflakeTelemetryChannelStatus;
  }

  @Override
  public void setLatestConsumerGroupOffset(long consumerOffset) {
    if (consumerOffset > this.currentConsumerGroupOffset.get()) {
      this.currentConsumerGroupOffset.set(consumerOffset);
    }
  }

  /**
   * Converts the original kafka sink record into a Json Record. i.e key and values are converted
   * into Json so that it can be used to insert into variant column of Snowflake Table.
   *
   * <p>TODO: SNOW-630885 - When schematization is enabled, we should create the map directly from
   * the SinkRecord instead of first turning it into json
   */
  private SinkRecord getSnowflakeSinkRecordFromKafkaRecord(final SinkRecord kafkaSinkRecord) {
    SinkRecord snowflakeRecord = kafkaSinkRecord;
    if (shouldConvertContent(kafkaSinkRecord.value())) {
      snowflakeRecord = handleNativeRecord(kafkaSinkRecord, false);
    }
    if (shouldConvertContent(kafkaSinkRecord.key())) {
      snowflakeRecord = handleNativeRecord(snowflakeRecord, true);
    }

    return snowflakeRecord;
  }

  private Map<String, Object> transformDataBeforeSending(SinkRecord kafkaSinkRecord) {
    SinkRecord snowflakeSinkRecord = getSnowflakeSinkRecordFromKafkaRecord(kafkaSinkRecord);
    // broken record
    if (isRecordBroken(snowflakeSinkRecord)) {
      // check for error tolerance and log tolerance values
      // errors.log.enable and errors.tolerance
      LOGGER.debug(
          "Broken record offset:{}, topic:{}",
          kafkaSinkRecord.kafkaOffset(),
          kafkaSinkRecord.topic());
      kafkaRecordErrorReporter.reportError(kafkaSinkRecord, new DataException("Broken Record"));
    } else {
      // lag telemetry, note that sink record timestamp might be null
      if (kafkaSinkRecord.timestamp() != null
          && kafkaSinkRecord.timestampType() != NO_TIMESTAMP_TYPE) {
        // TODO:SNOW-529751 telemetry
      }

      // Convert this records into Json Schema which has content and metadata, add it to DLQ if
      // there is an exception
      try {
        return recordService.getProcessedRecordForStreamingIngest(snowflakeSinkRecord);
      } catch (JsonProcessingException e) {
        LOGGER.warn(
            "Record has JsonProcessingException offset:{}, topic:{}",
            kafkaSinkRecord.kafkaOffset(),
            kafkaSinkRecord.topic());
        kafkaRecordErrorReporter.reportError(kafkaSinkRecord, e);
      } catch (SnowflakeKafkaConnectorException e) {
        if (e.checkErrorCode(SnowflakeErrors.ERROR_0010)) {
          LOGGER.warn(
              "Cannot parse record offset:{}, topic:{}. Sending to DLQ.",
              kafkaSinkRecord.kafkaOffset(),
              kafkaSinkRecord.topic());
          kafkaRecordErrorReporter.reportError(kafkaSinkRecord, e);
        } else {
          throw e;
        }
      }
    }

    // return empty
    return ImmutableMap.of();
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
     * Fallback invoked when {@link SnowflakeStreamingIngestChannel#insertRows(Iterable, String,
     * String)} has failures.
     */
    INSERT_ROWS_FALLBACK,

    /**
     * Fallback invoked when {@link SnowflakeStreamingIngestChannel#getLatestCommittedOffsetToken()}
     * has failures.
     */
    GET_OFFSET_TOKEN_FALLBACK,

    /** Fallback invoked when schema evolution kicks in during insert rows */
    INSERT_ROWS_SCHEMA_EVOLUTION_FALLBACK,
    ;

    /** @return Used to LOG which API tried to invoke fallback function. */
    @Override
    public String toString() {
      return "[" + this.name() + "]";
    }
  }
}
