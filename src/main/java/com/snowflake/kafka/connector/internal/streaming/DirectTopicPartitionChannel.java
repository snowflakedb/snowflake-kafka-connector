package com.snowflake.kafka.connector.internal.streaming;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.ENABLE_CHANNEL_OFFSET_TOKEN_MIGRATION_CONFIG;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.ENABLE_CHANNEL_OFFSET_TOKEN_MIGRATION_DEFAULT;
import static com.snowflake.kafka.connector.Utils.getDatabase;
import static com.snowflake.kafka.connector.Utils.getSchema;
import static java.util.stream.Collectors.toMap;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.dlq.KafkaRecordErrorReporter;
import com.snowflake.kafka.connector.internal.KCLogger;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException;
import com.snowflake.kafka.connector.internal.metrics.MetricsJmxReporter;
import com.snowflake.kafka.connector.internal.streaming.channel.TopicPartitionChannel;
import com.snowflake.kafka.connector.internal.streaming.common.ColumnProperties;
import com.snowflake.kafka.connector.internal.streaming.schemaevolution.InsertErrorMapper;
import com.snowflake.kafka.connector.internal.streaming.schemaevolution.SchemaEvolutionService;
import com.snowflake.kafka.connector.internal.streaming.schemaevolution.SchemaEvolutionTargetItems;
import com.snowflake.kafka.connector.internal.streaming.telemetry.SnowflakeTelemetryChannelCreation;
import com.snowflake.kafka.connector.internal.streaming.telemetry.SnowflakeTelemetryChannelStatus;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryService;
import com.snowflake.kafka.connector.records.RecordServiceFactory;
import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeExecutor;
import dev.failsafe.Fallback;
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
import org.apache.kafka.connect.errors.ConnectException;
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

  // Used for telemetry and logging only
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

  private final SchemaEvolutionService schemaEvolutionService;

  /**
   * Available from {@link org.apache.kafka.connect.sink.SinkTask} which has access to various
   * utility methods.
   */
  private final SinkTaskContext sinkTaskContext;

  // Whether schema evolution could be done on this channel
  private final boolean enableSchemaEvolution;

  // Reference to the Snowflake connection service
  private final SnowflakeConnectionService conn;

  private final SnowflakeTelemetryChannelStatus snowflakeTelemetryChannelStatus;

  private final InsertErrorMapper insertErrorMapper;

  private final ChannelOffsetTokenMigrator channelOffsetTokenMigrator;

  private final StreamingRecordService streamingRecordService;

  /**
   * Used to send telemetry to Snowflake. Currently, TelemetryClient created from a Snowflake
   * Connection Object, i.e. not a session-less Client
   */
  private final SnowflakeTelemetryService telemetryServiceV2;

  private final FailsafeExecutor<Long> offsetTokenExecutor;

  private final StreamingErrorHandler streamingErrorHandler;

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
        sinkTaskContext,
        conn,
        new StreamingRecordService(
            RecordServiceFactory.createRecordService(
                false, Utils.isSchematizationEnabled(sfConnectorConfig), false),
            kafkaRecordErrorReporter),
        telemetryService,
        false,
        null,
        schemaEvolutionService,
        insertErrorMapper,
        new StreamingErrorHandler(sfConnectorConfig, kafkaRecordErrorReporter, telemetryService));
  }

  /**
   * @param streamingIngestClient client created specifically for this task
   * @param topicPartition topic partition corresponding to this Streaming Channel
   *     (TopicPartitionChannel)
   * @param channelNameFormatV1 channel Name which is deterministic for topic and partition
   * @param tableName table to ingest in snowflake
   * @param enableSchemaEvolution if the schema evolution should be performed on the table
   * @param sfConnectorConfig configuration set for snowflake connector
   * @param sinkTaskContext context on Kafka Connect's runtime
   * @param conn the snowflake connection service
   * @param streamingRecordService record service for processing incoming offsets from Kafka
   * @param telemetryService Telemetry Service which includes the Telemetry Client, sends Json data
   *     to Snowflake
   * @param insertErrorMapper Mapper to map insert errors to schema evolution items
   * @param streamingErrorHandler contains DLQ and error logging related logic
   */
  public DirectTopicPartitionChannel(
      SnowflakeStreamingIngestClient streamingIngestClient,
      TopicPartition topicPartition,
      final String channelNameFormatV1,
      final String tableName,
      final boolean enableSchemaEvolution,
      final Map<String, String> sfConnectorConfig,
      SinkTaskContext sinkTaskContext,
      SnowflakeConnectionService conn,
      StreamingRecordService streamingRecordService,
      SnowflakeTelemetryService telemetryService,
      boolean enableCustomJMXMonitoring,
      MetricsJmxReporter metricsJmxReporter,
      SchemaEvolutionService schemaEvolutionService,
      InsertErrorMapper insertErrorMapper,
      StreamingErrorHandler streamingErrorHandler) {
    final long startTime = System.currentTimeMillis();

    this.streamingIngestClient = Preconditions.checkNotNull(streamingIngestClient);
    Preconditions.checkState(!streamingIngestClient.isClosed());
    this.topicPartition = Preconditions.checkNotNull(topicPartition);
    this.channelNameFormatV1 = Preconditions.checkNotNull(channelNameFormatV1);
    this.tableName = Preconditions.checkNotNull(tableName);
    this.sfConnectorConfig = Preconditions.checkNotNull(sfConnectorConfig);
    this.sinkTaskContext = Preconditions.checkNotNull(sinkTaskContext);
    this.conn = conn;

    this.streamingRecordService = streamingRecordService;
    this.telemetryServiceV2 = Preconditions.checkNotNull(telemetryService);

    this.enableSchemaEvolution = enableSchemaEvolution;
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

    this.offsetTokenExecutor =
        LatestCommitedOffsetTokenExecutor.getExecutor(
            this.getChannelNameFormatV1(),
            SFException.class,
            () ->
                streamingApiFallbackSupplier(
                    StreamingApiFallbackInvoker.GET_OFFSET_TOKEN_FALLBACK));

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
    this.streamingErrorHandler = streamingErrorHandler;
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

  private void transformAndSend(SinkRecord kafkaSinkRecord) {
    try {
      Map<String, Object> transformedRecord = streamingRecordService.transformData(kafkaSinkRecord);
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
          Map<String, ColumnProperties> tableSchema = getTableSchemaFromChannel();
          schemaEvolutionService.evolveSchemaIfNeeded(
              schemaEvolutionTargetItems, kafkaSinkRecord, tableSchema);
          streamingApiFallbackSupplier(
              StreamingApiFallbackInvoker.INSERT_ROWS_SCHEMA_EVOLUTION_FALLBACK);
        } catch (SnowflakeKafkaConnectorException e) {
          LOGGER.error(
              "Error while performing schema evolution for channel:{}",
              this.getChannelNameFormatV1(),
              e);
          if (Objects.equals(e.getCode(), SnowflakeErrors.ERROR_5026.getCode())) {
            streamingErrorHandler.handleError(Collections.singletonList(e), kafkaSinkRecord);
          } else {
            throw e;
          }
        }

        return;
      }
    }

    streamingErrorHandler.handleError(
        insertErrors.stream()
            .map(InsertValidationResponse.InsertError::getException)
            .collect(Collectors.toList()),
        kafkaSinkRecord);
  }

  private Map<String, ColumnProperties> getTableSchemaFromChannel() {
    return channel.getTableSchema().entrySet().stream()
        .collect(toMap(Map.Entry::getKey, entry -> new ColumnProperties(entry.getValue())));
  }

  @Override
  @VisibleForTesting
  public long fetchOffsetTokenWithRetry() {
    return offsetTokenExecutor.get(this::fetchLatestCommittedOffsetFromSnowflake);
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
            .setDBName(getDatabase(sfConnectorConfig))
            .setSchemaName(getSchema(sfConnectorConfig))
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
    if (consumerOffset > this.currentConsumerGroupOffset.get()) {
      this.currentConsumerGroupOffset.set(consumerOffset);
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
