package com.snowflake.kafka.connector.internal.streaming;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.ERRORS_TOLERANCE_CONFIG;
import static com.snowflake.kafka.connector.internal.streaming.StreamingUtils.DURATION_BETWEEN_GET_OFFSET_TOKEN_RETRY;
import static com.snowflake.kafka.connector.internal.streaming.StreamingUtils.MAX_GET_OFFSET_TOKEN_RETRIES;
import static java.time.temporal.ChronoUnit.SECONDS;
import static org.apache.kafka.common.record.TimestampType.NO_TIMESTAMP_TYPE;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.dlq.KafkaRecordErrorReporter;
import com.snowflake.kafka.connector.internal.KCLogger;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.metrics.MetricsJmxReporter;
import com.snowflake.kafka.connector.internal.streaming.telemetry.SnowflakeTelemetryChannelCreation;
import com.snowflake.kafka.connector.internal.streaming.telemetry.SnowflakeTelemetryChannelStatus;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryService;
import com.snowflake.kafka.connector.records.RecordService;
import com.snowflake.kafka.connector.records.SnowflakeJsonSchema;
import com.snowflake.kafka.connector.records.SnowflakeRecordContent;
import dev.failsafe.Failsafe;
import dev.failsafe.Fallback;
import dev.failsafe.RetryPolicy;
import dev.failsafe.function.CheckedSupplier;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import net.snowflake.client.jdbc.internal.fasterxml.jackson.core.JsonProcessingException;
import net.snowflake.ingest.internal.apache.commons.math3.analysis.function.Sin;
import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.utils.Pair;
import net.snowflake.ingest.utils.SFException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;

/**
 * This is a wrapper on top of Streaming Ingest Channel which is responsible for ingesting rows to
 * Snowflake.
 *
 * <p>There is a one to one relation between partition and channel.
 *
 * <p>The number of TopicPartitionChannel objects can scale in proportion to the number of
 * partitions of a topic.
 *
 * <p>Whenever a new instance is created, the cache(Map) in SnowflakeSinkService is also replaced,
 * and we will reload the offsets from SF and reset the consumer offset in kafka
 *
 * <p>During rebalance, we would lose this state and hence there is a need to invoke
 * getLatestOffsetToken from Snowflake
 */
public class TopicPartitionChannel {
  private static final KCLogger LOGGER = new KCLogger(TopicPartitionChannel.class.getName());

  public static final long NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE = -1L;

  // used to communicate to the streaming ingest's insertRows API
  // This is non final because we might decide to get the new instance of Channel
  private SnowflakeStreamingIngestChannel channel;

  // -------- private final fields -------- //

  // This offset represents the data persisted in Snowflake. More specifically it is the Snowflake
  // offset determined from the insertRows API call. It is set after calling the fetchOffsetToken
  // API for this channel
  private final AtomicLong offsetPersistedInSnowflake =
      new AtomicLong(NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE);

  // This offset represents the current record processed in KC. More specifically it is the KC offset to ensure
  // exactly once functionality. On creation it is set to the latest committed token in Snowflake
  // (see offsetPersistedInSnowflake) and updated on each new row from KC.
  private final AtomicLong processedOffset =
      new AtomicLong(NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE);

  private final SnowflakeStreamingIngestClient streamingIngestClient;

  // Topic partition Object from connect consisting of topic and partition
  private final TopicPartition topicPartition;

  /* Channel Name is computed from topic and partition */
  private final String channelName;

  /* table is required for opening the channel */
  private final String tableName;

  /* Error handling, DB, schema, Snowflake URL and other snowflake specific connector properties are defined here. */
  private final Map<String, String> sfConnectorConfig;

  /* Responsible for converting records to Json */
  private final RecordService recordService;

  /* Responsible for returning errors to DLQ if records have failed to be ingested. */
  private final KafkaRecordErrorReporter kafkaRecordErrorReporter;

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

  /**
   * Used to send telemetry to Snowflake. Currently, TelemetryClient created from a Snowflake
   * Connection Object, i.e. not a session-less Client
   */
  private final SnowflakeTelemetryService telemetryServiceV2;

  /** Testing only, initialize TopicPartitionChannel without the connection service */
  @VisibleForTesting
  public TopicPartitionChannel(
      SnowflakeStreamingIngestClient streamingIngestClient,
      TopicPartition topicPartition,
      final String channelName,
      final String tableName,
      final Map<String, String> sfConnectorConfig,
      KafkaRecordErrorReporter kafkaRecordErrorReporter,
      SinkTaskContext sinkTaskContext,
      SnowflakeTelemetryService telemetryService) {
    this(
        streamingIngestClient,
        topicPartition,
        channelName,
        tableName,
        false, /* No schema evolution permission */
        sfConnectorConfig,
        kafkaRecordErrorReporter,
        sinkTaskContext,
        null, /* Null Connection */
        new RecordService(telemetryService),
        telemetryService,
        false,
        null);
  }

  /**
   * @param streamingIngestClient client created specifically for this task
   * @param topicPartition topic partition corresponding to this Streaming Channel
   *     (TopicPartitionChannel)
   * @param channelName channel Name which is deterministic for topic and partition
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
   */
  public TopicPartitionChannel(
      SnowflakeStreamingIngestClient streamingIngestClient,
      TopicPartition topicPartition,
      final String channelName,
      final String tableName,
      boolean hasSchemaEvolutionPermission,
      final Map<String, String> sfConnectorConfig,
      KafkaRecordErrorReporter kafkaRecordErrorReporter,
      SinkTaskContext sinkTaskContext,
      SnowflakeConnectionService conn,
      RecordService recordService,
      SnowflakeTelemetryService telemetryService,
      boolean enableCustomJMXMonitoring,
      MetricsJmxReporter metricsJmxReporter) {
    final long startTime = System.currentTimeMillis();

    this.streamingIngestClient = Preconditions.checkNotNull(streamingIngestClient);
    Preconditions.checkState(!streamingIngestClient.isClosed());
    this.topicPartition = Preconditions.checkNotNull(topicPartition);
    this.channelName = Preconditions.checkNotNull(channelName);
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
    this.enableSchematization =
        this.recordService.setAndGetEnableSchematizationFromConfig(sfConnectorConfig);

    this.enableSchemaEvolution = this.enableSchematization && hasSchemaEvolutionPermission;

    // Open channel and reset the offset in kafka
    this.channel = Preconditions.checkNotNull(openChannelForTable());
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
            channelName,
            startTime,
            enableCustomJMXMonitoring,
            metricsJmxReporter,
            this.offsetPersistedInSnowflake,
            this.processedOffset);
    this.telemetryServiceV2.reportKafkaPartitionStart(
        new SnowflakeTelemetryChannelCreation(this.tableName, this.channelName, startTime));

    if (lastCommittedOffsetToken != NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE) {
      this.sinkTaskContext.offset(this.topicPartition, lastCommittedOffsetToken + 1L);
    } else {
      LOGGER.info(
          "TopicPartitionChannel:{}, offset token is NULL, will rely on Kafka to send us the"
              + " correct offset instead",
          this.getChannelName());
    }
  }

  /**
   * Inserts the record into buffer
   *
   * <p>Step 1: Initializes this channel by fetching the offsetToken from Snowflake for the first
   * time this channel/partition has received offset after start/restart.
   *
   * <p>Step 2: Decides whether given offset from Kafka needs to be processed and whether it
   * qualifies for being added into buffer.
   *
   * @param kafkaSinkRecord input record from Kafka
   */
  public InsertValidationResponse insertRecord(SinkRecord kafkaSinkRecord) {
    final long currentOffsetPersistedInSnowflake = this.offsetPersistedInSnowflake.get();
    InsertValidationResponse finalResponse = new InsertValidationResponse();

    // check SF offset to see if we can insert
    if (shouldInsertRecord(kafkaSinkRecord, currentOffsetPersistedInSnowflake)) {
      try {
        // try insert with fallback
        finalResponse = insertRowsWithFallback(this.transformData(kafkaSinkRecord), kafkaSinkRecord);
        LOGGER.info(
            "Successfully called insertRows for channel:{}, insertResponseHasErrors:{}",
            this.getChannelName(),
            finalResponse.hasErrors());

        // handle errors
        if (finalResponse.hasErrors()) {
          handleInsertRowsFailures(finalResponse.getInsertErrors(), kafkaSinkRecord);
        }
      } catch (TopicPartitionChannelInsertionException ex) {
        // Suppressing the exception because other channels might still continue to ingest
        LOGGER.warn(
            String.format(
                "[INSERT_RECORDS] Failure inserting rows for channel:%s",
                this.getChannelName()),
            ex);
      }
      this.processedOffset.set(kafkaSinkRecord.kafkaOffset());
    }

    return finalResponse;
  }

  private boolean shouldInsertRecord(SinkRecord kafkaSinkRecord, long currentOffsetPersistedInSnowflake) {
    if (currentOffsetPersistedInSnowflake == NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE) {
      LOGGER.debug("Insert record because there is no offsetPersistedInSnowflake for channel:{}", this.getChannelName());
      return true;
    }

    if (kafkaSinkRecord.kafkaOffset() > currentOffsetPersistedInSnowflake) {
      LOGGER.debug(
          "Insert record because kafkaOffset {} > offsetPersistedInSnowflake {} for channel:{}",
          kafkaSinkRecord.kafkaOffset(),
          this.getChannelName());
      return true;
    }

    LOGGER.debug(
        "Ignore adding kafkaOffset:{} in channel:{}. offsetPersistedInSnowflake:{}",
        kafkaSinkRecord.kafkaOffset(),
        this.getChannelName(),
        currentOffsetPersistedInSnowflake);
    return false;
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
      newSFContent = new SnowflakeRecordContent(schema, content);
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
   * @return InsertRowsResponse a response that wraps around InsertValidationResponse
   */
  private InsertValidationResponse insertRowsWithFallback(Pair<Map<String, Object>, Long> recordsAndOffsets, SinkRecord sinkRecord) {
    Fallback<Object> reopenChannelFallbackExecutorForInsertRows =
        Fallback.builder(
                executionAttemptedEvent -> {
                  insertRowsFallbackSupplier(executionAttemptedEvent.getLastException());
                })
            .handle(SFException.class)
            .onFailedAttempt(
                event ->
                    LOGGER.warn(
                        String.format(
                            "Failed Attempt to invoke the insertRows API"),
                        event.getLastException()))
            .onFailure(
                event ->
                    LOGGER.error(
                        String.format(
                            "%s Failed to open Channel or fetching offsetToken for channel:%s",
                            StreamingApiFallbackInvoker.INSERT_ROWS_FALLBACK,
                            this.getChannelName()),
                        event.getException()))
            .build();

    return Failsafe.with(reopenChannelFallbackExecutorForInsertRows)
        .get(
            new InsertRowsApiResponseSupplier(
                this.channel, recordsAndOffsets, sinkRecord, this.enableSchemaEvolution, this.conn));
  }

  /** Invokes the API given the channel and streaming Buffer. */
  private static class InsertRowsApiResponseSupplier
      implements CheckedSupplier<InsertValidationResponse> {

    // Reference to the Snowpipe Streaming channel
    private final SnowflakeStreamingIngestChannel channel;

    private final Pair<Map<String, Object>, Long> recordsAndOffsets;
    private final SinkRecord sinkRecord;

    // Whether the schema evolution is enabled
    private final boolean enableSchemaEvolution;

    // Connection service which will be used to do the ALTER TABLE command for schema evolution
    private final SnowflakeConnectionService conn;

    private InsertRowsApiResponseSupplier(
        SnowflakeStreamingIngestChannel channelForInsertRows,
        Pair<Map<String, Object>, Long> recordsAndOffsets,
        SinkRecord sinkRecord,
        boolean enableSchemaEvolution,
        SnowflakeConnectionService conn) {
      this.channel = channelForInsertRows;
      this.recordsAndOffsets = recordsAndOffsets;
      this.sinkRecord = sinkRecord;
      this.enableSchemaEvolution = enableSchemaEvolution;
      this.conn = conn;
    }

    @Override
    public InsertValidationResponse get() throws Throwable {
      LOGGER.debug(
          "Invoking insertRows API for channel:{}",
          this.channel.getFullyQualifiedName());
      Map<String, Object> record = recordsAndOffsets.getKey();
      Long offset = recordsAndOffsets.getValue();
      return this.channel.insertRow(record, Long.toString(offset));
    }
  }

  /**
   * We will reopen the channel on {@link SFException} and reset offset in kafka. But, we will throw
   * a custom exception to show that the streamingBuffer was not added into Snowflake.
   *
   * @throws TopicPartitionChannelInsertionException exception is thrown after channel reopen has
   *     been successful and offsetToken was fetched from Snowflake
   */
  private void insertRowsFallbackSupplier(Throwable ex)
      throws TopicPartitionChannelInsertionException {
    final long offsetRecoveredFromSnowflake =
        streamingApiFallbackSupplier(StreamingApiFallbackInvoker.INSERT_ROWS_FALLBACK);
    throw new TopicPartitionChannelInsertionException(
        String.format(
            "%s Failed to insert rows for channel:%s. Recovered offset from Snowflake is:%s",
            StreamingApiFallbackInvoker.INSERT_ROWS_FALLBACK,
            this.getChannelName(),
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
  private void handleInsertRowsFailures(
      List<InsertValidationResponse.InsertError> insertErrors,
      SinkRecord sinkRecord) {
    // evolve schema if needed
    if (enableSchemaEvolution) {
      InsertValidationResponse.InsertError insertError = insertErrors.get(0);
      List<String> extraColNames = insertError.getExtraColNames();
      List<String> nonNullableColumns = insertError.getMissingNotNullColNames();
      if (extraColNames != null || nonNullableColumns != null) {
        SchematizationUtils.evolveSchemaIfNeeded(
            this.conn,
            this.channel.getTableName(),
            nonNullableColumns,
            extraColNames,
            sinkRecord);
      }
    }

    // log errors if requested, check error tolerance and DLQ behavior
    if (logErrors) {
      for (InsertValidationResponse.InsertError insertError : insertErrors) {
        LOGGER.error("Insert Row Error message:{}", insertError.getException().getMessage());
      }
    }
    if (errorTolerance) {
      if (!isDLQTopicSet) {
        LOGGER.warn(
            "Although config:{} is set, Dead Letter Queue topic:{} is not set.",
            ERRORS_TOLERANCE_CONFIG,
            ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG);
      } else {
        for (InsertValidationResponse.InsertError insertError : insertErrors) {
          // Map error row number to index in sinkRecords list.
          this.kafkaRecordErrorReporter.reportError(sinkRecord,
              insertError.getException());
        }
      }
    } else {
      final String errMsg =
          String.format(
              "Error inserting Records using Streaming API with msg:%s",
              insertErrors.get(0).getException().getMessage());
      this.telemetryServiceV2.reportKafkaConnectFatalError(errMsg);
      throw new DataException(errMsg, insertErrors.get(0).getException());
    }
  }

  // TODO: SNOW-529755 POLL committed offsets in backgraound thread

  /**
   * Get committed offset from Snowflake. It does an HTTP call internally to find out what was the
   * last offset inserted.
   *
   * <p>If committedOffset fetched from Snowflake is null, we would return -1(default value of
   * committedOffset) back to original call. (-1) would return an empty Map of partition and offset
   * back to kafka.
   *
   * <p>Else, we will convert this offset and return the offset which is safe to commit inside Kafka
   * (+1 of this returned value).
   *
   * <p>Check {@link com.snowflake.kafka.connector.SnowflakeSinkTask#preCommit(Map)}
   *
   * <p>Note:
   *
   * <p>If we cannot fetch offsetToken from snowflake even after retries and reopening the channel,
   * we will throw app
   *
   * @return (offsetToken present in Snowflake + 1), else -1
   */
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

  /**
   * Fetches the offset token from Snowflake.
   *
   * <p>It uses <a href="https://github.com/failsafe-lib/failsafe">Failsafe library </a> which
   * implements retries, fallbacks and circuit breaker.
   *
   * <p>Here is how Failsafe is implemented.
   *
   * <p>Fetches offsetToken from Snowflake (Streaming API)
   *
   * <p>If it returns a valid offset number, that number is returned back to caller.
   *
   * <p>If {@link net.snowflake.ingest.utils.SFException} is thrown, we will retry for max 3 times.
   * (Including the original try)
   *
   * <p>Upon reaching the limit of maxRetries, we will {@link Fallback} to opening a channel and
   * fetching offsetToken again.
   *
   * <p>Please note, upon executing fallback, we might throw an exception too. However, in that case
   * we will not retry.
   *
   * @return long offset token present in snowflake for this channel/partition.
   */
  @VisibleForTesting
  protected long fetchOffsetTokenWithRetry() {
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
                        this.getChannelName(),
                        event.getException().toString()))
            .build();

    // Read it from reverse order. Fetch offsetToken, apply retry policy and then fallback.
    return Failsafe.with(offsetTokenFallbackExecutor)
        .onFailure(
            event ->
                LOGGER.error(
                    "[OFFSET_TOKEN_RETRY_FAILSAFE] Failure to fetch offsetToken even after retry"
                        + " and fallback from snowflake for channel:{}, elapsedTimeSeconds:{}",
                    this.getChannelName(),
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
    final long offsetRecoveredFromSnowflake =
        getRecoveredOffsetFromSnowflake(streamingApiFallbackInvoker);
    resetChannelMetadataAfterRecovery(streamingApiFallbackInvoker, offsetRecoveredFromSnowflake);
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
   */
  private void resetChannelMetadataAfterRecovery(
      final StreamingApiFallbackInvoker streamingApiFallbackInvoker,
      final long offsetRecoveredFromSnowflake) {
    if (offsetRecoveredFromSnowflake == NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE) {
      LOGGER.info(
          "{} Channel:{}, offset token is NULL, will use the consumer offset managed by the"
              + " connector instead",
          streamingApiFallbackInvoker,
          this.getChannelName());
    }

    // If the offset token in the channel is null, use the consumer offset managed by the connector;
    // otherwise use the offset token stored in the channel
    final long offsetToResetInKafka = offsetRecoveredFromSnowflake + 1L;
    if (offsetToResetInKafka == NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE) {
      return;
    }

    // Reset Offset in kafka for this topic partition.
    this.sinkTaskContext.offset(this.topicPartition, offsetToResetInKafka);

    // Need to update the in memory processed offset otherwise if same offset is send again, it
    // might get rejected.
    this.offsetPersistedInSnowflake.set(offsetRecoveredFromSnowflake);
    this.processedOffset.set(offsetRecoveredFromSnowflake);

    // State that there was some exception and only clear that state when we have received offset
    // starting from offsetRecoveredFromSnowflake
    LOGGER.warn(
        "{} Channel:{}, OffsetRecoveredFromSnowflake:{}, reset kafka offset to:{}",
        streamingApiFallbackInvoker,
        this.getChannelName(),
        offsetRecoveredFromSnowflake,
        offsetToResetInKafka);
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
  private long getRecoveredOffsetFromSnowflake(
      final StreamingApiFallbackInvoker streamingApiFallbackInvoker) {
    LOGGER.warn("{} Re-opening channel:{}", streamingApiFallbackInvoker, this.getChannelName());
    this.channel = Preconditions.checkNotNull(openChannelForTable());
    LOGGER.warn(
        "{} Fetching offsetToken after re-opening the channel:{}",
        streamingApiFallbackInvoker,
        this.getChannelName());
    return fetchLatestCommittedOffsetFromSnowflake();
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
    LOGGER.debug("Fetching last committed offset for partition channel:{}", this.getChannelName());
    String offsetToken = null;
    try {
      offsetToken = this.channel.getLatestCommittedOffsetToken();
      LOGGER.info(
          "Fetched offsetToken for channelName:{}, offset:{}", this.getChannelName(), offsetToken);
      return offsetToken == null
          ? NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE
          : Long.parseLong(offsetToken);
    } catch (NumberFormatException ex) {
      LOGGER.error(
          "The offsetToken string does not contain a parsable long:{} for channel:{}",
          offsetToken,
          this.getChannelName());
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
  private SnowflakeStreamingIngestChannel openChannelForTable() {
    OpenChannelRequest channelRequest =
        OpenChannelRequest.builder(this.channelName)
            .setDBName(this.sfConnectorConfig.get(Utils.SF_DATABASE))
            .setSchemaName(this.sfConnectorConfig.get(Utils.SF_SCHEMA))
            .setTableName(this.tableName)
            .setOnErrorOption(OpenChannelRequest.OnErrorOption.CONTINUE)
            .build();
    LOGGER.info(
        "Opening a channel with name:{} for table name:{}", this.channelName, this.tableName);
    return streamingIngestClient.openChannel(channelRequest);
  }

  /**
   * Close channel associated to this partition Not rethrowing connect exception because the
   * connector will stop. Channel will eventually be reopened.
   */
  public void closeChannel() {
    try {
      this.channel.close().get();

      // telemetry and metrics
      this.telemetryServiceV2.reportKafkaPartitionUsage(this.snowflakeTelemetryChannelStatus, true);
      this.snowflakeTelemetryChannelStatus.tryUnregisterChannelJMXMetrics();
    } catch (InterruptedException | ExecutionException e) {
      final String errMsg =
          String.format(
              "Failure closing Streaming Channel name:%s msg:%s",
              this.getChannelName(), e.getMessage());
      this.telemetryServiceV2.reportKafkaConnectFatalError(errMsg);
      LOGGER.error(errMsg, e);
    }
  }

  /* Return true is channel is closed. Caller should handle the logic for reopening the channel if it is closed. */
  public boolean isChannelClosed() {
    return this.channel.isClosed();
  }

  public String getChannelName() {
    return this.channel.getFullyQualifiedName();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("offsetPersistedInSnowflake", this.offsetPersistedInSnowflake)
        .add("channelName", this.getChannelName())
        .add("isStreamingIngestClientClosed", this.streamingIngestClient.isClosed())
        .toString();
  }

  @VisibleForTesting
  protected long getOffsetPersistedInSnowflake() {
    return this.offsetPersistedInSnowflake.get();
  }

  @VisibleForTesting
  protected long getProcessedOffset() {
    return this.processedOffset.get();
  }

  @VisibleForTesting
  protected SnowflakeStreamingIngestChannel getChannel() {
    return this.channel;
  }

  @VisibleForTesting
  protected SnowflakeTelemetryService getTelemetryServiceV2() {
    return this.telemetryServiceV2;
  }

  @VisibleForTesting
  protected SnowflakeTelemetryChannelStatus getSnowflakeTelemetryChannelStatus() {
    return this.snowflakeTelemetryChannelStatus;
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

  public Pair<Map<String, Object>, Long> transformData(SinkRecord kafkaSinkRecord) {
    SinkRecord snowflakeRecord = getSnowflakeSinkRecordFromKafkaRecord(kafkaSinkRecord);

    // broken record
    if (isRecordBroken(snowflakeRecord)) {
      // check for error tolerance and log tolerance values
      // errors.log.enable and errors.tolerance
      LOGGER.debug(
          "Broken record offset:{}, topic:{}",
          kafkaSinkRecord.kafkaOffset(),
          kafkaSinkRecord.topic());
      kafkaRecordErrorReporter.reportError(kafkaSinkRecord, new DataException("Broken Record"));
    } else {
      // lag telemetry, note that sink record timestamp might be null
      if (snowflakeRecord.timestamp() != null
          && snowflakeRecord.timestampType() != NO_TIMESTAMP_TYPE) {
        // TODO:SNOW-529751 telemetry
      }

      // Convert this records into Json Schema which has content and metadata, add it to DLQ if
      // there is an exception
      try {
        Map<String, Object> tableRow =
            recordService.getProcessedRecordForStreamingIngest(snowflakeRecord);

        return new Pair<>(tableRow, snowflakeRecord.kafkaOffset());
      } catch (JsonProcessingException e) {
        LOGGER.warn(
            "Record has JsonProcessingException offset:{}, topic:{}",
            kafkaSinkRecord.kafkaOffset(),
            kafkaSinkRecord.topic());
        kafkaRecordErrorReporter.reportError(kafkaSinkRecord, e);
      }
    }

    // return empty
    return new Pair<>(new HashMap<>(), -1L);
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
     * Fallback invoked when {@link SnowflakeStreamingIngestChannel#insertRows(Iterable, String)}
     * has failures.
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
