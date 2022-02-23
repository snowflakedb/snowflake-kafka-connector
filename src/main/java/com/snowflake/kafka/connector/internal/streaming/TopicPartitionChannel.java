package com.snowflake.kafka.connector.internal.streaming;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.*;
import static com.snowflake.kafka.connector.internal.streaming.StreamingUtils.DURATION_BETWEEN_GET_OFFSET_TOKEN_RETRY;
import static com.snowflake.kafka.connector.internal.streaming.StreamingUtils.MAX_GET_OFFSET_TOKEN_RETRIES;
import static java.time.temporal.ChronoUnit.SECONDS;
import static org.apache.kafka.common.record.TimestampType.NO_TIMESTAMP_TYPE;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.dlq.KafkaRecordErrorReporter;
import com.snowflake.kafka.connector.internal.Logging;
import com.snowflake.kafka.connector.internal.PartitionBuffer;
import com.snowflake.kafka.connector.records.RecordService;
import com.snowflake.kafka.connector.records.SnowflakeJsonSchema;
import com.snowflake.kafka.connector.records.SnowflakeRecordContent;
import dev.failsafe.Failsafe;
import dev.failsafe.Fallback;
import dev.failsafe.RetryPolicy;
import dev.failsafe.function.CheckedSupplier;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.utils.SFException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a wrapper on top of Streaming Ingest Channel which is responsible for ingesting rows to
 * Snowflake.
 *
 * <p>There is a one to one relation between partition and channel.
 *
 * <p>The number of TopicPartitionChannel objects can scale in proportion to the number of
 * partitions of a topic.
 *
 * <p>Whenever a new instance is created, the cache(Map) in SnowflakeSinkService is also replaced.
 *
 * <p>During rebalance, we would lose this state and hence there is a need to invoke
 * getLatestOffsetToken from Snowflake
 */
public class TopicPartitionChannel {
  private static final Logger LOGGER = LoggerFactory.getLogger(TopicPartitionChannel.class);

  private static final long NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE = -1L;

  // last time we invoked insertRows API
  private long previousFlushTimeStampMs;

  /* Buffer to hold JSON converted incoming SinkRecords */
  private StreamingBuffer streamingBuffer;

  final Lock bufferLock = new ReentrantLock(true);

  /**
   * States whether this channel has received any records before.
   *
   * <p>If this is false, the topicPartitionChannel has recently been initialised and didnt receive
   * any records before or TopicPartitionChannel was recently created.
   *
   * <p>If the channel is closed, or if partition reassignment is triggered (Rebalancing), we wipe
   * off partitionsToChannel cache in {@link SnowflakeSinkServiceV2}
   */
  private boolean hasChannelReceivedAnyRecordsBefore = false;

  // This offset is updated when Snowflake has received offset from insertRows API
  // We will update this value after calling offsetToken API for this channel
  // We will only update it during start of the channel initialization
  private long offsetPersistedInSnowflake = NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE;

  // used to communicate to the streaming ingest's insertRows API
  // This is non final because we might decide to get the new instance of Channel
  private SnowflakeStreamingIngestChannel channel;

  // -------- private final fields -------- //

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

  // ------- Kafka record related properties ------- //
  // Offset number we would want to commit back to kafka
  // This value is + 1 of what we find in snowflake.
  // It tells kafka to start sending offsets from this offset because earlier ones have been
  // ingested. (Hence it +1)
  private final AtomicLong offsetSafeToCommitToKafka;

  // added to buffer before calling insertRows
  private final AtomicLong processedOffset; // processed offset

  /* Error related properties */

  // If set to true, we will send records to DLQ provided DLQ name is valid.
  private final boolean errorTolerance;

  // Whether to log errors to log file.
  private final boolean logErrors;

  // Set to false if DLQ topic is null or empty. True if it is a valid string in config
  private final boolean isDLQTopicSet;

  // Used to identify when to flush (Time, bytes or number of records)
  private final StreamingBufferThreshold streamingBufferThreshold;

  /**
   * @param streamingIngestClient client created specifically for this task
   * @param topicPartition topic partition corresponding to this Streaming Channel
   *     (TopicPartitionChannel)
   * @param channelName channel Name which is deterministic for topic and partition
   * @param tableName table to ingest in snowflake
   * @param streamingBufferThreshold bytes, count of records and flush time thresholds.
   * @param sfConnectorConfig configuration set for snowflake connector
   * @param kafkaRecordErrorReporter kafka errpr reporter for sending records to DLQ
   * @param sinkTaskContext context on Kafka Connect's runtime
   */
  public TopicPartitionChannel(
      SnowflakeStreamingIngestClient streamingIngestClient,
      TopicPartition topicPartition,
      final String channelName,
      final String tableName,
      final StreamingBufferThreshold streamingBufferThreshold,
      final Map<String, String> sfConnectorConfig,
      KafkaRecordErrorReporter kafkaRecordErrorReporter,
      SinkTaskContext sinkTaskContext) {
    this.streamingIngestClient = Preconditions.checkNotNull(streamingIngestClient);
    Preconditions.checkState(!streamingIngestClient.isClosed());
    this.topicPartition = Preconditions.checkNotNull(topicPartition);
    this.channelName = Preconditions.checkNotNull(channelName);
    this.tableName = Preconditions.checkNotNull(tableName);
    this.streamingBufferThreshold = Preconditions.checkNotNull(streamingBufferThreshold);
    this.sfConnectorConfig = Preconditions.checkNotNull(sfConnectorConfig);
    this.channel = Preconditions.checkNotNull(openChannelForTable());
    this.kafkaRecordErrorReporter = Preconditions.checkNotNull(kafkaRecordErrorReporter);
    this.sinkTaskContext = Preconditions.checkNotNull(sinkTaskContext);

    this.recordService = new RecordService();
    this.previousFlushTimeStampMs = System.currentTimeMillis();

    this.streamingBuffer = new StreamingBuffer();
    this.processedOffset = new AtomicLong(-1);
    this.offsetSafeToCommitToKafka = new AtomicLong(0);

    /* Error properties */
    this.errorTolerance = StreamingUtils.tolerateErrors(this.sfConnectorConfig);
    this.logErrors = StreamingUtils.logErrors(this.sfConnectorConfig);
    this.isDLQTopicSet =
        !Strings.isNullOrEmpty(StreamingUtils.getDlqTopicName(this.sfConnectorConfig));
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
   * @param record input record from Kafka
   */
  public void insertRecordToBuffer(SinkRecord record) {
    precomputeOffsetTokenForChannel(record);

    // discard the record if the record offset is smaller or equal to server side offset, or if
    // record is smaller than any other record offset we received before
    if (record.kafkaOffset() > this.offsetPersistedInSnowflake
        && record.kafkaOffset() > processedOffset.get()) {
      SinkRecord snowflakeRecord = record;
      if (shouldConvertContent(snowflakeRecord.value())) {
        snowflakeRecord = handleNativeRecord(snowflakeRecord, false);
      }
      if (shouldConvertContent(snowflakeRecord.key())) {
        snowflakeRecord = handleNativeRecord(snowflakeRecord, true);
      }

      // broken record
      if (isRecordBroken(snowflakeRecord)) {
        // check for error tolerance and log tolerance values
        // errors.log.enable and errors.tolerance
        LOGGER.debug("Broken record offset:{}, topic:{}", record.kafkaOffset(), record.topic());
        this.kafkaRecordErrorReporter.reportError(record, new DataException("Broken Record"));
      } else {
        // lag telemetry, note that sink record timestamp might be null
        if (snowflakeRecord.timestamp() != null
            && snowflakeRecord.timestampType() != NO_TIMESTAMP_TYPE) {
          // TODO:SNOW-529751 telemetry
        }
        StreamingBuffer copiedStreamingBuffer = null;
        // acquire the lock before writing the record into buffer or copying it over to new buffer.
        bufferLock.lock();
        try {
          streamingBuffer.insert(snowflakeRecord);
          processedOffset.set(snowflakeRecord.kafkaOffset());
          // # of records or size based flushing
          if (this.streamingBufferThreshold.isFlushBufferedBytesBased(
                  streamingBuffer.getBufferSizeBytes())
              || this.streamingBufferThreshold.isFlushBufferedRecordCountBased(
                  streamingBuffer.getNumOfRecords())) {
            copiedStreamingBuffer = streamingBuffer;
            this.streamingBuffer = new StreamingBuffer();
            LOGGER.debug(
                "Flush based on buffered bytes or buffered number of records for channel:{},"
                    + "currentBufferSizeInBytes:{}, currentBufferedRecordCount:{}",
                this.getChannelName(),
                copiedStreamingBuffer.getBufferSizeBytes(),
                copiedStreamingBuffer.getSinkRecords().size());
          }
        } finally {
          bufferLock.unlock();
        }

        // If we found reaching buffer size threshold or count based threshold, we will immediately
        // flush (Insert them)
        if (copiedStreamingBuffer != null) {
          insertBufferedRows(copiedStreamingBuffer);
        }
      }
    }
  }

  /**
   * Pre-computes the offset token which might have been persisted into Snowflake for this channel.
   *
   * <p>It is possible the offset token is null. In this case, -1 is set
   *
   * <p>Note: This code is execute only for the first time when:
   *
   * <ul>
   *   <li>Connector starts
   *   <li>Connector restarts because task was killed
   *   <li>Rebalance/Partition reassignment
   * </ul>
   *
   * @param record record this partition received.
   */
  private void precomputeOffsetTokenForChannel(final SinkRecord record) {
    if (!hasChannelReceivedAnyRecordsBefore) {
      LOGGER.info(
          "Received offset:{} for topic:{} as the first offset for this partition:{} after"
              + " start/restart/rebalance",
          record.kafkaOffset(),
          record.topic(),
          record.kafkaPartition());
      // This will only be called once at the beginning when an offset arrives for first time
      // after connector starts
      final long lastCommittedOffsetToken = fetchOffsetTokenWithRetry();
      this.offsetPersistedInSnowflake =
          (lastCommittedOffsetToken == -1L)
              ? NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE
              : lastCommittedOffsetToken;
      this.hasChannelReceivedAnyRecordsBefore = true;
    }
  }

  private boolean shouldConvertContent(final Object content) {
    return content != null && !(content instanceof SnowflakeRecordContent);
  }

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

  // --------------- BUFFER FLUSHING LOGIC --------------- //
  /**
   * If difference between current time and previous flush time is more than threshold, insert the
   * buffered Rows.
   *
   * <p>Note: We acquire buffer lock since we copy the buffer.
   *
   * <p>Threshold is config parameter: {@link
   * com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig#BUFFER_FLUSH_TIME_SEC}
   *
   * <p>Previous flush time here means last time we called insertRows API with rows present in
   */
  protected void insertBufferedRowsIfFlushTimeThresholdReached() {
    if (this.streamingBufferThreshold.isFlushTimeBased(this.previousFlushTimeStampMs)) {
      LOGGER.debug(
          "Time based flush for channel:{}, CurrentTimeMs:{}, previousFlushTimeMs:{},"
              + " bufferThresholdMs:{}",
          this.getChannelName(),
          System.currentTimeMillis(),
          this.previousFlushTimeStampMs,
          this.streamingBufferThreshold.getFlushTimeThresholdSeconds());
      StreamingBuffer copiedStreamingBuffer;
      bufferLock.lock();
      try {
        copiedStreamingBuffer = this.streamingBuffer;
        this.streamingBuffer = new StreamingBuffer();
      } finally {
        bufferLock.unlock();
      }
      insertBufferedRows(copiedStreamingBuffer);
    }
  }

  /**
   * Invokes insertRows API using the provided offsets which were initially buffered for this
   * partition. This buffer is decided based on the flush time threshold, buffered bytes or number
   * of records
   */
  InsertValidationResponse insertBufferedRows(StreamingBuffer streamingBufferToInsert) {

    // intermediate buffer can be empty here if time interval reached but kafka produced no records.
    if (streamingBufferToInsert.isEmpty()) {
      LOGGER.debug("No Rows Buffered, returning;");
      this.previousFlushTimeStampMs = System.currentTimeMillis();
      return null;
    }

    InsertValidationResponse response = insertRowsWithFallback(streamingBufferToInsert);
    // Updates the flush time (last time we called insertRows API)
    this.previousFlushTimeStampMs = System.currentTimeMillis();

    LOGGER.info(
        "[INSERT_ROWS] Successfully called insertRows for channel:{}, noOfRecords:{},"
            + " startOffset:{}, endOffset:{}, hasErrors:{}",
        this.getChannelName(),
        streamingBufferToInsert.getNumOfRecords(),
        streamingBufferToInsert.getFirstOffset(),
        streamingBufferToInsert.getLastOffset(),
        response.hasErrors());
    if (response.hasErrors()) {
      handleInsertRowsFailures(
          response.getInsertErrors(), streamingBufferToInsert.getSinkRecords());
    }
    return response;
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
   * @param buffer buffer to insert into snowflake
   * @return InsertValidationResponse object sent from insertRows API.
   */
  private InsertValidationResponse insertRowsWithFallback(StreamingBuffer buffer) {
    Fallback<Object> reopenChannelFallbackExecutorForInsertRows =
        Fallback.builder(() -> insertRowsFallbackSupplier(buffer))
            .handle(SFException.class)
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
        .get(new InsertRowsApiResponseSupplier(this.channel, buffer));
  }

  /** Invokes the API given the channel and streaming Buffer. */
  private static class InsertRowsApiResponseSupplier
      implements CheckedSupplier<InsertValidationResponse> {

    private final SnowflakeStreamingIngestChannel channel;

    private final StreamingBuffer streamingBuffer;

    private InsertRowsApiResponseSupplier(
        SnowflakeStreamingIngestChannel channel, StreamingBuffer streamingBuffer) {
      this.channel = channel;
      this.streamingBuffer = streamingBuffer;
    }

    @Override
    public InsertValidationResponse get() throws Throwable {
      LOGGER.debug(
          "Invoking insertRows API for channel:{}, noOfRecords:{}, startOffset:{}, endOffset:{}",
          this.channel.getFullyQualifiedName(),
          streamingBuffer.getNumOfRecords(),
          streamingBuffer.getFirstOffset(),
          streamingBuffer.getLastOffset());
      return this.channel.insertRows(
          streamingBuffer.getData(), Long.toString(streamingBuffer.getLastOffset()));
    }
  }

  /** We will reopen the channel on {@link SFException} and reset offset in kafka. */
  private void insertRowsFallbackSupplier(StreamingBuffer streamingBuffer) {
    final long offsetRecoveredFromSnowflake =
        streamingApiFallbackSupplier(StreamingApiFallbackInvoker.INSERT_ROWS_FALLBACK);
    LOGGER.warn(
        "{} Fetched offsetToken:{} for channel:{}",
        StreamingApiFallbackInvoker.INSERT_ROWS_FALLBACK,
        offsetRecoveredFromSnowflake,
        this.getChannelName());
    throw new RetriableException(
        String.format(
            "%s Failed to insert rows for channel:%s, startOffset:%s, endOffset:%s",
            StreamingApiFallbackInvoker.INSERT_ROWS_FALLBACK,
            this.getChannelName(),
            streamingBuffer.getFirstOffset(),
            streamingBuffer.getLastOffset()));
  }

  /**
   * Invoked only when {@link InsertValidationResponse} has errors.
   *
   * <p>This function checks if we need to log errors, send it to DLQ or just ignore and throw
   * exception.
   *
   * @param insertErrors errors from validation response. (Only if it has errors)
   * @param insertedRecordsToBuffer to map {@link SinkRecord} with insertErrors
   */
  private void handleInsertRowsFailures(
      List<InsertValidationResponse.InsertError> insertErrors,
      List<SinkRecord> insertedRecordsToBuffer) {
    if (logErrors) {
      for (InsertValidationResponse.InsertError insertError : insertErrors) {
        LOGGER.error("Insert Row Error message", insertError.getException());
      }
    }
    if (errorTolerance) {
      if (!isDLQTopicSet) {
        LOGGER.warn(
            "Although config:{} is set, Dead Letter Queue topic:{} is not set.",
            ERRORS_TOLERANCE_CONFIG,
            ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG);
      } else {
        for (int i = 0; i < insertErrors.size(); i++) {
          // Write now I am assuming each error maps to its original row which is incorrect.
          // TODO: SNOW-545729 would handle mapping errors with original SinkRecord.
          this.kafkaRecordErrorReporter.reportError(
              insertedRecordsToBuffer.get(i), insertErrors.get(i).getException());
        }
      }
    } else {
      throw new DataException(
          "Error inserting Records using Streaming API", insertErrors.get(0).getException());
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
      return offsetSafeToCommitToKafka.get();
    } else {
      // Return an offset which is + 1 of what was present in snowflake.
      // Idea of sending + 1 back to Kafka is that it should start sending offsets after task
      // restart from this offset
      return offsetSafeToCommitToKafka.updateAndGet(operand -> committedOffsetInSnowflake + 1);
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

    /**
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
                            + " channel:{}",
                        this.getChannelName(),
                        event.getException()))
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
   * <p>Idea behind resetting offset (1 more than what we found in snowflake) is that Kafka should
   * send offsets from this offset number so as to not miss any data.
   *
   * @param streamingApiFallbackInvoker Streaming API which is using this fallback function. Used
   *     for logging mainly.
   * @return offset which was last present in Snowflake
   */
  private long streamingApiFallbackSupplier(
      final StreamingApiFallbackInvoker streamingApiFallbackInvoker) {
    final long offsetRecoveredFromSnowflake =
        getRecoveredOffsetFromSnowflake(streamingApiFallbackInvoker);
    final long offsetToResetInKafka = offsetRecoveredFromSnowflake + 1L;
    this.sinkTaskContext.offset(topicPartition, offsetToResetInKafka);
    // Need to update the in memory processed offset otherwise if same offset is send again, it
    // might get rejected.
    this.processedOffset.set(offsetRecoveredFromSnowflake);
    this.offsetPersistedInSnowflake = offsetRecoveredFromSnowflake;
    LOGGER.info(
        "{} Channel:{}, OffsetRecoveredFromSnowflake:{}, Reset kafka offset to:{}",
        streamingApiFallbackInvoker,
        this.getChannelName(),
        offsetRecoveredFromSnowflake,
        offsetToResetInKafka);
    return offsetRecoveredFromSnowflake;
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
    LOGGER.debug(
        "Fetching last committed offset for partition channel:{}",
        this.channel.getFullyQualifiedName());
    String offsetToken = null;
    try {
      offsetToken = this.channel.getLatestCommittedOffsetToken();
      if (offsetToken == null) {
        LOGGER.info("OffsetToken not present for channelName:{}", this.getChannelName());
        return NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE;
      } else {
        long latestCommittedOffsetInSnowflake = Long.parseLong(offsetToken);
        LOGGER.info(
            "Fetched offsetToken:{} for channelName:{}",
            latestCommittedOffsetInSnowflake,
            this.channel.getFullyQualifiedName());
        return latestCommittedOffsetInSnowflake;
      }
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
    } catch (InterruptedException | ExecutionException e) {
      LOGGER.error(
          Logging.logMessage(
              "Failure closing Streaming Channel name:{} msg:{}",
              this.getChannelName(),
              e.getMessage()),
          e);
    }
  }

  /* Return true is channel is closed. Caller should handle the logic for reopening the channel if it is closed. */
  public boolean isChannelClosed() {
    return this.channel.isClosed();
  }

  // ------ GETTERS ------ //

  public StreamingBuffer getStreamingBuffer() {
    return streamingBuffer;
  }

  public long getPreviousFlushTimeStampMs() {
    return previousFlushTimeStampMs;
  }

  public String getChannelName() {
    return this.channel.getFullyQualifiedName();
  }

  @Override
  public String toString() {
    return "TopicPartitionChannel{"
        + "previousFlushTimeStampMs="
        + previousFlushTimeStampMs
        + ", hasChannelReceivedAnyRecordsBefore="
        + hasChannelReceivedAnyRecordsBefore
        + ", channel="
        + this.getChannelName()
        + ", offsetSafeToCommitToKafka="
        + offsetSafeToCommitToKafka
        + ", processedOffset="
        + processedOffset
        + '}';
  }

  /* For testing */
  protected long getOffsetPersistedInSnowflake() {
    return this.offsetPersistedInSnowflake;
  }

  /* For testing */
  protected boolean isPartitionBufferEmpty() {
    return streamingBuffer.isEmpty();
  }

  /* For testing */
  protected SnowflakeStreamingIngestChannel getChannel() {
    return this.channel;
  }

  // ------ INNER CLASS ------ //
  /**
   * A buffer which holds the rows before calling insertRows API. It implements the PartitionBuffer
   * class which has all common fields about a buffer.
   *
   * <p>This Buffer has same buffer threshold as of Snowpipe Buffer.
   *
   * <p>We would remove this long lived buffer logic in separate commit. SNOW-473896
   */
  @VisibleForTesting
  protected class StreamingBuffer extends PartitionBuffer<List<Map<String, Object>>> {
    // Used to buffer rows per channel
    // Map has key of column name and Object is its value
    // For KC, it will be metadata and content columns.
    // Every List element corresponds to one record(offset) of a partition
    private final List<Map<String, Object>> tableRows;

    private final List<SinkRecord> sinkRecords;

    StreamingBuffer() {
      super();
      tableRows = new ArrayList<>();
      sinkRecords = new ArrayList<>();
    }

    @Override
    public void insert(SinkRecord record) {

      // has two keys(two columns), each of the values are JsonNode
      Map<String, Object> tableRow = recordService.getProcessedRecordForStreamingIngest(record);

      if (tableRows.size() == 0) {
        setFirstOffset(record.kafkaOffset());
      }

      tableRows.add(tableRow);
      sinkRecords.add(record);

      // probably do below things in a separate method.
      // call it collect buffer metrics
      setNumOfRecords(getNumOfRecords() + 1);
      setLastOffset(record.kafkaOffset());
      // need to loop through the map and get the object node
      tableRow.forEach(
          (key, value) -> {
            setBufferSizeBytes(
                getBufferSizeBytes() + ((String) value).length() * 2L); // 1 char = 2 bytes
          });
    }

    /* Get all rows which are present in this list. Each map corresponds to one row whose keys are column names. */
    @Override
    public List<Map<String, Object>> getData() {
      LOGGER.debug(
          "Get rows for streaming ingest. {} records, {} bytes, offset {} - {}",
          getNumOfRecords(),
          getBufferSizeBytes(),
          getFirstOffset(),
          getLastOffset());
      return tableRows;
    }

    @Override
    public List<SinkRecord> getSinkRecords() {
      return sinkRecords;
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
     * Fallback invoked when {@link SnowflakeStreamingIngestChannel#insertRows(Iterable, String)}
     * has failures.
     */
    INSERT_ROWS_FALLBACK,

    /**
     * Fallback invoked when {@link SnowflakeStreamingIngestChannel#getLatestCommittedOffsetToken()}
     * has failures.
     */
    GET_OFFSET_TOKEN_FALLBACK,
    ;

    /** @return Used to LOG which API tried to invoke fallback function. */
    @Override
    public String toString() {
      return "[" + this.name() + "]";
    }
  }
}
