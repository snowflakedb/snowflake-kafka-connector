package com.snowflake.kafka.connector.internal.streaming.v2;


import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.snowflake.ingest.streaming.AppendResult;
import com.snowflake.ingest.streaming.OpenChannelResult;
import com.snowflake.ingest.streaming.SFException;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.KCLogger;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.streaming.LatestCommitedOffsetTokenExecutor;
import com.snowflake.kafka.connector.internal.streaming.StreamingClientProperties;
import com.snowflake.kafka.connector.internal.streaming.StreamingRecordService;
import com.snowflake.kafka.connector.internal.streaming.TopicPartitionChannelInsertionException;
import com.snowflake.kafka.connector.internal.streaming.channel.TopicPartitionChannel;
import com.snowflake.kafka.connector.internal.streaming.telemetry.SnowflakeTelemetryChannelStatus;
import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeExecutor;
import dev.failsafe.Fallback;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import net.snowflake.ingest.streaming.InsertValidationResponse;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;

public class SnowpipeStreamingV2PartitionChannel implements TopicPartitionChannel {
  private static final KCLogger LOGGER =
      new KCLogger(SnowpipeStreamingV2PartitionChannel.class.getName());

  private static final String STREAMING_CLIENT_V2_PREFIX_NAME = "KC_CLIENT_V2_";
  private static final String DEFAULT_CLIENT_NAME = "DEFAULT_CLIENT";

  private static final AtomicInteger createdClientId = new AtomicInteger(0);

  // used to communicate to the streaming ingest's insertRows API
  // This is non final because we might decide to get the new instance of Channel
  private SnowflakeStreamingIngestChannel channel;

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

  private final String channelName;

  /**
   * Available from {@link org.apache.kafka.connect.sink.SinkTask} which has access to various
   * utility methods.
   */
  private final SinkTaskContext sinkTaskContext;

  private final StreamingRecordService streamingRecordService;

  private final FailsafeExecutor<Long> offsetTokenExecutor;

  public SnowpipeStreamingV2PartitionChannel(
      String tableName,
      String channelName,
      TopicPartition topicPartition,
      SnowflakeConnectionService connectionService,
      Map<String, String> connectorConfig,
      StreamingRecordService streamingRecordService,
      SinkTaskContext sinkTaskContext) {
    this.topicPartition = topicPartition;
    this.streamingRecordService = streamingRecordService;
    this.channelName = channelName;
    this.sinkTaskContext = sinkTaskContext;

    String clientName = clientName(connectorConfig);
    String dbName = Utils.database(connectorConfig);
    String schemaName = Utils.schema(connectorConfig);

    String pipeName = PipeNameProvider.pipeName(connectorConfig.get(Utils.NAME), tableName);

    connectionService.createPipeForSSv2(tableName, pipeName);

    StreamingClientProperties streamingClientProperties =
        new StreamingClientProperties(connectorConfig);

    // TODO - single client optimization?
    this.streamingIngestClient =
        SnowflakeStreamingIngestClientFactory.builder(clientName, dbName, schemaName, pipeName)
            .setProperties(StreamingClientPropertiesMapper.getClientProperties(connectorConfig))
            .setParameterOverrides(streamingClientProperties.parameterOverrides)
            .build();
    this.channel = openChannelForTable(channelName);

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
        // TODO - currently returns empty object
        insertRowWithFallback(transformedRecord, kafkaSinkRecord.kafkaOffset());
        this.processedOffset.set(kafkaSinkRecord.kafkaOffset());
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
   * Uses {@link Fallback} API to reopen the channel if insertRows throws {@link
   * net.snowflake.ingest.utils.SFException}.
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
  private AppendResult insertRowWithFallback(Map<String, Object> transformedRecord, long offset) {
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
                            StreamingApiFallbackInvoker.APPEND_ROW_FALLBACK,
                            this.getChannelNameFormatV1()),
                        event.getException()))
            .build();

    return Failsafe.with(reopenChannelFallbackExecutorForInsertRows)
        .get(() -> this.channel.appendRow(transformedRecord, Long.toString(offset)));
  }

  /**
   * We will reopen the channel on {@link net.snowflake.ingest.utils.SFException} and reset offset
   * in kafka. But, we will throw a custom exception to show that the streamingBuffer was not added
   * into Snowflake.
   *
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
    return Preconditions.checkNotNull(openChannelForTable(this.channelName));
  }

  @Override
  public String getChannelNameFormatV1() {
    return channel.getFullyQualifiedChannelName();
  }

  @Override
  public void setLatestConsumerGroupOffset(long consumerOffset) {
    if (consumerOffset > this.currentConsumerGroupOffset.get()) {
      this.currentConsumerGroupOffset.set(consumerOffset);
    }
  }

  @Override
  public long getOffsetPersistedInSnowflake() {
    return this.offsetPersistedInSnowflake.get();
  }

  @Override
  public long getProcessedOffset() {
    return processedOffset.get();
  }

  @Override
  public long getLatestConsumerOffset() {
    return this.currentConsumerGroupOffset.get();
  }

  @Override
  public SnowflakeTelemetryChannelStatus getSnowflakeTelemetryChannelStatus() {
    return null;
  }

  private String clientName(Map<String, String> connectorConfig) {
    return STREAMING_CLIENT_V2_PREFIX_NAME
        + connectorConfig.getOrDefault(Utils.NAME, DEFAULT_CLIENT_NAME)
        + createdClientId.getAndIncrement();
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
  private SnowflakeStreamingIngestChannel openChannelForTable(String channelName) {
    OpenChannelResult result = streamingIngestClient.openChannel(channelName, null);
    if (result.getChannelStatus().getStatusCode().equals("SUCCESS")) {
      return result.getChannel();
    } else {
      throw new RuntimeException(
          "Got openChannel() code=" + result.getChannelStatus().getStatusCode());
    }
  }

  @Override
  public void closeChannel() {
    try {
      channel.close();
    } catch (SFException e) {
//      final String errMsg =
//              String.format(
//                      "Failure closing Streaming Channel name:%s msg:%s",
//                      this.getChannelNameFormatV1(), e.getMessage());
//      this.telemetryServiceV2.reportKafkaConnectFatalError(errMsg);
      LOGGER.error(
              "Closing Streaming Channel={} encountered an exception {}: {} {}",
              this.getChannelNameFormatV1(),
              e.getClass(),
              e.getMessage(),
              Arrays.toString(e.getStackTrace()));
    } finally {
      streamingIngestClient.close();
    }
  }

  @Override
  public CompletableFuture<Void> closeChannelAsync() {
    return CompletableFuture.runAsync(
        () -> {
          channel.close();
          streamingIngestClient.close();
        });
  }

  @Override
  public boolean isChannelClosed() {
    return this.channel.isClosed();
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

    /** @return Used to LOG which API tried to invoke fallback function. */
    @Override
    public String toString() {
      return "[" + this.name() + "]";
    }
  }
}
