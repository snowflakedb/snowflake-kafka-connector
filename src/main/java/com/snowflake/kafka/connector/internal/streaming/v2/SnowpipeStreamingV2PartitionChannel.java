package com.snowflake.kafka.connector.internal.streaming.v2;

import static com.snowflake.kafka.connector.internal.SnowflakeErrors.ERROR_5027;
import static com.snowflake.kafka.connector.internal.SnowflakeErrors.ERROR_5028;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.snowflake.ingest.streaming.AppendResult;
import com.snowflake.ingest.streaming.OpenChannelResult;
import com.snowflake.ingest.streaming.SFException;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.KCLogger;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.metrics.MetricsJmxReporter;
import com.snowflake.kafka.connector.internal.streaming.LatestCommitedOffsetTokenExecutor;
import com.snowflake.kafka.connector.internal.streaming.StreamingClientProperties;
import com.snowflake.kafka.connector.internal.streaming.StreamingErrorHandler;
import com.snowflake.kafka.connector.internal.streaming.StreamingRecordService;
import com.snowflake.kafka.connector.internal.streaming.TopicPartitionChannelInsertionException;
import com.snowflake.kafka.connector.internal.streaming.channel.TopicPartitionChannel;
import com.snowflake.kafka.connector.internal.streaming.telemetry.SnowflakeTelemetryChannelCreation;
import com.snowflake.kafka.connector.internal.streaming.telemetry.SnowflakeTelemetryChannelStatus;
import com.snowflake.kafka.connector.internal.streaming.validation.RowSchema;
import com.snowflake.kafka.connector.internal.streaming.validation.RowSchemaManager;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryService;
import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeExecutor;
import dev.failsafe.Fallback;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicLong;
import net.snowflake.ingest.streaming.InsertValidationResponse;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.sink.SinkTaskContext;

public class SnowpipeStreamingV2PartitionChannel implements TopicPartitionChannel {
  private static final KCLogger LOGGER =
      new KCLogger(SnowpipeStreamingV2PartitionChannel.class.getName());
  private final StreamingClientProperties streamingClientProperties;
  private final StreamingIngestClientV2Provider streamingIngestClientV2Provider;

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

  // Used for telemetry and logging only
  private final AtomicLong currentConsumerGroupOffset =
      new AtomicLong(NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE);

  private long lastAppendRowsOffset = NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE;

  // Indicates whether we need to skip and discard any leftover rows in the current batch, this
  // could happen when the channel gets invalidated and reset
  private boolean needToSkipCurrentBatch = false;

  // Topic partition Object from connect consisting of topic and partition
  private final TopicPartition topicPartition;

  private final String channelName;

  /* table is required for opening the channel */
  private final String tableName;

  /** Available from {@link SinkTask} which has access to various utility methods. */
  private final SinkTaskContext sinkTaskContext;

  // Whether schema evolution will be done on this channel
  private final boolean schemaEvolutionEnabled;

  private final SnowflakeTelemetryChannelStatus snowflakeTelemetryChannelStatus;

  private final StreamingRecordService streamingRecordService;

  /**
   * Used to send telemetry to Snowflake. Currently, TelemetryClient created from a Snowflake
   * Connection Object, i.e. not a session-less Client
   */
  private final SnowflakeTelemetryService telemetryServiceV2;

  private final FailsafeExecutor<Long> offsetTokenExecutor;

  private final RowSchemaManager rowSchemaManager;

  private final String pipeName;

  private final Map<String, String> connectorConfig;

  private final StreamingErrorHandler streamingErrorHandler;

  public SnowpipeStreamingV2PartitionChannel(
      String tableName,
      final boolean schemaEvolutionEnabled,
      String channelName,
      TopicPartition topicPartition,
      SnowflakeConnectionService conn,
      Map<String, String> connectorConfig,
      StreamingRecordService streamingRecordService,
      SinkTaskContext sinkTaskContext,
      boolean enableCustomJMXMonitoring,
      MetricsJmxReporter metricsJmxReporter,
      StreamingIngestClientV2Provider streamingIngestClientV2Provider,
      RowSchemaManager rowSchemaManager,
      StreamingErrorHandler streamingErrorHandler,
      SSv2PipeCreator ssv2PipeCreator) {
    this.tableName = tableName;
    this.schemaEvolutionEnabled = schemaEvolutionEnabled;
    this.channelName = channelName;
    this.topicPartition = topicPartition;
    this.connectorConfig = connectorConfig;
    this.streamingRecordService = streamingRecordService;
    this.sinkTaskContext = sinkTaskContext;
    this.streamingIngestClientV2Provider = streamingIngestClientV2Provider;
    this.rowSchemaManager = rowSchemaManager;
    this.streamingErrorHandler = streamingErrorHandler;

    this.telemetryServiceV2 = conn.getTelemetryClient();
    this.pipeName = PipeNameProvider.pipeName(connectorConfig.get(Utils.NAME), tableName);
    this.streamingClientProperties = new StreamingClientProperties(connectorConfig);

    ssv2PipeCreator.createPipeIfNotExists();
    this.channel = openChannelForTable(channelName);
    this.offsetTokenExecutor =
        LatestCommitedOffsetTokenExecutor.getExecutor(
            this.getChannelNameFormatV1(),
            Exception.class,
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
    final long startTime = System.currentTimeMillis();
    this.snowflakeTelemetryChannelStatus =
        new SnowflakeTelemetryChannelStatus(
            tableName,
            connectorName,
            channelName,
            startTime,
            enableCustomJMXMonitoring,
            metricsJmxReporter,
            this.offsetPersistedInSnowflake,
            this.processedOffset,
            this.currentConsumerGroupOffset);

    this.telemetryServiceV2.reportKafkaPartitionStart(
        new SnowflakeTelemetryChannelCreation(tableName, channelName, startTime));

    setOffsetInKafka(lastCommittedOffsetToken);
  }

  private void setOffsetInKafka(long lastCommittedOffsetToken) {
    if (lastCommittedOffsetToken != NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE) {
      this.sinkTaskContext.offset(this.topicPartition, lastCommittedOffsetToken + 1L);
    } else {
      LOGGER.info(
          "TopicPartitionChannel:{}, offset token is NULL, will rely on Kafka to send us the"
              + " correct offset instead",
          this.getChannelNameFormatV1());
    }
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
          "Channel {} - skipping current record - expected offset {} but received {}. The"
              + " current offset stored in Snowflake: {}",
          this.getChannelNameFormatV1(),
          currentProcessedOffset,
          kafkaSinkRecord.kafkaOffset(),
          currentOffsetPersistedInSnowflake);
    }
  }

  private void transformAndSend(SinkRecord kafkaSinkRecord) {
    try {
      Map<String, Object> transformedRecord = streamingRecordService.transformData(kafkaSinkRecord);
      if (schemaEvolutionEnabled) {
        Optional<RowSchema.Error> error = validateRecord(transformedRecord);
        if (error.isPresent()) {
          LOGGER.info(
              "Record doesn't match table schema. " + " topic={}, partition={}, offset={}",
              kafkaSinkRecord.topic(),
              kafkaSinkRecord.kafkaPartition(),
              kafkaSinkRecord.kafkaOffset());
          streamingErrorHandler.handleError(List.of(error.get().cause()), kafkaSinkRecord);
          this.processedOffset.set(kafkaSinkRecord.kafkaOffset());
          return;
        }
      }

      // for schema evolution all identifiers are quoted
      // in SSv2 we still need quoted identifiers for ALTER TABLE statements
      // but unquoted for map keys that are passed to ssv2
      Map<String, Object> unquotedTransformedRecord = unquoteIdentifiers(transformedRecord);
      if (!transformedRecord.isEmpty()) {
        insertRowWithFallback(unquotedTransformedRecord, kafkaSinkRecord.kafkaOffset());
        this.processedOffset.set(kafkaSinkRecord.kafkaOffset());
      }
    } catch (TopicPartitionChannelInsertionException ex) {
      // Suppressing the exception because other channels might still continue to ingest
      LOGGER.warn(
          "Failed to insert row for channel:{}. Will be retried by Kafka. Exception: {}",
          this.getChannelNameFormatV1(),
          ex);
    }
  }

  private Optional<RowSchema.Error> validateRecord(Map<String, Object> transformedRecord) {
    Map<String, Object> fieldsToValidate = new HashMap<>(transformedRecord);
    // skip RECORD_METADATA cause SSv1 validations don't accept POJOs
    fieldsToValidate.remove("RECORD_METADATA");
    return Optional.ofNullable(
        rowSchemaManager.get(tableName, connectorConfig).validate(fieldsToValidate));
  }

  @Override
  public void waitForLastProcessedRecordCommitted() {
    if (lastAppendRowsOffset == NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE) {
      return;
    }

    streamingIngestClientV2Provider
        .getClient(connectorConfig, pipeName, streamingClientProperties)
        .initiateFlush();

    WaitForLastOffsetCommittedPolicy.getPolicy(
        () -> {
          long offsetCommittedToBackend = fetchLatestCommittedOffsetFromSnowflake();
          if (offsetCommittedToBackend == lastAppendRowsOffset) {
            return true;
          }
          throw ERROR_5027.getException();
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
   * Uses {@link Fallback} API to reopen the channel if insertRows throws {@link SFException}.
   *
   * <p>We have deliberately not performed retries on insertRows because it might slow down overall
   * ingestion and introduce lags in committing offsets to Kafka.
   *
   * <p>Note that insertRows API does perform channel validation which might throw SFException if
   * channel is invalidated.
   *
   * <p>It can also send errors {@link InsertValidationResponse.InsertError} in form of response
   * inside {@link InsertValidationResponse}
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
                        "Failed Attempt to invoke the appendRow API for channel: {}. Exception: {}",
                        getChannelNameFormatV1(),
                        event.getLastException()))
            .onFailure(
                event ->
                    LOGGER.error(
                        "{} Failed to open Channel or fetching offsetToken for channel:{}."
                            + " Exception: {}",
                        StreamingApiFallbackInvoker.APPEND_ROW_FALLBACK,
                        this.getChannelNameFormatV1(),
                        event.getException()))
            .build();

    return Failsafe.with(reopenChannelFallbackExecutorForInsertRows)
        .get(
            () -> {
              AppendResult result =
                  this.channel.appendRow(transformedRecord, Long.toString(offset));
              this.lastAppendRowsOffset = offset;
              return result;
            });
  }

  /**
   * We will reopen the channel on {@link SFException} and reset offset in kafka. But, we will throw
   * a custom exception to show that records were not added into Snowflake.
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
    SnowflakeStreamingIngestChannel newChannel = openChannelForTable(channelName);

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
   * Resets the offset in kafka, resets metadata related to offsets. If we don't get a valid offset
   * token (because of a table recreation or channel inactivity), we will rely on kafka to send us
   * the correct offset
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

    // Set the flag so that any leftover rows should be skipped, it will be
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
    SnowflakeStreamingIngestClient streamingIngestClient =
        streamingIngestClientV2Provider.getClient(
            connectorConfig, pipeName, streamingClientProperties);
    OpenChannelResult result = streamingIngestClient.openChannel(channelName, null);
    if (result.getChannelStatus().getStatusCode().equals("SUCCESS")) {
      return result.getChannel();
    } else {
      throw ERROR_5028.getException(
          String.format(
              "Failed to open channel %s. Error code %s",
              channelName, result.getChannelStatus().getStatusCode()));
    }
  }

  @Override
  public void closeChannel() {
    try {
      channel.close();

      // telemetry and metrics
      this.telemetryServiceV2.reportKafkaPartitionUsage(this.snowflakeTelemetryChannelStatus, true);
      this.snowflakeTelemetryChannelStatus.tryUnregisterChannelJMXMetrics();
    } catch (SFException e) {
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
      return CompletableFuture.runAsync(() -> channel.close());
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

  @Override
  public boolean isChannelClosed() {
    return this.channel.isClosed();
  }

  @Override
  public String getChannelNameFormatV1() {
    return channel.getFullyQualifiedChannelName();
  }

  @Override
  public String toString() {
    SnowflakeStreamingIngestClient streamingIngestClient =
        streamingIngestClientV2Provider.getClient(
            connectorConfig, pipeName, streamingClientProperties);
    return MoreObjects.toStringHelper(this)
        .add("offsetPersistedInSnowflake", this.offsetPersistedInSnowflake)
        .add("channelName", this.getChannelNameFormatV1())
        .add("isStreamingIngestClientClosed", streamingIngestClient.isClosed())
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
