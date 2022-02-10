package com.snowflake.kafka.connector.internal.streaming;

import static org.apache.kafka.common.record.TimestampType.NO_TIMESTAMP_TYPE;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.snowflake.kafka.connector.internal.Logging;
import com.snowflake.kafka.connector.internal.PartitionBuffer;
import com.snowflake.kafka.connector.records.RecordService;
import com.snowflake.kafka.connector.records.SnowflakeJsonSchema;
import com.snowflake.kafka.connector.records.SnowflakeRecordContent;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
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
 */
public class TopicPartitionChannel {
  private static final Logger LOGGER = LoggerFactory.getLogger(TopicPartitionChannel.class);

  private static final long NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE = -1L;

  // more of previousInsertRowTsMs
  private long previousFlushTimeStampMs;

  /**
   * States whether this channel has had any data before. If this is false, the
   * topicPartitionChannel has recently been initialised and didnt receive any records before.
   *
   * <p>If the channel is closed, this state remains unchanged.
   */
  private boolean hasChannelReceivedAnyRecordsBefore = false;

  /* Buffer to hold JSON converted incoming SinkRecords */
  /* Eventually this buffer will only be a transient buffer and wont exist across multiple PUT api calls */
  private PartitionBuffer streamingBuffer;

  final Lock bufferLock = new ReentrantLock(true);

  // used to communicate to the streaming ingest's insertRows API
  // This channel is not everlasting
  private SnowflakeStreamingIngestChannel channel;

  // This offset is updated when Snowflake has received offset from insertRows API
  // We will update this value after calling offsetToken API for this channel
  // We will only update it during start of the channel initialization
  private long offsetPersistedInSnowflake = NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE;

  // -------- private final fields -------- //

  /* Responsible for converting records to Json */
  private final RecordService recordService;

  // ------- Kafka record related properties ------- //
  // Offset number we would want to commit back to kafka
  // This value is + 1 of what we find in snowflake.
  // It tells kafka to start sending offsets from this offset because earlier ones have been
  // ingested. (Hence it +1)
  private final AtomicLong committedOffset;

  // added to buffer before calling insertRows
  private final AtomicLong processedOffset; // processed offset

  // Ctor
  public TopicPartitionChannel(SnowflakeStreamingIngestChannel channel) {
    this.channel = channel;
    this.recordService = new RecordService();
    this.previousFlushTimeStampMs = System.currentTimeMillis();

    this.streamingBuffer = new StreamingBuffer();
    this.processedOffset = new AtomicLong(-1);
    this.committedOffset = new AtomicLong(0);
  }

  /* Update the channel */
  protected void updateStreamingIngestChannel(SnowflakeStreamingIngestChannel updatedChannel) {
    this.channel = updatedChannel;
  }

  // inserts the record into buffer
  public void insertRecordToBuffer(SinkRecord record) {
    if (!hasChannelReceivedAnyRecordsBefore) {
      LOGGER.info(
          "Received offset:{} for topic:{} as the first offset for this partition:{} after"
              + " start/restart",
          record.kafkaOffset(),
          record.topic(),
          record.kafkaPartition());
      // This will only be called once at the beginning when an offset arrives for first time
      // after connector starts
      fetchLastCommittedOffsetToken();

      this.hasChannelReceivedAnyRecordsBefore = true;
    }

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
        // write it to DLQ SNOW-451197
      } else {
        // lag telemetry, note that sink record timestamp might be null
        if (snowflakeRecord.timestamp() != null
            && snowflakeRecord.timestampType() != NO_TIMESTAMP_TYPE) {
          // TODO:SNOW-529751 telemetry
        }

        // acquire the lock before writing the record into buffer
        bufferLock.lock();
        try {
          streamingBuffer.insert(snowflakeRecord);
          processedOffset.set(snowflakeRecord.kafkaOffset());
        } finally {
          bufferLock.unlock();
        }
      }
    }
  }

  /* For EOS, fetch the offset from snowflake during only during first time after connector starts and record is received in Kafka Connector */
  @VisibleForTesting
  protected void fetchLastCommittedOffsetToken() {
    String offsetToken = this.channel.getLatestCommittedOffsetToken();
    try {
      if (offsetToken == null) {
        this.offsetPersistedInSnowflake = NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE;
      } else {
        this.offsetPersistedInSnowflake = Long.parseLong(offsetToken);
      }
      LOGGER.info(
          "Fetched offsetToken:{} for channelName:{} during channel initialization.",
          this.offsetPersistedInSnowflake,
          this.getChannelName());
    } catch (NumberFormatException e) {
      final String errorMsg =
          String.format(
              "The offsetToken string does not contain a parsable long: %s,"
                  + " offsetTokenLastRetrieved: %s. ",
              this.getChannelName(), offsetToken);
      LOGGER.error(errorMsg);
      throw new ConnectException(errorMsg, e);
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

  /**
   * Invokes insertRows API using the offsets buffered for this partition. TODO: SNOW-530369 Retry
   * handling mechanisms
   */
  public void insertBufferedRows() {
    PartitionBuffer intermediateBuffer = null;
    bufferLock.lock();
    try {
      intermediateBuffer = streamingBuffer;
      streamingBuffer = new StreamingBuffer();
    } finally {
      // release lock
      bufferLock.unlock();
    }
    // intermediate buffer can be empty here if time interval reached but kafka produced no records.
    if (intermediateBuffer.isEmpty()) {
      LOGGER.info("No Rows Buffered, returning;");
      this.previousFlushTimeStampMs = System.currentTimeMillis();
      return;
    }
    LOGGER.debug(
        "Invoking insertRows API for channel:{}, noOfRecords:{}, startOffset:{}, endOffset:{}",
        this,
        intermediateBuffer.getNumOfRecord(),
        intermediateBuffer.getFirstOffset(),
        intermediateBuffer.getLastOffset());
    InsertValidationResponse response =
        channel.insertRows(
            (Iterable<Map<String, Object>>) intermediateBuffer.getData(),
            Long.toString(intermediateBuffer.getLastOffset()));

    // need to update the flush time(last I called insertrows)
    this.previousFlushTimeStampMs = System.currentTimeMillis();

    // should we retry if response.hasErrors() == true?
    // how can we retry if buffer was cleared?
    // lets have an exponential back off and jitter retry policy

    if (response.hasErrors()) {
      LOGGER.error(
          "Inserted rows API response has errors for rows with offset start:{} and end:{}",
          intermediateBuffer.getFirstOffset(),
          intermediateBuffer.getLastOffset());
      response
          .getInsertErrors()
          .forEach(
              insertError -> {
                LOGGER.error(
                    "Insert row Error message:{}, with ex:{}",
                    insertError.getMessage(),
                    insertError.getException().getMessage());
              });
    }
  }

  // should we rely on precommits frequency to give us the last committed offset token?
  // or should we have a BG thread polling on every channel to update the Atomic long and simply
  // return the atomic long's current value
  // For now we will rely on preCommit API to fetch the inserted offsets into Snowflake
  // TODO: SNOW-529755 POLL committed offsets in backgraound thread
  /**
   * Get committed offset from Snowflake. It does an HTTP call internally to find out what was the
   * last offset inserted.
   *
   * <p>If committedOffset fetched from Snowflake is null, we would return -1(default value of
   * committedOffset) back to original call. (-1) would return an empty Map of partition and offset
   * back to kafka. (We)
   *
   * <p>Check {@link com.snowflake.kafka.connector.SnowflakeSinkTask#preCommit(Map)}
   *
   * <p>Else, we will convert this offset and return the offset which is safe to commit inside
   * Kafka.
   *
   * @return offsetToken present in Snowflake, else -1
   */
  public long getCommittedOffset() {
    LOGGER.debug(
        "Fetching last committed offset for partition channel:{}",
        this.channel.getFullyQualifiedName());
    String lastOffsetCommitted = channel.getLatestCommittedOffsetToken();
    LOGGER.info(
        "Last committed offset for partition channel:{} is :{}",
        this.channel.getFullyQualifiedName(),
        lastOffsetCommitted);
    if (Strings.isNullOrEmpty(lastOffsetCommitted)) {
      return committedOffset.get();
    } else {
      // Return an offset which is + 1 of what was present in snowflake.
      // Idea of sending + 1 back to Kafka is that it should start sending offsets after task
      // restart from this offset
      return committedOffset.updateAndGet(operand -> Long.parseLong(lastOffsetCommitted) + 1);
    }
  }

  /* Close channel associated to this partition */
  public void closeChannel() {
    try {
      this.channel.close().get();
    } catch (InterruptedException | ExecutionException e) {
      LOGGER.error(
          Logging.logMessage(
              "Failure closing Streaming Channel name:{} msg:{}, cause:{}",
              this.getChannelName(),
              e.getMessage(),
              Arrays.toString(e.getCause().getStackTrace())));
    }
  }

  /* Return true is channel is closed. Caller should handle the logic for reopening the channel if it is closed. */
  public boolean isChannelClosed() {
    return this.channel.isClosed();
  }

  // ------ GETTERS ------ //

  public PartitionBuffer getStreamingBuffer() {
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
        + ", hasChannelInitialized="
        + hasChannelReceivedAnyRecordsBefore
        + ", channel="
        + this.getChannelName()
        + ", committedOffset="
        + committedOffset
        + ", processedOffset="
        + processedOffset
        + '}';
  }

  /* For testing */
  protected long getOffsetPersistedInSnowflake() {
    return this.offsetPersistedInSnowflake;
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
  private class StreamingBuffer extends PartitionBuffer<List<Map<String, Object>>> {
    // Used to buffer rows per channel
    // Map has key of column name and Object is its value
    // For KC, it will be metadata and content columns.
    // Every List element corresponds to one record(offset) of a partition
    private final List<Map<String, Object>> tableRows;

    private StreamingBuffer() {
      super();
      tableRows = new ArrayList<>();
    }

    @Override
    public void insert(SinkRecord record) {

      // has two keys(two columns), each of the values are JsonNode
      Map<String, Object> tableRow = recordService.getProcessedRecordForStreamingIngest(record);

      if (tableRows.size() == 0) {
        setFirstOffset(record.kafkaOffset());
      }

      tableRows.add(tableRow);

      // probably do below things in a separate method.
      // call it collect buffer metrics
      setNumOfRecord(getNumOfRecord() + 1);
      setLastOffset(record.kafkaOffset());
      // need to loop through the map and get the object node
      tableRow.forEach(
          (key, value) -> {
            setBufferSize(getBufferSize() + ((String) value).length() * 2); // 1 char = 2 bytes
          });
    }

    /* Get all rows which are present in this list. Each map corresponds to one row whose keys are column names. */
    @Override
    public List<Map<String, Object>> getData() {
      LOGGER.info(
          "Get rows for streaming ingest. {} records, {} bytes, offset {} - {}",
          getNumOfRecord(),
          getBufferSize(),
          getFirstOffset(),
          getLastOffset());
      return tableRows;
    }
  }
}
