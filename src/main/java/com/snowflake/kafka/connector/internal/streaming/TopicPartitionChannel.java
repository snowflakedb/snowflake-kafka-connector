package com.snowflake.kafka.connector.internal.streaming;

import static org.apache.kafka.common.record.TimestampType.NO_TIMESTAMP_TYPE;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.snowflake.kafka.connector.internal.Logging;
import com.snowflake.kafka.connector.internal.PartitionBuffer;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
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
  private static final ObjectMapper MAPPER = new ObjectMapper();

  // more of previousInsertRowTsMs
  private long previousFlushTimeStampMs;

  private boolean hasChannelInitialized = false;

  /* Buffer to hold JSON converted incoming SinkRecords */
  /* Eventually this buffer will only be a transient buffer and wont exist across multiple PUT api calls */
  private PartitionBuffer streamingBuffer;

  final Lock bufferLock = new ReentrantLock(true);

  // -------- private final fields -------- //

  // used to communicate to the streaming ingest's insertRows API
  private final SnowflakeStreamingIngestChannel channel;

  private final SnowflakeConnectionService snowflakeConnectionService;

  /* Responsible for converting records to Json */
  private final RecordService recordService;

  private final String tableName;

  // ------- Kafka record related properties ------- //
  // Offset number we would want to commit back to kafka
  // This value is + 1 of what we find in snowflake.
  // It tells kafka to start sending offsets from this offset because earlier ones have been
  // ingested. (Hence it +1)
  private final AtomicLong committedOffset;

  // added to buffer before calling insertRows
  private final AtomicLong processedOffset; // processed offset

  // Ctor
  public TopicPartitionChannel(
      SnowflakeStreamingIngestChannel channel,
      SnowflakeConnectionService connectionService,
      final String tableName) {
    this.channel = channel;
    this.snowflakeConnectionService = connectionService;
    this.recordService = new RecordService();
    this.tableName = tableName;
    this.previousFlushTimeStampMs = System.currentTimeMillis();

    this.streamingBuffer = new StreamingBuffer();
    this.processedOffset = new AtomicLong(-1);
    this.committedOffset = new AtomicLong(0);
  }

  // inserts the record into buffer
  public void insertRecordToBuffer(SinkRecord record) {
    if (!hasChannelInitialized) {
      // This will only be called once at the beginning when an offset arrives for first time
      // after connector starts/rebalance
      init(record.kafkaOffset());
      //            metricsJmxReporter.start();
      this.hasChannelInitialized = true;
    }

    // ignore ingested files
    if (record.kafkaOffset() > processedOffset.get()) {
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

  /* TODO: SNOW-529753 Deal with rebalances and restarts */
  /* TODO: SNOW-536429 For EOS, also fetch the offset from snowflake during rebalance/restart */
  private void init(long recordOffset) {
    try {
      startCleaner(recordOffset);
    } catch (Exception e) {
      LOGGER.error("Cleaner and Flusher threads shut down before initialization");
    }
  }

  // we will deal with restart/rebalance issues after we get the basics working.
  void startCleaner(long recordOffset) {}

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

  // ------ GETTERS ------ //

  public PartitionBuffer getStreamingBuffer() {
    return streamingBuffer;
  }

  public long getPreviousFlushTimeStampMs() {
    return previousFlushTimeStampMs;
  }

  public String getChannelName() {
    return this.channel.getFullyQualifiedTableName();
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
