package com.snowflake.kafka.connector.internal.streaming;

import static org.apache.kafka.common.record.TimestampType.NO_TIMESTAMP_TYPE;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.snowflake.kafka.connector.internal.PartitionBuffer;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import com.snowflake.kafka.connector.records.RecordService;
import com.snowflake.kafka.connector.records.SnowflakeJsonSchema;
import com.snowflake.kafka.connector.records.SnowflakeRecordContent;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.internal.InsertValidationResponse;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Instance which holds a StreamingIngestChannel which is based on a topic and its partition.
 *
 * <p>Please note: No two tasks can work on a same partition
 *
 * <p>@see <a
 * href="https://kafka.apache.org/24/javadoc/org/apache/kafka/connect/sink/SinkTask.html">here</a>
 */
public class TopicPartitionChannel {
  private static final Logger LOGGER = LoggerFactory.getLogger(TopicPartitionChannel.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  // more of previousInsertRowTsMs
  private long previousFlushTimeStampMs;

  private boolean hasChannelInitialized = false;

  // We will use this buffer only for storing temporary records which are converted into Snowflake
  // understood table format.
  // Records will be buffered after they are being processed. (Key and values are converted to Json
  // format)
  // The data from this buffer will then be used to call insertRows API
  // Hence, buffer here is just transient and doesnt live long enough.
  private PartitionBuffer streamingBuffer;

  // This arrayList corresponds to records fetched from put API of SnowflakeSinkTask
  private List<SinkRecord> sinkRecordsFromKafka = new ArrayList<>();

  // TODO: Do we even need this lock?
  // Can we pass in the List to a function which does the calculation in place?
  // Anyways no two tasks will work on a partition and instance of TopicPartitionChannel is per
  // topic per partition
  // There wont be any concurrency problems testing will be much easy
  final Lock bufferLock = new ReentrantLock(true);

  final ReadWriteLock sinkRecordListLock = new ReentrantReadWriteLock(true);

  // -------- private final fields -------- //

  // used to communicate to the streaming ingest's insertRows API
  private final SnowflakeStreamingIngestChannel channel;

  private final SnowflakeConnectionService snowflakeConnectionService;

  private final RecordService recordService;

  private final String tableName;

  // Kafka record properties
  // For which
  private final AtomicLong committedOffset; // loaded offset + 1

  // assume this offset as an offset for which we called insertRows API
  private final AtomicLong flushedOffset; // flushed offset (file on stage)

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
    this.flushedOffset = new AtomicLong(-1);
    this.committedOffset = new AtomicLong(0);
  }

  public void processAndInsertSinkRecords(final List<SinkRecord> recordsFromPut) {
    LOGGER.debug(
        "Processing {} records from Kafka. TopicPartitionChannelInfo:{}",
        recordsFromPut.size(),
        this);
    recordsFromPut.forEach(this::insertRecordToBuffer);

    LOGGER.debug(
        "Successfully Processed/buffered {} records from Kafka for TopicPartitionChannelInfo:{}",
        this.streamingBuffer.getNumOfRecord(),
        this);
    insertBufferedRows();
  }

  // inserts the record into buffer
  public void insertRecordToBuffer(SinkRecord record) {
    // init pipe
    if (!hasChannelInitialized) {
      // This will only be called once at the beginning when an offset arrives for first time
      // after connector starts/rebalance
      init(record.kafkaOffset());
      //            metricsJmxReporter.start();
      this.hasChannelInitialized = true;
    }

    // ignore ingested records
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
        // write it to DLQ
      } else {
        // lag telemetry, note that sink record timestamp might be null
        if (snowflakeRecord.timestamp() != null
            && snowflakeRecord.timestampType() != NO_TIMESTAMP_TYPE) {
          //                    pipeStatus.updateKafkaLag(System.currentTimeMillis() -
          // snowflakeRecord.timestamp());
        }

        // acquire the lock before adding record to temporary list
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

  private void init(long recordOffset) {
    //    createTableIfNotExists();

    try {
      startCleaner(recordOffset);
    } catch (Exception e) {
      LOGGER.error("Cleaner and Flusher threads shut down before initialization");
    }
  }

  private void createTableIfNotExists() {
    if (snowflakeConnectionService.tableExist(this.tableName)) {
      if (snowflakeConnectionService.isTableCompatible(this.tableName)) {
        LOGGER.info("Using existing table {}.", this.tableName);
      } else {
        throw SnowflakeErrors.ERROR_5003.getException("table name: " + this.tableName);
      }
    } else {
      LOGGER.info("Creating new table {}.", tableName);
      snowflakeConnectionService.createTable(tableName);
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
    LOGGER.info(
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
  // or should we have a BG thread polling on every channel to updae the Atomic long and simply
  // return the atomic long's current value
  public long getCommittedOffset() {
    LOGGER.info(
        "Fetching last committed offset for partition channel:{}",
        this.channel.getFullyQualifiedName());
    String lastOffsetCommitted = channel.getLatestCommittedOffsetToken();
    LOGGER.info(
        "Last committed offset for partition channel:{} is :{}",
        this.channel.getFullyQualifiedName(),
        lastOffsetCommitted);
    if (Strings.isNullOrEmpty(lastOffsetCommitted)
        || lastOffsetCommitted.equalsIgnoreCase("null")) {
      return committedOffset.get();
    } else {
      return committedOffset.updateAndGet(operand -> Long.parseLong(lastOffsetCommitted));
    }
  }

  public void closeChannel() {
    try {
      this.channel.close().get(1000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ExecutionException e) {
      e.printStackTrace();
    } catch (TimeoutException e) {
      e.printStackTrace();
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
    return this.channel.getFullyQualifiedName();
  }

  // ------ SETTERS ------ //

  public void setSinkRecordsFromKafka(List<SinkRecord> recordsFromKafka) {
    // Even though no two tasks can receive records from same partition, we would like to acquire a
    // write lock before writing it to array list
    // And, topicPartitionChannel object is per topic and partition
    sinkRecordListLock.writeLock().lock();
    try {
      LOGGER.debug(
          "Adding {} records into topicPartitionChannel:{}",
          recordsFromKafka.size(),
          getChannelName());
      sinkRecordsFromKafka.addAll(recordsFromKafka);
    } finally {
      sinkRecordListLock.writeLock().unlock();
    }
  }

  // ------ INNER CLASS ------ //
  /**
   * A buffer which holds the rows before calling insertRows API. It implements the PartitionBuffer
   * class which has all common fields about a buffer.
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

  public String toString() {
    return "StreamingChannelName:"
        + getChannelName()
        + ", FullyQualifiedTableName:{}"
        + this.channel.getFullyQualifiedTableName();
  }
}
