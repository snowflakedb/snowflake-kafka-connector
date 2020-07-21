package com.snowflake.kafka.connector.internal;

import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.records.RecordService;
import com.snowflake.kafka.connector.records.SnowflakeJsonSchema;
import com.snowflake.kafka.connector.records.SnowflakeMetadataConfig;
import com.snowflake.kafka.connector.records.SnowflakeRecordContent;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class SnowflakeSinkServiceV1 extends Logging implements SnowflakeSinkService
{
  private static final long ONE_HOUR = 60 * 60 * 1000L;
  private static final long TEN_MINUTES = 10 * 60 * 1000L;
  private static final long CLEAN_TIME = 60 * 1000L; //one minutes

  private long flushTime; // in seconds
  private long fileSize;
  private long recordNum;
  private final SnowflakeConnectionService conn;
  private final Map<String, ServiceContext> pipes;
  private final RecordService recordService;
  private boolean isStopped;
  private final SnowflakeTelemetryService telemetryService;
  private Map<String, String> topic2TableMap;

  SnowflakeSinkServiceV1(SnowflakeConnectionService conn)
  {
    if (conn == null || conn.isClosed())
    {
      throw SnowflakeErrors.ERROR_5010.getException();
    }

    this.fileSize = SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES_DEFAULT;
    this.recordNum = SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS_DEFAULT;
    this.flushTime = SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC_DEFAULT;
    this.pipes = new HashMap<>();
    this.conn = conn;
    this.recordService = new RecordService();
    isStopped = false;
    this.telemetryService = conn.getTelemetryClient();
    this.topic2TableMap = new HashMap<>();
  }

  @Override
  public void startTask(final String tableName, final String topic,
                        final int partition)
  {
    String stageName = Utils.stageName(conn.getConnectorName(), tableName);
    String nameIndex = getNameIndex(topic, partition);
    if (pipes.containsKey(nameIndex))
    {
      logError("task is already registered, name: {}", nameIndex);
    }
    else
    {
      String pipeName = Utils.pipeName(conn.getConnectorName(), tableName,
        partition);

      pipes.put(nameIndex, new ServiceContext(tableName, stageName, pipeName,
        conn, partition));
    }

  }

  @Override
  public void insert(final Collection<SinkRecord> records)
  {
    // note that records can be empty
    for (SinkRecord record : records)
    {
      insert(record);
    }
    // check all sink context to see if they need to be flushed
    for (ServiceContext pipe : pipes.values()) {
      if (pipe.shouldFlush())
      {
        pipe.flushBuffer();
      }
    }
  }

  @Override
  public void insert(SinkRecord record)
  {
    String nameIndex = getNameIndex(record.topic(), record.kafkaPartition());
    //init a new topic partition
    if (!pipes.containsKey(nameIndex))
    {
      logWarn("Topic: {} Partition: {} hasn't been initialized by OPEN " +
              "function", record.topic(), record.kafkaPartition());
      startTask(Utils.tableName(record.topic(), this.topic2TableMap),
                record.topic(), record.kafkaPartition());
    }
    pipes.get(nameIndex).insert(record);
  }

  @Override
  public long getOffset(final TopicPartition topicPartition)
  {
    String name = getNameIndex(topicPartition.topic(),
      topicPartition.partition());
    if (pipes.containsKey(name))
    {
      return pipes.get(name).getOffset();
    }
    else
    {
      logError("Failed to find offset of Topic: {}, Partition: {}, sink " +
        "service hasn't been initialized", topicPartition.topic(),
        topicPartition.partition());
      return 0;
    }
  }

  // used for testing only
  @Override
  public void callAllGetOffset()
  {
    for (ServiceContext pipe : pipes.values()) {
      pipe.getOffset();
    }
  }

  @Override
  public void close(Collection<TopicPartition> partitions)
  {
    partitions.forEach(tp -> {
      String name = getNameIndex(tp.topic(), tp.partition());
      ServiceContext sc = pipes.remove(name);
      if (sc != null)
      {
        try
        {
          sc.close();
        } catch (Exception e)
        {
          logError("Failed to close sink service for Topic: {}, Partition: " +
            "{}\nMessage:{}", tp.topic(), tp.partition(), e.getMessage());
        }
      }
      else
      {
        logWarn("Failed to close sink service for Topic: {}, Partition: {}, " +
          "sink service hasn't been initialized", tp.topic(), tp.partition());
      }
    });
  }

  @Override
  public void closeAll()
  {
    this.isStopped = true; // release all cleaner and flusher threads
    pipes.forEach(
      (name, context) -> context.close()
    );
    pipes.clear();
  }

  @Override
  public boolean isClosed()
  {
    return this.isStopped;
  }

  @Override
  public void setRecordNumber(final long num)
  {
    if (num < 0)
    {
      logError("number of record in each file is {}, it is negative, reset to" +
        " 0");
      this.recordNum = 0;
    }
    else
    {
      this.recordNum = num;
      logInfo("set number of record limitation to {}", num);
    }
  }

  @Override
  public void setFileSize(final long size)
  {
    if (size > SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES_MAX)
    {
      logError("file size is {} bytes, it is larger than the maximum file " +
          "size {} bytes, reset to the maximum file size",
        size, SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES_MAX
      );
      this.fileSize = SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES_MAX;
    }
    else
    {
      this.fileSize = size;
      logInfo("set file size limitation to {} bytes", size);
    }
  }

  @Override
  public void setFlushTime(final long time)
  {
    if (time < SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC_MIN)
    {
      logError("flush time is {} seconds, it is smaller than the minimum " +
          "flush time {} seconds, reset to the minimum flush time",
        time, SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC_MIN
      );
      this.flushTime = SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC_MIN;
    }
    else
    {
      this.flushTime = time;
      logInfo("set flush time to {} seconds", time);
    }

  }

  @Override
  public void setTopic2TableMap(Map<String, String> topic2TableMap)
  {
    this.topic2TableMap = topic2TableMap;
  }

  @Override
  public void setMetadataConfig(SnowflakeMetadataConfig configMap)
  {
    this.recordService.setMetadataConfig(configMap);
  }

  @Override
  public long getRecordNumber()
  {
    return this.recordNum;
  }

  @Override
  public long getFlushTime()
  {
    return this.flushTime;
  }

  @Override
  public long getFileSize()
  {
    return this.fileSize;
  }

  private static String getNameIndex(String topic, int partition)
  {
    return topic + "_" + partition;
  }

  private class ServiceContext
  {
    private final String tableName;
    private final String stageName;
    private final String pipeName;
    private final SnowflakeConnectionService conn;
    private final SnowflakeIngestionService ingestionService;
    private List<String> fileNames;
    private List<String> cleanerFileNames;
    private PartitionBuffer buffer;
    private final String prefix;
    private final AtomicLong committedOffset; // loaded offset + 1
    private final AtomicLong flushedOffset;   // flushed offset (file on stage)
    private final AtomicLong processedOffset; // processed offset
    private long previousFlushTimeStamp;

    //threads
    private final ExecutorService cleanerExecutor;
    private final Lock bufferLock;
    private final Lock fileListLock;
    private final Lock usageDataLock;

    //telemetry
    private long startTime;
    private long totalNumberOfRecord;
    private long totalSizeOfData;

    //make the initialization lazy
    private boolean hasInitialized = false;


    private ServiceContext(String tableName, String stageName,
                           String pipeName, SnowflakeConnectionService conn,
                           int partition)
    {
      this.pipeName = pipeName;
      this.tableName = tableName;
      this.stageName = stageName;
      this.conn = conn;
      this.fileNames = new LinkedList<>();
      this.cleanerFileNames = new LinkedList<>();
      this.buffer = new PartitionBuffer();
      this.ingestionService = conn.buildIngestService(stageName, pipeName);
      this.prefix = FileNameUtils.filePrefix(conn.getConnectorName(), tableName, partition);
      this.processedOffset = new AtomicLong(-1);
      this.flushedOffset = new AtomicLong(-1);
      this.committedOffset = new AtomicLong(0);
      this.previousFlushTimeStamp = System.currentTimeMillis();

      this.bufferLock = new ReentrantLock();
      this.fileListLock = new ReentrantLock();
      this.usageDataLock = new ReentrantLock();

      this.totalNumberOfRecord = 0;
      this.totalSizeOfData = 0;
      this.startTime = System.currentTimeMillis();

      this.cleanerExecutor = Executors.newSingleThreadExecutor();

      logInfo("pipe: {} - service started", pipeName);
    }

    private void init()
    {
      logInfo("init pipe: {}", pipeName);
      // wait for sinkConnector to start
      createTableAndStage();
      // recover will only check pipe status and create pipe if it does not exist.
      recover();

      try
      {
        startCleaner();
      } catch (Exception e)
      {
        logWarn("Cleaner and Flusher threads shut down before initialization");
      }

    }

    private void startCleaner()
    {
      // when cleaner start, scan stage for all files of this pipe
      List<String> tmpFileNames = conn.listStage(stageName, prefix);
      fileListLock.lock();
      try
      {
        cleanerFileNames.addAll(tmpFileNames);
      } finally
      {
        fileListLock.unlock();
      }

      cleanerExecutor.submit(
        () ->
        {
          logInfo("pipe {}: cleaner started", pipeName);
          while (!isStopped)
          {
            try
            {
              Thread.sleep(CLEAN_TIME);
              checkStatus();
              if (System.currentTimeMillis() - startTime > ONE_HOUR)
              {
                sendUsageReport();
              }
            } catch (InterruptedException e)
            {
              logInfo("Cleaner terminated by an interrupt:\n{}", e.getMessage());
              break;
            }
          }
        }
      );
    }

    private void stopCleaner()
    {
      cleanerExecutor.shutdownNow();
      logInfo("pipe {}: cleaner terminated", pipeName);
    }

    private void insert(final SinkRecord record)
    {
      //init pipe
      if (!hasInitialized)
      {
        init();
        this.hasInitialized = true;
      }

      //ignore ingested files
      if (record.kafkaOffset() > processedOffset.get())
      {
        SinkRecord snowflakeRecord = record;
        if (shouldConvertContent(snowflakeRecord.value()))
        {
          snowflakeRecord = handleNativeRecord(snowflakeRecord, false);
        }
        if (shouldConvertContent(snowflakeRecord.key()))
        {
          snowflakeRecord = handleNativeRecord(snowflakeRecord, true);
        }

        //broken record
        if (isRecordBroken(snowflakeRecord))
        {
          writeBrokenDataToTableStage(snowflakeRecord);
          //don't move committed offset in this case
          //only move it in the normal cases
        }
        else
        {
          PartitionBuffer tmpBuff = null;
          bufferLock.lock();
          try
          {
            processedOffset.set(snowflakeRecord.kafkaOffset());
            buffer.insert(snowflakeRecord);
            if (buffer.getBufferSize() >= getFileSize() ||
                (getRecordNumber() != 0 && buffer.getNumOfRecord() >= getRecordNumber()))
            {
              tmpBuff = buffer;
              this.buffer = new PartitionBuffer();
            }
          } finally
          {
            bufferLock.unlock();
          }

          if (tmpBuff != null)
          {
            flush(tmpBuff);
          }
        }
      }

    }

    private boolean shouldConvertContent(final Object content)
    {
      return content != null && !(content instanceof SnowflakeRecordContent);
    }

    private boolean isRecordBroken(final SinkRecord record)
    {
      return isContentBroken(record.value()) || isContentBroken(record.key());
    }

    private boolean isContentBroken(final Object content)
    {
      return content != null && ((SnowflakeRecordContent) content).isBroken();
    }

    private SinkRecord handleNativeRecord(SinkRecord record, boolean isKey)
    {
      SnowflakeRecordContent newSFContent;
      Schema schema = isKey ? record.keySchema() : record.valueSchema();
      Object content = isKey ? record.key() : record.value();
      try
      {
        newSFContent = new SnowflakeRecordContent(schema, content);
      } catch (Exception e)
      {
        logError("Native content parser error:\n{}", e.getMessage());
        try {
          // try to serialize this object and send that as broken record
          ByteArrayOutputStream out = new ByteArrayOutputStream();
          ObjectOutputStream os = new ObjectOutputStream(out);
          os.writeObject(content);
          newSFContent = new SnowflakeRecordContent(out.toByteArray());
        } catch (Exception serializeError)
        {
          logError("Failed to convert broken native record to byte data:\n{}", serializeError.getMessage());
          throw e;
        }
      }
      // create new sinkRecord
      Schema keySchema = isKey ? new SnowflakeJsonSchema() : record.keySchema();
      Object keyContent = isKey ? newSFContent : record.key();
      Schema valueSchema = isKey ? record.valueSchema() : new SnowflakeJsonSchema();
      Object valueContent = isKey ? record.value() : newSFContent;
      return new SinkRecord(record.topic(), record.kafkaPartition(), keySchema, keyContent,
        valueSchema, valueContent, record.kafkaOffset(), record.timestamp(), record.timestampType(), record.headers());
    }

    private boolean shouldFlush()
    {
      return (System.currentTimeMillis() - this.previousFlushTimeStamp) >= (getFlushTime() * 1000);
    }

    private void flushBuffer()
    {
      // Just checking buffer size, no atomic operation required
      if (buffer.isEmpty())
      {
        return;
      }
      PartitionBuffer tmpBuff;
      bufferLock.lock();
      try
      {
        tmpBuff = buffer;
        this.buffer = new PartitionBuffer();
      } finally
      {
        bufferLock.unlock();
      }
      flush(tmpBuff);
    }

    private void writeBrokenDataToTableStage(SinkRecord record)
    {
      SnowflakeRecordContent key = (SnowflakeRecordContent) record.key();
      SnowflakeRecordContent value = (SnowflakeRecordContent) record.value();
      if (key != null)
      {
        String fileName = FileNameUtils.brokenRecordFileName(prefix, record.kafkaOffset(), true);
        conn.putToTableStage(tableName, fileName, snowflakeContentToByteArray(key));
      }
      if (value != null)
      {
        String fileName = FileNameUtils.brokenRecordFileName(prefix, record.kafkaOffset(), false);
        conn.putToTableStage(tableName, fileName, snowflakeContentToByteArray(value));
      }
    }

    private byte[] snowflakeContentToByteArray(SnowflakeRecordContent content)
    {
      if (content == null)
      {
        return null;
      }
      if (content.isBroken())
      {
        return content.getBrokenData();
      }
      return Arrays.asList(content.getData()).toString().getBytes();
    }

    private long getOffset()
    {
      if (fileNames.isEmpty())
      {
        return committedOffset.get();
      }

      List<String> fileNamesCopy = new ArrayList<>();
      fileListLock.lock();
      try
      {
        fileNamesCopy.addAll(fileNames);
        fileNames = new LinkedList<>();
      } finally
      {
        fileListLock.unlock();
      }
      // This api should throw exception if backoff failed
      ingestionService.ingestFiles(fileNamesCopy);
      committedOffset.set(flushedOffset.get());
      return committedOffset.get();
    }

    private void flush(final PartitionBuffer buff)
    {
      if (buff == null || buff.isEmpty())
      {
        return;
      }
      this.previousFlushTimeStamp = System.currentTimeMillis();

      // If we failed to submit/put, throw an runtime exception that kills the connector.
      // SnowflakeThreadPoolUtils.flusherThreadPool.submit(
      String fileName = FileNameUtils.fileName(prefix, buff.getFirstOffset(),
                                               buff.getLastOffset());
      String content = buff.getData();
      conn.put(stageName, fileName, content);

      flushedOffset.set(Math.max(buff.getLastOffset() + 1, flushedOffset.get()));
      fileListLock.lock();
      try
      {
        fileNames.add(fileName);
        cleanerFileNames.add(fileName);
      } finally
      {
        fileListLock.unlock();
      }

      logInfo("pipe {}, flush pipe: {}", pipeName, fileName);
    }

    private void checkStatus()
    {
      List<String> tmpFileNames;

      fileListLock.lock();
      try
      {
        tmpFileNames = cleanerFileNames;
        cleanerFileNames = new LinkedList<>();
      } finally
      {
        fileListLock.unlock();
      }

      long currentTime = System.currentTimeMillis();
      List<String> loadedFiles = new LinkedList<>();
      List<String> failedFiles = new LinkedList<>();

      //ingest report
      filterResult(ingestionService.readIngestReport(tmpFileNames),
        tmpFileNames,
        loadedFiles, failedFiles);

      //old files
      List<String> oldFiles = new LinkedList<>();

      // iterate over a copy since failed files get removed from it
      new LinkedList<>(tmpFileNames).forEach(
        name ->
        {
          long time = FileNameUtils.fileNameToTimeIngested(name);
          if (time < currentTime - ONE_HOUR)
          {
            failedFiles.add(name);
            tmpFileNames.remove(name);
          }
          else if (time < currentTime - TEN_MINUTES)
          {
            oldFiles.add(name);
          }
        }
      );
      //load history
      if (!oldFiles.isEmpty())
      {
        filterResult(ingestionService.readOneHourHistory(tmpFileNames,
          currentTime - ONE_HOUR), tmpFileNames, loadedFiles, failedFiles);
      }

      purge(loadedFiles);
      moveToTableStage(failedFiles);

      fileListLock.lock();
      try
      {
        cleanerFileNames.addAll(tmpFileNames);
      } finally
      {
        fileListLock.unlock();
      }
    }

    private void filterResult(Map<String, InternalUtils.IngestedFileStatus> fileStatus,
                              List<String> allFiles,
                              List<String> loadedFiles,
                              List<String> failedFiles)
    {
      fileStatus.forEach(
        (name, status) ->
        {
          switch (status)
          {
            case LOADED:
              loadedFiles.add(name);
              allFiles.remove(name);
              break;
            case FAILED:
            case PARTIALLY_LOADED:
              failedFiles.add(name);
              allFiles.remove(name);
              break;
            default:
              //otherwise, do nothing
          }
        }
      );
    }

    private void purge(List<String> files)
    {
      if (!files.isEmpty())
      {
        conn.purgeStage(stageName, files);
      }
    }

    private void moveToTableStage(List<String> files)
    {
      if (!files.isEmpty())
      {
        conn.moveToTableStage(tableName, stageName, files);
        telemetryService.reportKafkaFileFailure(tableName, stageName, files);
      }
    }

    private void recover()
    {
      if (conn.pipeExist(pipeName))
      {
        if (!conn.isPipeCompatible(tableName, stageName, pipeName))
        {
          throw SnowflakeErrors.ERROR_5005.getException("pipe name: " + pipeName,
            conn.getTelemetryClient());
        }
        logInfo("pipe {}, recovered from existing pipe", pipeName);
      }
      else
      {
        conn.createPipe(tableName, stageName, pipeName);
      }
    }

    private void close()
    {
      try
      {
        stopCleaner();
      } catch (Exception e)
      {
        logWarn("Failed to terminate Cleaner or Flusher");
      }
      ingestionService.close();
      sendUsageReport();
      logInfo("pipe {}: service closed", pipeName);
    }

    /**
     * SinkConnector ans SinkTasks start at the same time, however, SinkTasks
     * need create table and wait SinkConnector to create stage.
     * This method checks table and stage existence for at most 120 times(10
     * min)
     * And then throws exceptions if table or stage doesn't exit
     */
    private void createTableAndStage()
    {
      //create table if not exists
      if (conn.tableExist(tableName))
      {
        if (conn.isTableCompatible(tableName))
        {
          logInfo("Using existing table {}.", tableName);
          telemetryService.reportKafkaReuseTable(tableName);
        }
        else
        {
          throw SnowflakeErrors.ERROR_5003.getException("table name: " + tableName, telemetryService);
        }
      }
      else
      {
        logInfo("Creating new table {}.", tableName);
        conn.createTable(tableName);
      }

      if (conn.stageExist(stageName))
      {
        if (conn.isStageCompatible(stageName))
        {
          logInfo("Using existing stage {}.", stageName);
          telemetryService.reportKafkaReuseStage(stageName);
        }
        else
        {
          throw SnowflakeErrors.ERROR_5004.getException("stage name: " + stageName, telemetryService);
        }
      }
      else
      {
        logInfo("Creating new stage {}.", stageName);
        conn.createStage(stageName);
      }
    }

    private void sendUsageReport()
    {
      usageDataLock.lock();
      long numOfRecord;
      long sizeOfData;
      long start;
      long end;
      try
      {
        numOfRecord = totalNumberOfRecord;
        this.totalNumberOfRecord = 0;
        sizeOfData = totalSizeOfData;
        this.totalSizeOfData = 0;
        start = this.startTime;
        this.startTime = System.currentTimeMillis();
        end = this.startTime;
      } finally
      {
        usageDataLock.unlock();
      }
      telemetryService.reportKafkaUsage(start, end, numOfRecord, sizeOfData);
    }

    private void updateUsageData(long numOfRecord, long sizeOfData)
    {
      usageDataLock.lock();
      try
      {
        this.totalSizeOfData += sizeOfData;
        this.totalNumberOfRecord += numOfRecord;
      } finally
      {
        usageDataLock.unlock();
      }

    }

    private class PartitionBuffer
    {
      private final StringBuilder stringBuilder;
      private int numOfRecord;
      private int bufferSize;
      private long firstOffset;
      private long lastOffset;

      private int getNumOfRecord()
      {
        return numOfRecord;
      }

      private int getBufferSize()
      {
        return bufferSize;
      }

      private long getFirstOffset()
      {
        return firstOffset;
      }

      private long getLastOffset()
      {
        return lastOffset;
      }

      private PartitionBuffer()
      {
        stringBuilder = new StringBuilder();
        numOfRecord = 0;
        bufferSize = 0;
        firstOffset = -1;
        lastOffset = -1;
      }

      private void insert(SinkRecord record)
      {
        String data = recordService.processRecord(record);
        if (bufferSize == 0)
        {
          firstOffset = record.kafkaOffset();
        }

        stringBuilder.append(data);
        numOfRecord++;
        bufferSize += data.length() * 2; //1 char = 2 bytes
        lastOffset = record.kafkaOffset();
      }

      private boolean isEmpty()
      {
        return numOfRecord == 0;
      }

      private String getData()
      {
        String result = stringBuilder.toString();
        logDebug("flush buffer: {} records, {} bytes, offset {} - {}",
          numOfRecord, bufferSize, firstOffset, lastOffset);
        updateUsageData(this.numOfRecord, this.bufferSize);
        return result;
      }

    }

  }

}
