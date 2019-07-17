package com.snowflake.kafka.connector.internal;

import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.records.RecordService;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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
  private boolean stopped;

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
    stopped = false;
  }

  @Override
  public void startTask(final String tableName, final String topic, final int partition)
  {
    String stageName = Utils.stageName(conn.getConnectorName(), tableName);
    String nameIndex = topic + "_" + partition;
    if (pipes.containsKey(nameIndex))
    {
      logError("task is already registered, name: {}", nameIndex);
    }
    else
    {
      String pipeName = Utils.pipeName(conn.getConnectorName(), tableName, partition);

      pipes.put(nameIndex, new ServiceContext(tableName, stageName, pipeName,
        conn, partition));
    }

  }

  @Override
  public void insert(final SinkRecord record)
  {
    String nameIndex = record.topic() + "_" + record.kafkaPartition();
    pipes.get(nameIndex).insert(record);
  }

  @Override
  public long getOffset(final TopicPartition topicPartition)
  {
    return pipes.get(topicPartition.topic() + "_" + topicPartition.partition()).getOffset();
  }

  @Override
  public void close()
  {
    this.stopped = true; // release all cleaner and flusher threads
    pipes.forEach(
      (name, context) -> context.close()
    );
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

  private class ServiceContext
  {
    private final String tableName;
    private final String stageName;
    private final String pipeName;
    private final SnowflakeConnectionService conn;
    private final SnowflakeIngestionService ingestionService;
    private List<String> fileNames;
    private PartitionBuffer buffer;
    private final String prefix;
    private long committedOffset; // loaded offset + 1
    private long processedOffset; // processed offset

    //threads
    private final ExecutorService cleanerExecutor;
    private final ExecutorService flusherExecutor;
    private final Lock bufferLock;
    private final Lock fileListLock;


    private ServiceContext(String tableName, String stageName,
                           String pipeName, SnowflakeConnectionService conn,
                           int partition)
    {
      this.pipeName = pipeName;
      this.tableName = tableName;
      this.stageName = stageName;
      this.conn = conn;
      this.fileNames = new LinkedList<>();
      this.buffer = new PartitionBuffer();
      this.ingestionService = conn.buildIngestService(stageName, pipeName);
      this.prefix = FileNameUtils.filePrefix(conn.getConnectorName(),
        tableName, partition);
      this.processedOffset = -1;
      this.committedOffset = 0;
      this.bufferLock = new ReentrantLock();
      this.fileListLock = new ReentrantLock();
      //wait sinkConnector start
      waitTableAndStage();
      recover();

      cleanerExecutor = Executors.newSingleThreadExecutor();
      flusherExecutor = Executors.newSingleThreadExecutor();

      startCleaner();
      startFlusher();

      logInfo("pipe: {} - service started", pipeName);
    }

    private void startCleaner()
    {
      logInfo("pipe {}: cleaner started", pipeName);
      cleanerExecutor.submit(
        () ->
        {
          while (!stopped)
          {
            try
            {
              Thread.sleep(CLEAN_TIME);
              checkStatus();
            } catch (InterruptedException e)
            {
              logError("cleaner error:\n{}", e.getMessage());
            }
          }
        }
      );
    }

    private void stopCleaner()
    {
      try
      {
        cleanerExecutor.awaitTermination(1, TimeUnit.MINUTES);
        logInfo("pipe {}: cleaner terminated", pipeName);
      } catch (InterruptedException e)
      {
        logError("pipe {} : cleaner termination timeout", pipeName);
      }
    }

    private void startFlusher()
    {
      logInfo("pipe {}: flusher started", pipeName);

      flusherExecutor.submit(
        () ->
        {
          while (!stopped)
          {
            try
            {
              Thread.sleep(getFlushTime() * 1000);

              bufferLock.lock();

              PartitionBuffer tmpBuff = buffer;
              buffer = new PartitionBuffer();

              bufferLock.unlock();

              flush(tmpBuff);
            } catch (InterruptedException e)
            {
              logError("flusher error:\n{}", e.getMessage());
            }
          }
        }
      );
    }

    private void stopFlusher()
    {
      try
      {
        flusherExecutor.awaitTermination(1, TimeUnit.MINUTES);
        logInfo("pipe {}: flusher terminated", pipeName);
      } catch (InterruptedException e)
      {
        logError("pipe {} : flusher termination timeout", pipeName);
      }
    }

    private void insert(final SinkRecord record)
    {

      //ignore ingested files
      if (record.kafkaOffset() > processedOffset)
      {
        PartitionBuffer tmpBuff = null;

        bufferLock.lock();

        processedOffset = record.kafkaOffset();
        buffer.insert(record);
        if (buffer.getBufferSize() >= getFileSize() ||
          (getRecordNumber() != 0 && buffer.getNumOfRecord() >= getRecordNumber()))
        {
          tmpBuff = buffer;
          this.buffer = new PartitionBuffer();
        }

        bufferLock.unlock();

        flush(tmpBuff);
      }

    }

    private long getOffset()
    {
      return committedOffset;
    }

    private void flush(PartitionBuffer buff)
    {
      if(buff == null || buff.isEmpty()) return;


      String fileName = FileNameUtils.fileName(prefix, buff.getFirstOffset(),
        buff.getLastOffset());
      String content = buff.getData();
      conn.put(stageName, fileName, content);
      ingestionService.ingestFile(fileName);

      fileListLock.lock();

      fileNames.add(fileName);

      fileListLock.unlock();

      logInfo("pipe {}, flush pipe: {}", pipeName, fileName);
    }

    private void checkStatus()
    {
      fileListLock.lock();

      List<String> tmpFileNames = fileNames;
      fileNames = new LinkedList<>();

      fileListLock.unlock();

      long currentTime = System.currentTimeMillis();
      List<String> loadedFiles = new LinkedList<>();
      List<String> failedFiles = new LinkedList<>();

      //ingest report
      filterResult(ingestionService.readIngestReport(tmpFileNames), tmpFileNames,
        loadedFiles, failedFiles);

      //old files
      List<String> oldFiles = new LinkedList<>();
      tmpFileNames.forEach(
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

      updateOffset(tmpFileNames, loadedFiles, failedFiles);
      purge(loadedFiles);
      moveToTableStage(failedFiles);

      fileListLock.lock();

      fileNames.addAll(tmpFileNames);

      fileListLock.unlock();
    }

    private void updateOffset(List<String> allFiles,
                              List<String> loadedFiles,
                              List<String> failedFiles)
    {
      if (allFiles.isEmpty())
      {
        if(loadedFiles.isEmpty() && failedFiles.isEmpty()) {
          return;
        }
        long result = 0;
        for (String name : loadedFiles)
        {
          long endOffset = FileNameUtils.fileNameToEndOffset(name) + 1;
          if (endOffset > result)
          {
            result = endOffset;
          }
        }
        for (String name : failedFiles)
        {
          long endOffset = FileNameUtils.fileNameToEndOffset(name) + 1;
          if (endOffset > result)
          {
            result = endOffset;
          }
        }
        committedOffset = result;
      }
      else
      {
        long result = Long.MAX_VALUE;
        for (String name : allFiles)
        {
          long startOffset = FileNameUtils.fileNameToStartOffset(name);
          if (startOffset < result)
          {
            result = startOffset;
          }
        }
        committedOffset = result;
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
      conn.purgeStage(stageName, files);
    }

    private void moveToTableStage(List<String> files)
    {
      conn.moveToTableStage(tableName, stageName, files);
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

        fileListLock.lock();

        recoverFileStatues().forEach(
          (name, status) -> fileNames.add(name)
        );

        fileListLock.unlock();

        logInfo("pipe {}, recovered from existing pipe", pipeName);
      }
      else
      {
        conn.createPipe(tableName, stageName, pipeName);
      }
    }

    private Map<String, InternalUtils.IngestedFileStatus> recoverFileStatues()
    {
      List<String> files = conn.listStage(stageName, prefix);
      if (files.isEmpty())
      {
        return new HashMap<>(); //no file on stage
      }
      Map<String, InternalUtils.IngestedFileStatus> result = new HashMap<>();

      List<String> loadedFiles = new LinkedList<>();
      List<String> failedFiles = new LinkedList<>();

      //sort by time
      //may be an issue when continuously recovering
      // because this time is time when file uploaded.
      // if files ingested again, this time will not be
      // updated. So the real ingestion time maybe different
      // in the second time recovery.
      files.sort(Comparator.comparingLong(FileNameUtils::fileNameToTimeIngested));

      long startTime = FileNameUtils.fileNameToTimeIngested(files.get(0));

      committedOffset = Long.MAX_VALUE;
      processedOffset = -1;

      ingestionService.readOneHourHistory(files, startTime).forEach(
        (name, status) ->
        {
          long startOffset = FileNameUtils.fileNameToStartOffset(name);
          long endOffset = FileNameUtils.fileNameToEndOffset(name);
          if (processedOffset < endOffset)
          {
            processedOffset = endOffset;
          }
          switch (status)
          {
            case NOT_FOUND:
              //re ingest
              ingestionService.ingestFile(name);
              result.put(name, status);
              if (committedOffset > startOffset)
              {
                committedOffset = startOffset;
              }
              break;
            case LOAD_IN_PROGRESS:
              result.put(name, status);
              if (committedOffset > startOffset)
              {
                committedOffset = startOffset;
              }
              break;
            case LOADED:
              loadedFiles.add(name);
              break;
            default:
              failedFiles.add(name);
          }
        }
      );
      if (!loadedFiles.isEmpty())
      {
        purge(loadedFiles);
      }
      if (!failedFiles.isEmpty())
      {
        moveToTableStage(failedFiles);
      }
      logInfo("pipe {} : Recovered {} files", pipeName, files.size());
      return result;
    }

    private void close()
    {
      stopCleaner();
      stopFlusher();
      ingestionService.close();
      if (conn.listStage(stageName, prefix).isEmpty()) // keep pipe if stage not empty
      {
        conn.dropPipe(pipeName);
      }
      logInfo("pipe {}: service closed", pipeName);
    }

    /**
     * SinkConnector ans SinkTasks start at the same time, however, SinkTasks
     * need wait until SinkConnector created tables and stages.
     * This method checks table and stage existence for at most 120 times(10
     * min)
     * And then throws exceptions if table or stage doesn't exit
     */
    private void waitTableAndStage()
    {
      final int maxRetry = 120;
      final long sleepTime = 5 * 1000; //5 sec

      for (int i = 0; i < maxRetry; i++)
      {
        if (conn.tableExist(tableName) && conn.stageExist(stageName))
        {
          return;
        }
        try
        {
          logInfo("sleep 5sec to allow setup to complete");
          Thread.sleep(sleepTime);
        } catch (InterruptedException e)
        {
          logError("System error when thread sleep\n{}", e);
        }
      }
      //track this issue, may need more time to start
      throw SnowflakeErrors.ERROR_5008.getException(conn.getTelemetryClient());
    }

    private class PartitionBuffer
    {
      private StringBuilder stringBuilder;
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
        return result;
      }

    }

  }

}
