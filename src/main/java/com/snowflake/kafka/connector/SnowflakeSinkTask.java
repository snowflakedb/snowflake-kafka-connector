/*
 * Copyright (c) 2019 Snowflake Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.snowflake.kafka.connector;

import com.snowflake.kafka.connector.internal.Logging;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import com.snowflake.kafka.connector.internal.SnowflakeIngestService;
import com.snowflake.kafka.connector.internal.SnowflakeJDBCWrapper;
import com.snowflake.kafka.connector.internal.SnowflakeTelemetry;
import com.snowflake.kafka.connector.records.RecordService;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * SnowflakeSinkTask implements SinkTask for Kafka Connect framework.
 * expects configuration from SnowflakeSinkConnector
 * creates snowflake snowpipes for each partition assigned to this sink task
 * instance,
 * takes records loaded from those Kafka partitions,
 * creates files to be ingested to Snowflake,
 * ingests them through snowpipe (through a SnowflakeJDBCWrapper instance)
 */
public class SnowflakeSinkTask extends SinkTask
{
  private Map<String, String> config; // connector configuration, provided by
  // user through kafka connect framework
  private String connectorName;       // unique name of this connector instance
  private RecordService recordService;            // service to validate
  // record structure and append metadata

  private HashMap<TopicPartition, PartitionProperties> partitionProperties;

  private long bufferCountRecords;    // config buffer.count.records -- how
  // many records to buffer
  private long bufferSizeBytes;       // config buffer.size.bytes --
  // aggregate size in bytes of all records to buffer


  // SnowflakeJDBCWrapper provides methods to interact with user's snowflake
  // account
  // and execute queries
  private SnowflakeJDBCWrapper snowflakeConnection;

  // Snowflake Telemetry provides methods to report usage statistics
  private SnowflakeTelemetry telemetry;
  private long tStartTime;
  private long tNumRecords;
  private long tNumBytes;
  private long tReportingIntervalms;   // how often to report the data
  // processing telemetry

  private static final Logger LOGGER = LoggerFactory
    .getLogger(SnowflakeSinkTask.class);

  /**
   * default constructor, invoked by kafka connect framework
   */
  public SnowflakeSinkTask()
  {
    // instantiate in-memory structures
    partitionProperties = new HashMap<>();
  }

  /**
   * start method handles configuration parsing and one-time setup of the
   * task.
   * loads configuration
   *
   * @param parsedConfig - has the configuration settings
   */
  @Override
  public void start(final Map<String, String> parsedConfig)
  {
    LOGGER.info(Logging.logMessage("SnowflakeSinkTask:start"));

    this.config = parsedConfig;

    //enable jvm proxy
    Utils.enableJVMProxy(config);

    connectorName = config.get("name");

    recordService = new RecordService();    // default : include all

    this.bufferCountRecords = Long.parseLong(config.get
      (SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS));
    this.bufferSizeBytes = Long.parseLong(config.get
      (SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES));

    snowflakeConnection = new SnowflakeJDBCWrapper(config);

    telemetry = snowflakeConnection.getTelemetry();

    //todo: add to Config
    tReportingIntervalms = (config.containsKey("reserved.snowflake.telemetry" +
      ".reporting.interval.ms")) ?
      Long.parseLong(config.get("reserved.snowflake.telemetry.reporting" +
        ".interval.ms")) : 60 * 60 * 1000;
  }

  /**
   * stop method is invoked only once outstanding calls to other methods
   * have completed.
   * e.g. after current put, and a final preCommit has completed.
   */
  @Override
  public void stop()
  {
    LOGGER.info(Logging.logMessage("SnowflakeSinkTask:stop"));

  }

  /**
   * open method creates snowpipes for each partition assigned to this sink
   * task instance, and rebuilds in memory structures.
   * kafka connect calls this method right after task initialization
   * or for the purpose of partition rebalance
   * ASSUMPTION : There is no need to manage existing this.partitions set
   * in open method. kafka connect framework sends a complete list of
   * partitions
   * that this sink task is expected to process now onwards.
   *
   * @param partitions - The list of all partitions that are now assigned to
   *                   the task
   */
  @Override
  public void open(final Collection<TopicPartition> partitions)
  {
    LOGGER.info(Logging.logMessage(
      "SnowflakeSinkTask:open, TopicPartitions: {}", partitions
    ));

    //reset in-memory cache
    this.partitionProperties = new HashMap<>();

    partitions.forEach(
      partition -> {
        partitionProperties.put(partition, new PartitionProperties());

        String tableName = config.get(partition.topic());
        String stageName = Utils.stageName(connectorName, tableName);
        String pipeName = Utils.pipeName(connectorName, tableName, partition
          .partition());

        //Check Table and Stage existence
        //Retry 120 times (10 min) to wait SinkConnector creating stage and
        // table
        final int max_retry = 120;
        int retried = 0;
        while ((!snowflakeConnection.tableExist(tableName) ||
          !snowflakeConnection.stageExist(stageName))
          && retried < max_retry)
        {
          retried++;
          try
          {
            LOGGER.info(Logging.logMessage(
              "Sleeping 5 sec to allow setup to complete"
            ));
            Thread.sleep(5000);
          } catch (InterruptedException e)
          {
            LOGGER.error(Logging.logMessage(
              "System error when thread sleep {}", e));// should not happen
          }
        }

        if (!snowflakeConnection.tableExist(tableName) ||
          !snowflakeConnection.stageExist(stageName))
        {
          telemetry.reportKafkaFatalError(SnowflakeErrors
            .ERROR_5008.getDetail(), connectorName);
          this.stop();
          throw SnowflakeErrors.ERROR_5008.getException();
        }


        //recover
        final HashMap<String, SnowflakeFileMetadata> myFilesMap =
          partitionProperties.get(partition).getFileList();
        final SnowflakePartitionBuffer myBuffer =
          partitionProperties.get(partition).getBuffer();

        //check pipe existence
        //if pipe existence recovered from previous task
        //otherwise move all file on stage to table stage
        if (snowflakeConnection.pipeExist(pipeName))
        {
          if (!snowflakeConnection.pipeIsCompatible(pipeName, tableName,
            stageName))
          {
            telemetry.reportKafkaFatalError("incompatible pipe: " + pipeName,
              connectorName);
            throw SnowflakeErrors.ERROR_5005.getException("pipe name: " +
              pipeName);
          }


          //recover
          Map<String, Utils.IngestedFileStatus> files =
            snowflakeConnection.recoverPipe(pipeName, stageName,
              Utils.subdirectoryName(connectorName, tableName, partition
                .partition()));

          List<String> loadedFiles = new LinkedList<>();
          List<String> failedFiles = new LinkedList<>();

          if (!files.isEmpty())
          {
            files.forEach(
              (name, status) -> {
                SnowflakeFileMetadata meta = new SnowflakeFileMetadata(name);
                if (myBuffer.latestOffset() < meta.endOffset)
                {
                  myBuffer.setLatestOffset(meta.endOffset);
                }
                switch (status)
                {
                  case LOADED:
                    loadedFiles.add(name);
                    break;
                  case NOT_FOUND:
                  case LOAD_IN_PROGRESS:
                    myFilesMap.put(name, meta);
                    break;
                  case FAILED:
                  case PARTIALLY_LOADED:
                  default:
                    failedFiles.add(name);
                }

              }
            );

          }
          if (!failedFiles.isEmpty())
          {
            snowflakeConnection.moveToTableStage(stageName, tableName,
              failedFiles);
          }
          if (!loadedFiles.isEmpty())
          {
            snowflakeConnection.purge(stageName, loadedFiles);
          }

          LOGGER.info(Logging.logMessage("Connector recovery: latest " +
            "processed offset {}, for partition {}", myBuffer.latestOffset
            (), partition));

        }
        else
        {
          try
          {
            snowflakeConnection.createPipe(pipeName, tableName, stageName,
              true);
            telemetry.reportKafkaCreatePipe(pipeName, stageName, tableName,
              connectorName);
          } catch (Exception e)
          {
            telemetry.reportKafkaFatalError("Failed to create pipe: " +
              pipeName, connectorName);
            this.stop();
            throw SnowflakeErrors.ERROR_3005.getException(e);
          }
          LOGGER.info(Logging.logMessage("pipe {} does't exist, can't recover" +
            " from previous task, move all file related to this pipe to table" +
            " ({}) stage"), pipeName, tableName);

          //update last offset
          snowflakeConnection.listStage(stageName, Utils
            .subdirectoryName(connectorName, tableName, partition.partition()
            )).forEach(
            name ->
            {
              if (Utils.fileNameToEndOffset(name) > myBuffer.latestOffset())
              {
                myBuffer.setLatestOffset(Utils.fileNameToEndOffset(name));
              }
            }
          );
          snowflakeConnection.moveToTableStage(stageName, tableName, Utils
            .subdirectoryName(connectorName, tableName, partition.partition()));
        }


      }
    );

    resetDataTelemetry();
  }


  /**
   * close method handles the partitions that are no longer assigned to
   * this sink task.
   * kafka connect calls this method before a rebalance operation starts and
   * after the sink tasks stops fetching data
   * We don't drop snowpipe's in close method to avoid churn of snowpipes
   * due to rebalancing.
   * snowpipes are dropped in SinkTask:stop and SinkConnector:stop
   * ASSUMPTION : There is no need to manage this.partitions set in close
   * method.
   * kafka connect framework removes all partitions that this sink task was
   * processing prior to close.
   *
   * @param partitions - The list of all partitions that were assigned to the
   *                   task
   */
  @Override
  public void close(final Collection<TopicPartition> partitions)
  {
    LOGGER.info(Logging.logMessage("SnowflakeSinkTask:close: Unassigning " +
      "TopicPartitions {}", partitions));

    // handle last file on the stage
    partitions.forEach(partition ->
    {
      if (partitionProperties.containsKey(partition))
      {
        String tableName = config.get(partition.topic());
        String pipeName = Utils.pipeName(
          connectorName,
          tableName,
          partition.partition());
        String stageName = Utils.stageName(connectorName, tableName);

        PartitionProperties property = partitionProperties.get(partition);
        if (property.getLastFileName() != null)
        {
          List<String> fileList = new ArrayList<>(1);
          fileList.add(property.getLastFileName());
          if (property.isLastFileLoaded()) //loaded
          {
            snowflakeConnection.purge(stageName, fileList);
            LOGGER.debug(Logging.logMessage("purge last file: {}", fileList
              .get(0)));
          }
          else
          {
            snowflakeConnection.moveToTableStage(stageName, tableName,
              fileList);
            LOGGER.debug(Logging.logMessage("move last file {} to table {} " +
              "stage", fileList.get(0), tableName));
          }
        }

        //drop pipe if stage is empty
        if (snowflakeConnection.listStage(stageName, Utils.subdirectoryName
          (connectorName, tableName, partition.partition())).isEmpty())
        {
          snowflakeConnection.dropPipe(pipeName);
        }
      }
      else
      {
        //todo: report error?
      }
    });

  }

  /**
   * put method takes the collection of records for the partitions assigned
   * to this sink task.
   * Usually this invocation is asynchronous. If this operation fails,
   * the SinkTask may throw a RetriableException to indicate that
   * the framework should attempt to retry the same call again.
   *
   * @param records - collection of records from kafka topic/partitions for
   *                this connector
   */
  @Override
  public void put(final Collection<SinkRecord> records)
  {
    LOGGER.debug(Logging.logMessage("Number of records to process: {}", records
      .size()));

    // process each record by adding to the corresponding partitionBuffers
    for (SinkRecord record : records)
    {
      TopicPartition partition = new TopicPartition(
        record.topic(),
        record.kafkaPartition());
      // NOTE : kafka connect framework uses inconsistent naming to get
      // partition info

      if (partitionProperties.containsKey(partition))
      {
        SnowflakePartitionBuffer buffer = partitionProperties.get(partition)
          .getBuffer();

        // check if this record should be discarded
        // i.e. it may have been resent by Connect framework (expects
        // idempotent behavior),
        // or it may have been sent during a previous incarnation of the
        // connector

        if (record.kafkaOffset() > buffer.latestOffset())
        {
          // LOGGER.debug("Received record {}", record);
          String recordAsString = recordService.processRecord(record);
          buffer.bufferRecord(record, recordAsString);

          // create a file and ingest if we got enough data in the buffer
          if (buffer.recordCount() >= this.bufferCountRecords ||
            buffer.bufferSize() >= this.bufferSizeBytes)
          {
            ingestBufferedRecords(partition);
          }
        }
        else
        {
          LOGGER.info(Logging.logMessage("Skip Offset: {}", record
            .kafkaOffset()));
        }
      }
      else
      {
        // This shouldn't happen. It's a bug.
        telemetry.reportKafkaFatalError("Partition " + partition + " not " +
          "found", connectorName);

        // cleanup
        this.stop();

        throw SnowflakeErrors.ERROR_5009.getException("Partition name: " +
          partition);
      }
    }
  }

  /**
   * ingestBufferedRecords is a utility method that creates a file from
   * currently buffered records
   * for a topicPartition and ingests the file to table
   * Assumptions: the invoking methods are expected to ensure that buffer
   * exists for this topicPartition
   * so, we don't do additional checks in this method
   *
   * @param topicPartition
   */
  private void ingestBufferedRecords(TopicPartition topicPartition)
  {
    SnowflakePartitionBuffer buffer = partitionProperties.get(topicPartition)
      .getBuffer();
    if (buffer.recordCount() == 0)
    {
      return;
    }

    updateDataTelemetry(buffer.recordCount(), buffer.bufferSize());

    // The order should be create file, drop buffer, ingest file, update
    // in-memory file structure
    // It is important to drop the buffer before invoking ingest.
    // This ensures that we don't double ingest any record in a scenario where
    // ingest throws exception due to snowpipe availability issue.

    String tableName = config.get(topicPartition.topic());
    String stageName = Utils.stageName(connectorName, tableName);
    String fileName = Utils.fileName(
      connectorName,
      tableName,
      topicPartition.partition(),
      buffer.firstOffset(),
      buffer.latestOffset());
    String pipeName = Utils.pipeName(
      connectorName,
      tableName,
      topicPartition.partition());

    LOGGER.debug(Logging.logMessage("Ingesting file: {}, for partition: {}",
      fileName, topicPartition));

    // create file
    try
    {
      snowflakeConnection.put(fileName, buffer.bufferAsString(), stageName);
    } catch (Exception e)
    {
      telemetry.reportKafkaFatalError("Failed to write file " + fileName +
        " to stage " + stageName, connectorName);

      // cleanup
      this.stop();

      throw SnowflakeErrors.ERROR_2003.getException(e);
    }

    // drop buffer
    buffer.dropRecords();

    // ingest file
    try
    {
      snowflakeConnection.ingestFile(pipeName, fileName);
    } catch (Exception e)
    {
      telemetry.reportKafkaFatalError("Failed to ingest file: " + fileName +
        " through pipe " + pipeName, connectorName);

      // cleanup
      this.stop();

      throw SnowflakeErrors.ERROR_3001.getException(e);

      // TODO (post GA) : don't throw connect exception but limit the number
      // of files that.
      // are piling in the snowflake internal stage.
    }

    // update in-memory file structure
    partitionProperties.get(topicPartition).getFileList().put(fileName, new
      SnowflakeFileMetadata(fileName));

    LOGGER.debug(Logging.logMessage("Ingested file: {}, for partition: {}",
      fileName, topicPartition));
  }


  /**
   * preCommit method is called by the connect framework to check how far
   * along
   * the offsets are committed
   *
   * @param offsets - the current map of offsets as of the last call to put
   * @return a map of offsets by topic-partition that are safe to commit
   * @throws RetriableException when meet any issue during processing
   */
  @Override
  public Map<TopicPartition, OffsetAndMetadata> preCommit(
    Map<TopicPartition, OffsetAndMetadata> offsets)
    throws RetriableException
  {

    //todo: compare partition in offsets and partitionProperties
    //todo: report error if not match
    LOGGER.debug(Logging.logMessage("offsets: {}", offsets));

    // NOTE: minor performance improvement can be achieved by validating
    // server objects
    // upon a trigger e.g. too many load history scans, etc
    validateServerObjects();    // e.g. SNOW-68472

    // return object, containing offset value for each partition that can be
    // high water mark for
    // purging records from kafka
    Map<TopicPartition, OffsetAndMetadata> committedOffsets = new HashMap<>();

    partitionProperties.forEach((partition, property) ->
      {
        String tableName = config.get(partition.topic());
        // SnowflakeSinkConnector stores it here
        String stageName = Utils.stageName(connectorName, tableName);
        String pipeName = Utils.pipeName(connectorName, tableName, partition
          .partition());

        // ingest buffered records, if any
        ingestBufferedRecords(partition);

        if (property.getFileList().isEmpty())
        {
          LOGGER.debug(Logging.logMessage("No files pending ingestion " +
            "status check for partition: {}", partition));
        }
        else
        {

          HashMap<String, SnowflakeFileMetadata> myFilesMap = property
            .getFileList();
          List<String> myFiles = new ArrayList<>(myFilesMap.keySet());

          myFiles.sort(Comparator.comparingLong(Utils::fileNameToStartOffset));

          LOGGER.debug(Logging.logMessage("Checking status of files: {}, for " +
            "partition: {}", myFiles, partition));

          // # of file larger than 10000, or time of ingestion earlier than
          // 10min
          // todo: offset reuse? what if larger than max value of long
          boolean loadHistoryScan = myFiles.size() > 10000 ||
            System.currentTimeMillis() - Utils.fileNameToTimeIngested(
              myFiles.get(0)) > SnowflakeIngestService.TEN_MINUTES;


          Map<String, Utils.IngestedFileStatus> myFilesStatus;
          try
          {
            if (loadHistoryScan)
            {
              LOGGER.warn(Logging.logMessage("Using Load History Scan, number" +
                " of files {}, oldest file {}.", myFiles.size(), myFiles.get
                (0)));
              myFilesStatus = snowflakeConnection
                .verifyFromLastOnehourLoasHistory(pipeName, myFiles);
            }
            else
            {
              LOGGER.debug(Logging.logMessage("Using Insert Report, number of" +
                " files {}, oldest file {}.", myFiles.size(), myFiles.get(0)));
              myFilesStatus = snowflakeConnection.verifyFromIngestReport
                (pipeName, myFiles);
            }
          } catch (Exception e)
          {
            String errorMsg = "Failed to verify status of files from pipe " +
              pipeName + ". This action will be retried.\nException: " + e
              .toString();
            LOGGER.warn(Logging.logMessage(errorMsg));
            telemetry.reportKafkaNonFatalError(errorMsg, connectorName);
            throw new RetriableException(errorMsg);
          }

          // list of files that are verified as successfully ingested.
          // They will be purged from stage
          List<String> filesSucceeded = new LinkedList<>();

          // list of files that have *any* failures
          // They will be moved to table stage
          List<String> filesFailed = new LinkedList<>();

          // offset while this is true
          long committedOffsetValue = offsets.get(partition).offset();
          String latestFileName = null;
          boolean isLatestFileSuccessful = false;

          for (String fileName : myFiles)
          {
            Utils.IngestedFileStatus fileStatus = myFilesStatus.get(fileName);

            boolean isNotProcessed = false;
            switch (fileStatus)
            {
              case LOADED:
                filesSucceeded.add(fileName);
                LOGGER.debug(Logging.logMessage("File {}, partition {}: " +
                  "successfully loaded. File will be purged from internal " +
                  "stage.", fileName, partition));
                myFilesMap.remove(fileName);
                latestFileName = fileName;
                isLatestFileSuccessful = true;
                break;
              case PARTIALLY_LOADED:
                filesFailed.add(fileName);
                LOGGER.error(Logging.logMessage("File {}, partition {}: some " +
                  "records failed to load. File is being moved to table stage" +
                  " for further investigation.", fileName, partition));
                myFilesMap.remove(fileName);
                latestFileName = fileName;
                isLatestFileSuccessful = false;
                break;
              case FAILED:
                filesFailed.add(fileName);
                LOGGER.error("File {}, partition {}: file failed to load. " +
                  "File is being moved to table stage for further " +
                  "investigation.", fileName, partition);
                myFilesMap.remove(fileName);
                latestFileName = fileName;
                isLatestFileSuccessful = false;
                break;
              case NOT_FOUND:
                if (System.currentTimeMillis() - Utils.fileNameToTimeIngested
                  (fileName) > SnowflakeIngestService.ONE_HOUR)
                {
                  filesFailed.add(fileName);
                  LOGGER.error(Logging.logMessage("File {}, partition {}: " +
                    "file " +
                    "too old. History is not available to check whether it " +
                    "was " +
                    "successfully loaded. File is being moved to table stage " +
                    "for further investigation.", fileName, partition));
                  myFilesMap.remove(fileName);
                  latestFileName = fileName;
                  isLatestFileSuccessful = false;
                  break;
                }
              case LOAD_IN_PROGRESS:
              default:
                // stop updating the committed offset for now until the next
                // call to preCommit, but continue the file traversal to
                // purge the files which are'DONE'
                isNotProcessed = true;

            }

            if (isNotProcessed)
            {
              //if meet any not processed file then break.
              //check the remaining file in next preCommit function call
              break;
            }
          }


          if (latestFileName != null) // if any update
          {
            //remove file name from file lists
            if (isLatestFileSuccessful)
            {
              filesSucceeded.remove(filesSucceeded.size() - 1);
            }
            else
            {
              filesFailed.remove(filesFailed.size() - 1);
            }

            if (property.getLastFileName() != null)
            {
              // add last file to file lists
              if (property.isLastFileLoaded())
              {
                filesSucceeded.add(property.getLastFileName());
              }
              else
              {
                filesFailed.add(property.getLastFileName());
              }
            }
            //update file list
            property.setLastFileName(latestFileName, isLatestFileSuccessful);

            //keep last file on stage
            LOGGER.info(Logging.logMessage("File {} is the latest file and is" +
              " in a finalized state. Leaving it in the stage for future " +
              "recovery.", latestFileName, partition));

            //update offset
            committedOffsetValue = Utils.fileNameToEndOffset(latestFileName)
              + 1;
          }

          // purge the successful files
          try
          {
            snowflakeConnection.purge(stageName, filesSucceeded);
          } catch (Exception e)
          {
            String errorMsg = "Failed to purge files from stage " + stageName +
              ". This action will be retried.\nException: " + e.toString();
            LOGGER.warn(Logging.logMessage(errorMsg));
            telemetry.reportKafkaNonFatalError(errorMsg, connectorName);

            throw new RetriableException(errorMsg);
          }

          // move the failed files
          try
          {
            snowflakeConnection.moveToTableStage(stageName, tableName,
              filesFailed);
          } catch (Exception e)
          {
            String errorMsg = "Failed to move files from internal stage " +
              stageName + " to table stage for table " + tableName +
              ". This action will be retried.\nException: " + e.toString();
            LOGGER.warn(Logging.logMessage(errorMsg));
            telemetry.reportKafkaNonFatalError(errorMsg, connectorName);

            throw new RetriableException(errorMsg);
          }

          // update committed offset in the in-memory structure and output
          partitionProperties.get(partition).getBuffer().setCommittedOffset
            (committedOffsetValue);
          committedOffsets.put(partition, new OffsetAndMetadata
            (committedOffsetValue));
        }
      }
    );

    return committedOffsets;
  }

  /**
   * validateServerObjects checks for existence of server objects.
   * Currently, it only checks for existence of tables to avoid SNOW-68472.
   * We don't yet have reasons to check for existence of stage and pipes.
   */
  private void validateServerObjects()
  {
    List<String> topics = new ArrayList<>(Arrays.asList(config.get("topics")
      .split(",")));
    for (String topic : topics)
    {
      String table = config.get(topic);
      if (!snowflakeConnection.tableIsCompatible(table))  // checks for
      // existence and compatibility
      {
        telemetry.reportKafkaFatalError("Table is incompatible: " + table,
          connectorName);

        // cleanup
        stop();

        throw SnowflakeErrors.ERROR_5003.getException("Table name: " + table);
      }

    }
  }

  /**
   * telemetry helper function
   */
  private void resetDataTelemetry()
  {
    tStartTime = System.currentTimeMillis();
    tNumRecords = 0;
    tNumBytes = 0;
  }

  /**
   * telemetry helper function
   */
  private void updateDataTelemetry(long numRecords, long numBytes)
  {
    tNumRecords += numRecords;
    tNumBytes += numBytes;

    long currentTime = System.currentTimeMillis();
    if ((currentTime - tStartTime) > tReportingIntervalms)
    {
      telemetry.reportKafkaUsage(tStartTime, currentTime, tNumRecords,
        tNumBytes, connectorName);
      resetDataTelemetry();
    }
  }

  /**
   * @return connector version
   */
  @Override
  public String version()
  {
    return Utils.VERSION;
  }
}

/**
 * SnowflakeFileMetadata keeps track of the life cycle of a file
 */
class SnowflakeFileMetadata
{
  String fileName;
  long startOffset;
  long endOffset;
  long timeIngested;  // timestamp when file was sent to snowpipe

  SnowflakeFileMetadata(String fileName)
  {
    this.fileName = fileName;
    this.startOffset = Utils.fileNameToStartOffset(fileName);
    this.endOffset = Utils.fileNameToEndOffset(fileName);
    this.timeIngested = Utils.fileNameToTimeIngested(fileName);
  }

}

/**
 * SnowflakePartitionBuffer caches all the sink records received so far
 * from kafka connect for a partition, until they are ready to be written to
 * a file
 * and ingested through snowpipe.
 * It also tracks the start offset, end offset, and estimated file size.
 */
class SnowflakePartitionBuffer
{
  private StringBuilder buffer;                  // all records serialized as
  // String, with or without metadata
  private int recordCount;                // number of records current in the
  // buffer
  private int bufferSize;                 // cumulative size of records in
  // the buffer
  private long firstOffset;               // offset of the first record in
  // the buffer
  private long firstOffsetTime;           // buffer start time

  // offset of the latest record that was "processed" on this partition
  // in previous incarnation or current incarnation of the connector
  // This is set during connector recovery and updated as we process new records
  private long latestOffset;

  // offset of the latest record that was "committed" on this partition
  // in previous incarnation or current incarnation of the connector
  // This is set by preCommit
  private long committedOffset;


  SnowflakePartitionBuffer()
  {
    buffer = new StringBuilder();
    recordCount = 0;
    bufferSize = 0;
    firstOffset = -1;
    firstOffsetTime = 0;
    latestOffset = -1;
    committedOffset = -1;
  }

  void bufferRecord(SinkRecord record, String recordAsString)
  {
    // initialize if this is the first record entering buffer
    if (bufferSize == 0)
    {
      firstOffset = record.kafkaOffset();
      firstOffsetTime = System.currentTimeMillis();
    }

    buffer.append(recordAsString);
    recordCount++;
    bufferSize += recordAsString.length();
    latestOffset = record.kafkaOffset();
  }

  void dropRecords()
  {
    buffer = new StringBuilder();
    recordCount = 0;
    bufferSize = 0;
    firstOffset = -1;
    firstOffsetTime = 0;

    // NOTE: don't touch latestOffset and committedOffset as those have a
    // forever life cycle
  }

  String bufferAsString()
  {
    return buffer.toString();
  }

  int recordCount()
  {
    return recordCount;
  }

  long bufferSize()
  {
    return bufferSize;
  }

  long firstOffset()
  {
    return firstOffset;
  }

  long getFirstOffsetTime()
  {
    return firstOffsetTime;
  }

  long latestOffset()
  {
    return this.latestOffset;
  }

  long committedOffset()
  {
    return this.committedOffset;
  }

  // this method is called during connector recovery and
  // when we see a malformed json record
  void setLatestOffset(long offset)
  {
    this.latestOffset = offset;
  }

  // this method is only called from preCommit
  void setCommittedOffset(long offset)
  {
    this.committedOffset = offset;
  }
}

/**
 * Partition Properties
 */
class PartitionProperties
{
  private final SnowflakePartitionBuffer buffer;
  private String lastFileName;
  private boolean isLastFileSuccessful;
  private final HashMap<String, SnowflakeFileMetadata> fileList;

  PartitionProperties()
  {
    lastFileName = null;
    fileList = new HashMap<>();
    buffer = new SnowflakePartitionBuffer();
    isLastFileSuccessful = false;
  }

  void setLastFileName(String name, boolean successful)
  {
    lastFileName = name;
    isLastFileSuccessful = successful;
  }

  boolean isLastFileLoaded()
  {
    return isLastFileSuccessful;
  }

  String getLastFileName()
  {
    return lastFileName;
  }

  SnowflakePartitionBuffer getBuffer()
  {
    return buffer;
  }

  HashMap<String, SnowflakeFileMetadata> getFileList()
  {
    return fileList;
  }
}