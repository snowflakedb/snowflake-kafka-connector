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

import com.snowflake.kafka.connector.records.RecordService;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
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

  // Collection of current topic / partitions managed by this task instance
  private Collection<TopicPartition> partitions;

  // Map of current topic / partitions and the file buffer that holds the
  // sink records
  // as they arrive, until they are ready to be written to a file to ingest
  private Map<TopicPartition, SnowflakePartitionBuffer> partitionBuffers;
  private long bufferCountRecords;    // config buffer.count.records -- how
  // many records to buffer
  private long bufferSizeBytes;       // config buffer.size.bytes --
  // aggregate size in bytes of all records to buffer

  // Map of current topic / partitions and the list of files + file metadata,
  // capturing the file lifecycle through out the ingestion process.
  private Map<TopicPartition, HashMap<String, SnowflakeFileMetadata>>
    partitionFiles;

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
    partitions = new HashSet<>();
    partitionBuffers = new HashMap<>();
    partitionFiles = new HashMap<>();
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
    LOGGER.info("SnowflakeSinkTask:start");

    this.config = parsedConfig;

    connectorName = config.get("name");
    if (!connectorName.equalsIgnoreCase(Utils.getAppName()))
    {
      Utils.setAppName(connectorName);
      // need to do this again because this SinkTask maybe on a different
      // worker node than
      // the node where the SnowflakeSinkConnector was initialized
    }

    recordService = new RecordService();    // default : include all

    this.bufferCountRecords = Long.parseLong(config.get("buffer.count" +
      ".records"));
    this.bufferSizeBytes = Long.parseLong(config.get("buffer.size.bytes"));

    try
    {
      snowflakeConnection = new SnowflakeJDBCWrapper(config);
    } catch (SQLException ex)
    {
      // NOTE: this error is unlikely to happen since SnowflakeSinkConnector
      // has already
      // validated the configuration and established a connection. Anyways...
      String errorMsg = "Failed to connect to Snowflake with the provided " +
        "configuration. " +
        "Please see the documentation. Exception: " + ex.toString();
      LOGGER.error(errorMsg);

      // stop the connector as we are unlikely to automatically recover from
      // this error
      throw new ConnectException(errorMsg);
    }

    telemetry = snowflakeConnection.getTelemetry();
    tReportingIntervalms = (config.containsKey("reserved.snowflake.telemetry" +
      ".reporting.interval.ms")) ?
      Long.parseLong(config.get("reserved.snowflake.telemetry.reporting" +
        ".interval.ms")) : 60 * 60 * 1000;
  }

  /**
   * stop method is invoked only once outstanding calls to other methods
   * have completed.
   * e.g. after current put, and a final preCommit has completed.
   * <p>
   * drop snowpipes
   */
  @Override
  public void stop()
  {
    LOGGER.info("SnowflakeSinkTask:stop");

    for (TopicPartition partition : partitions)
    {
      String tableName = config.get(partition.topic());
      String pipeName = Utils.pipeName(
        tableName,
        partition.partition());
      try
      {
        if (snowflakeConnection.pipeExist(pipeName))
        {
          snowflakeConnection.dropPipe(pipeName);
        }
        else
        {
          // User should not mess with these pipes outside the connector
          String errorMsg = "Attempting to drop a non-existant pipe " +
            pipeName + ". " +
            "This should not happen. " +
            "This pipe may have been manually dropped, which may affect " +
            "ingestion guarantees.";
          LOGGER.warn(errorMsg);
          telemetry.reportKafkaNonFatalError(errorMsg, connectorName);

          // continue-on-error
        }
      } catch (SQLException ex)
      {
        String errorMsg = "Failed to drop empty pipe " + pipeName + ". " +
          "It should be manually removed. Exception: " + ex.toString();
        LOGGER.error(errorMsg);
        telemetry.reportKafkaNonFatalError(errorMsg, connectorName);

        // continue-on-error
      }
    }

    // NOTE: it is not necessary to clear the in-memory structures in stop
  }

  /**
   * open method creates snowpipes for each partition assigned to this sink
   * task instance, and rebuilds in memory structures.
   * kafka connect calls this method right after task initialization
   * or for the purpose of partition rebalance
   * <p>
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
    LOGGER.info("SnowflakeSinkTask:open, TopicPartitions: {}", partitions);

    // clear in-memory data structure from earlier
    this.partitions.clear();
    this.partitionBuffers.clear();
    this.partitionFiles.clear();

    this.partitions.addAll(partitions);
    for (TopicPartition partition : partitions)
    {
      // initialize in-memory structures
      partitionBuffers.put(partition, new SnowflakePartitionBuffer());
      partitionFiles.put(partition, new HashMap<>());

      String tableName = config.get(partition.topic());   //
      // SnowflakeSinkConnector stores it here
      String stageName = Utils.stageName(tableName);
      String pipeName = Utils.pipeName(tableName, partition.partition());

      // create or validate pipe
      // pipe may already exist if the open was called for rebalancing
      // OR if the connector went through restart / recovery
      try
      {
        LOGGER.info("creating or validating pipe {} for table {}, for stage {}",
          pipeName, tableName, stageName);
        if (snowflakeConnection.pipeExist(pipeName))
        {
          if (!snowflakeConnection.pipeIsCompatible(pipeName, tableName,
            stageName))
          {
            // possible name collision with a pipe created outside the connector
            String errorMsg = "Pipe " + pipeName + " exists" +
              " but is incompatible with Snowflake Kafka Connector" +
              " and must be dropped before restarting the connector.";
            LOGGER.error(errorMsg);
            telemetry.reportKafkaFatalError(errorMsg, connectorName);

            // stop the connector as we are unlikely to automatically recover
            // from this error
            throw new ConnectException(errorMsg);
          }
        }
        else
        {
          // Kafka Connect calls SnowflakeSinkTask:start without waiting for
          // SnowflakeSinkConnector:start to finish. This causes a race
          // conditions.

          int counter = 0;
          boolean ready = false;
          while (counter < 120)    // poll for 120*5 seconds (10 mins) maximum
          {
            if (snowflakeConnection.tableExist(tableName) &&
              snowflakeConnection.stageExist(stageName))
            {
              ready = true;
              break;
            }
            counter++;

            try
            {
              LOGGER.info("Sleeping 5000ms to allow setup to complete.");
              Thread.sleep(5000);
            } catch (InterruptedException ex)
            {
              LOGGER.info("Waiting for setup to complete got interrupted");
            }
          }
          if (!ready)
          {
            String errorMsg = "SnowflakeSinkTask timed out. " +
              "Tables or stages are not yet available for data ingestion to" +
              " start. " +
              "If this persists, please contact Snowflake support.";
            LOGGER.error(errorMsg);
            telemetry.reportKafkaFatalError(errorMsg, connectorName);

            // cleanup
            this.close(this.partitions);
            this.stop();

            // stop the connector as we are unlikely to automatically recover
            // from this error
            throw new ConnectException(errorMsg);
          }


          // NOTE: snowflake doesn't throttle pipe creation, so party away!
          LOGGER.info("creating pipe {} for table {}, for stage {}",
            pipeName, tableName, stageName);
          snowflakeConnection.createPipe(pipeName, tableName, stageName, true);
          telemetry.reportKafkaCreatePipe(pipeName, stageName, tableName,
            connectorName);
        }

      } catch (SQLException ex)
      {
        String errorMsg = "Failed to create pipe " + pipeName + ". " +
          "User may have insufficient privileges. " +
          "If this persists, please contact Snowflake support. " +
          "Exception: " + ex.toString();
        LOGGER.error(errorMsg);
        telemetry.reportKafkaFatalError(errorMsg, connectorName);

        // cleanup
        this.close(this.partitions);
        this.stop();

        // stop the connector as we are unlikely to automatically recover
        // from this error
        throw new ConnectException(errorMsg);
      }

      // reconstruct in-memory structures from the files in stage, if any,
      // in case of rebalance or restart / recovery
      // store the file metadata for each file in the stage, and
      // compute the offset of the latest record that was processed in those
      // files

      HashMap<String, SnowflakeFileMetadata> myFilesMap = partitionFiles.get
        (partition);
      SnowflakePartitionBuffer myBuffer = partitionBuffers.get(partition);
      long latestOffset = myBuffer.latestOffset();
      String latestFileName = "";

      // list of files currently visible in the stage, and their status as
      // per LoadHistoryScan
      // TODO (GA) : we don't need the file status. We can get it in
      // preCommit method
      // TODO (GA) : batch the files into smaller sets if there are large
      // number of files

      Map<String, Utils.IngestedFileStatus> recoveredFiles;

      try
      {
        recoveredFiles = snowflakeConnection.recoverPipe(
          pipeName,
          stageName,
          Utils.subdirectoryName(tableName, partition.partition()));
      } catch (Exception ex)
      {
        String errorMsg = "Failed to get list of files from stage. " +
          "Please contact Snowflake support. Exception: " + ex.toString();
        LOGGER.error(errorMsg);
        telemetry.reportKafkaFatalError(errorMsg, connectorName);

        // cleanup
        this.close((this.partitions));
        this.stop();

        // stop the connector as we are unlikely to automatically recover
        // from this error
        throw new ConnectException(errorMsg);
      }

      Iterator<Map.Entry<String, Utils.IngestedFileStatus>> fileIterator =
        recoveredFiles.entrySet().iterator();
      while (fileIterator.hasNext())
      {
        Map.Entry<String, Utils.IngestedFileStatus> myFile = fileIterator
          .next();

        String fileName = myFile.getKey();
        myFilesMap.put(fileName, new SnowflakeFileMetadata(fileName));

        long endOffset = Utils.fileNameToEndOffset(fileName);
        if (latestOffset < endOffset)
        {
          latestOffset = endOffset;
          latestFileName = fileName;
        }
      }

      // update latestOffset in partitionBuffers
      // this will help discard records that kafka connect might resend
      myBuffer.setLatestOffset(latestOffset);
      if (!latestFileName.isEmpty())
      {
        LOGGER.info("Connector recovery: latest processed offset {}, for " +
            "partition {}, in file {}.",
          latestOffset, partition, latestFileName);
      }
    }

    resetDataTelemetry();
  }

  /**
   * close method handles the partitions that are no longer assigned to
   * this sink task.
   * kafka connect calls this method before a rebalance operation starts and
   * after the sink tasks stops fetching data
   * <p>
   * We don't drop snowpipe's in close method to avoid churn of snowpipes
   * due to rebalancing.
   * snowpipes are dropped in SinkTask:stop and SinkConnector:stop
   * <p>
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
    LOGGER.info("SnowflakeSinkTask:close: Unassigning TopicPartitions {}",
      partitions);

    // TODO (GA) (SNOW-63003) : create the summary file and drop it to
    // 'diagnostics and recovery stage'

/**     commenting out unnecessary code for performance reasons.
 *
 // clear in-memory structures
 for (TopicPartition partition : partitions)
 {
 // if partitionBuffers has any records that are not yet ingested to snowflake
 // kafka connect will resend those records after rebalance recovery
 if (partitionBuffers.containsKey(partition) &&
 partitionBuffers.get(partition).recordCount() != 0)
 {
 LOGGER.debug("Found unprocessed {} records in the in-memory buffer of
 SinkTask, " +
 "for partition {}", partitionBuffers.get(partition).recordCount(), partition
 .toString());
 }
 partitionBuffers.remove(partition);

 // partitionFiles may have some files that are not yet completely loaded to
 snowflake
 // we will recover the file list and metadata in then next life of the
 connector
 if (partitionFiles.containsKey(partition) &&
 !partitionFiles.get(partition).isEmpty())
 {
 LOGGER.debug("Found unprocessed {} files in the in-memory buffer of
 SinkTask, " +
 "for partition {}", partitionFiles.get(partition).size(), partition.toString
 ());
 }
 partitionFiles.remove(partition);

 // DO NOT drop the snowpipe here
 }
 *
 */
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
    LOGGER.debug("Number of records to process: {}", records.size());

    // process each record by adding to the corresponding partitionBuffers
    for (SinkRecord record : records)
    {
      TopicPartition partition = new TopicPartition(
        record.topic(),
        record.kafkaPartition());
      // NOTE : kafka connect framework uses inconsistent naming to get
      // partition info

      if (partitionBuffers.containsKey(partition))
      {
        SnowflakePartitionBuffer buffer = partitionBuffers.get(partition);

        // check if this record should be discarded
        // i.e. it may have been resent by Connect framework (expects
        // idempotent behavior),
        // or it may have been sent during a previous incarnation of the
        // connector

        if (record.kafkaOffset() > buffer.latestOffset())
        {
          // validate the record, and add record metadata
          try
          {
            // LOGGER.debug("Received record {}", record);
            String recordAsString = recordService.processRecord(record);
            buffer.bufferRecord(record, recordAsString);
          } catch (IllegalArgumentException ex)
          {
            // update the latest offset to avoid repeat processing in
            // following scenarios :
            // if all records are bad records
            // if the last record is a bad record
            buffer.setLatestOffset(record.kafkaOffset());

            // NOTE : This exception is thrown only by JsonRecordService,
            // when the record is a malformed json.
            // Throw an error message to user and drop a file with this
            // record in table stage
            try
            {
              String errorMsg = "Found a malformed JSON record (" +
                " topic " + record.topic() +
                ", partition " + record.kafkaPartition() +
                ", offset " + record.kafkaOffset() + " )." +
                " Saving to Snowflake table stage for table " + config.get
                (record.topic()) +
                ". Please see the documentation for further investigation.";
              LOGGER.error(errorMsg);
              telemetry.reportKafkaNonFatalError(errorMsg, connectorName);

              String tableName = config.get(record.topic());
              snowflakeConnection.putToTableStage(
                Utils.fileName(
                  tableName,
                  record.kafkaPartition(),
                  record.kafkaOffset(),
                  record.kafkaOffset()),
                record.toString(),
                tableName);
            } catch (SQLException ex_putToTableStage)
            {
              String errorMsg = "Failed to write the malformed JSON record (" +
                " topic " + record.topic() +
                ", partition " + record.kafkaPartition() +
                ", offset " + record.kafkaOffset() + " )" +
                " to Snowflake table stage for table " + config.get(record
                .topic()) +
                ". Check the privileges of the user running the connector." +
                " Please see the documentation for further investigation." +
                " Exception: " + ex_putToTableStage.toString();
              LOGGER.error(errorMsg);
              telemetry.reportKafkaNonFatalError(errorMsg, connectorName);

              // TODO: (arun, isaac) should this be considered fatal enough
              // to stop the connector
            }

            // continue to the next record
            continue;
          }

          // create a file and ingest if we got enough data in the buffer
          if (buffer.recordCount() >= this.bufferCountRecords ||
            buffer.bufferSize() >= this.bufferSizeBytes)
          {
            ingestBufferedRecords(partition);
          }
        }
      }
      else
      {
        // This shouldn't happen. It's a bug.
        String errorMsg = "Partition " + partition +
          " not found in current task's in-memory buffer " +
          partitionBuffers +
          ". This should not happen. Please contact Snowflake support.";
        LOGGER.error(errorMsg);

        // NOTE: do not include customer data in the telemetry reports
        String telemetryMsg = "Partition " + partition +
          " not found in current task's in-memory buffer " +
          ". This should not happen. Please contact Snowflake support.";
        telemetry.reportKafkaFatalError(telemetryMsg, connectorName);

        // cleanup
        this.close((this.partitions));
        this.stop();

        // stop the connector as we are unlikely to automatically
        // recover from this error
        throw new ConnectException(errorMsg);
      }
    }
  }

  /**
   * ingestBufferedRecords is a utility method that creates a file from
   * currently buffered records
   * for a topicPartition and ingests the file to table
   * <p>
   * Assumptions: the invoking methods are expected to ensure that buffer
   * exists for this topicPartition
   * so, we don't do additional checks in this method
   *
   * @param topicPartition
   */
  private void ingestBufferedRecords(TopicPartition topicPartition)
  {
    SnowflakePartitionBuffer buffer = partitionBuffers.get(topicPartition);
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
    String stageName = Utils.stageName(tableName);
    String fileName = Utils.fileName(
      tableName,
      topicPartition.partition(),
      buffer.firstOffset(),
      buffer.latestOffset());
    String pipeName = Utils.pipeName(
      tableName,
      topicPartition.partition());

    LOGGER.debug("Ingesting file: {}, for partition: {}", fileName,
      topicPartition);

    // create file
    try
    {
      snowflakeConnection.put(fileName, buffer.bufferAsString(), stageName);
    } catch (Exception ex)
    {
      String errorMsg = "Failed to write file " + fileName +
        " to Snowflake internal stage " + stageName +
        ". Check the privileges of the user running the connector. " +
        "If this persists, please contact Snowflake support. " +
        "Exception: " + ex.toString();
      LOGGER.error(errorMsg);
      telemetry.reportKafkaFatalError(errorMsg, connectorName);

      // cleanup
      this.close((this.partitions));
      this.stop();

      // stop the connector as we are unlikely to automatically recover from
      // this error
      throw new ConnectException(errorMsg);
    }

    // drop buffer
    buffer.dropRecords();

    // ingest file
    try
    {
      snowflakeConnection.ingestFile(pipeName, fileName);
    } catch (Exception ex)
    {
      String errorMsg = "Failed to ingest file " + fileName +
        " to pipe " + pipeName +
        ". Check if there is a Snowflake availability issue. " +
        "Exception: " + ex.toString();
      LOGGER.error(errorMsg);
      telemetry.reportKafkaFatalError(errorMsg, connectorName);

      // cleanup
      this.close((this.partitions));
      this.stop();

      // stop the connector as we are unlikely to automatically recover from
      // this error
      throw new ConnectException(errorMsg);

      // TODO (post GA) : don't throw connect exception but limit the number
      // of files that.
      // are piling in the snowflake internal stage.
    }

    // update in-memory file structure
    partitionFiles.get(topicPartition).put(fileName, new
      SnowflakeFileMetadata(fileName));

    LOGGER.debug("Ingested file: {}, for partition: {}", fileName,
      topicPartition);
  }


  /**
   * NOTE : Instead of flush method we use preCommit method which is meant to
   * be a
   * replacement for flush.
   * flush doesn't work for snowflake kafka connector because record
   * processing is
   * asynchronous to offset management
   *
   * @Override
   * public void flush (Map<TopicPartition, OffsetAndMetadata>
   *   currentOffsets) {}
   */


  /**
   * preCommit method is called by the connect framework to check how far
   * along
   * the offsets are committed
   *
   * @param offsets - the current map of offsets as of the last call to put
   * @return a map of offsets by topic-partition that are safe to commit
   * @throws RetriableException
   */
  @Override
  public Map<TopicPartition, OffsetAndMetadata> preCommit(
    Map<TopicPartition, OffsetAndMetadata> offsets)
    throws RetriableException
  {
    LOGGER.debug("offsets: {}", offsets);

/**     commenting out unnecessary code for performance reasons.
 *
 // ASSUMPTION : all partitions managed by this sink task are included in the
 ' offsets'
 if (partitions.size() != offsets.size() && partitions.containsAll(offsets
 .keySet()))
 {
 // This shouldn't happen. It's a bug in snowflake connector or kafka connect
 f ramework
 String errorMsg = "Non-matching number of partitions " +partitions+
 ", and offsets " +offsets+
 ". This should not happen. Please contact Snowflake support.";
 LOGGER.error(errorMsg);
 telemetry.reportKafkaFatalError(errorMsg, connectorName);

 // cleanup
 this.close((this.partitions));
 this.stop();

 // stop the connector as we are unlikely to automatically recover from this
 error
 throw new ConnectException(errorMsg);
 }
 *
 */

    // NOTE: minor performance improvement can be achieved by validating
    // server objects
    // upon a trigger e.g. too many load history scans, etc
    validateServerObjects();    // e.g. SNOW-68472

    // return object, containing offset value for each partition that can be
    // high water mark for
    // purging records from kafka
    Map<TopicPartition, OffsetAndMetadata> committedOffsets = new HashMap<>();

    // iterate over input 'offsets' or in-memory structure 'partitions'.
    // Doesn't matter which one.
    Iterator<TopicPartition> partitionIterator = partitions.iterator();
    while (partitionIterator.hasNext())
    {
      TopicPartition partition = partitionIterator.next();
      String tableName = config.get(partition.topic());   //
      // SnowflakeSinkConnector stores it here
      String stageName = Utils.stageName(tableName);
      String pipeName = Utils.pipeName(
        tableName,
        partition.partition());

      // ingest buffered records, if any
      ingestBufferedRecords(partition);

      // skip processing this partition if there are no files to process
      // NOTE: Any exception thrown in SnowflakeSinkTask causes race condition
      // between preCommit and close method. close method cleans up in-memory
      // structures
      // causing NPE in preCommit method.
      if (partitionFiles.containsKey(partition))
      {
        if ((partitionFiles.get(partition)).isEmpty())
        {
          LOGGER.debug("No files pending ingestion status check for " +
            "partition: {}", partition);
          continue;
        }
      }
      else
      {
        LOGGER.debug("No files ingested for partition: {}", partition);
        continue;
      }

      // TODO (GA) : performance improvement.
      // Use Set instead of List all the way, and reduce conversion overheads

      // sorted list of files to check status on
      HashMap<String, SnowflakeFileMetadata> myFilesMap = partitionFiles.get
        (partition);
      List<String> myFiles = new ArrayList<>(myFilesMap.keySet());
      myFiles.sort(new SortFileNamesByFirstOffset());
      LOGGER.debug("Checking status of files: {}, for partition: {}",
        myFiles, partition);

      // identify if we should use load history scan or insert report
      // use load history scan iff
      //      number of file > 10,000
      //      OR
      //      oldest file > 10 minutes old
      // otherwise use the 'cheaper' insert report

      boolean loadHistoryScan = (myFiles.size() > 10000);
      String oldestFileName = "";
      long oldestFileTime = System.currentTimeMillis();
      if (!loadHistoryScan)
      {
        Iterator<String> fileIterator = myFiles.iterator();
        while (fileIterator.hasNext())
        {
          String currentFileName = fileIterator.next();
          long currentFileTime = Utils.fileNameToTimeIngested(currentFileName);
          if (oldestFileTime > currentFileTime)
          {
            oldestFileName = currentFileName;
            oldestFileTime = currentFileTime;
          }
          // TODO (GA) : performance improvement.
          // break the loop on the first old enough file
        }
        loadHistoryScan = ((System.currentTimeMillis() - oldestFileTime) >
          600000);
      }

      // status of files from snowpipe load history scan or insert report
      // TODO (GA) : batch the files into smaller sets if there are large
      // number of files
      Map<String, Utils.IngestedFileStatus> myFilesStatus;
      try
      {
        if (loadHistoryScan)
        {
          LOGGER.warn("Using Load History Scan, " +
              "number of files {}, oldest file {}.",
            myFiles.size(), oldestFileName);
          myFilesStatus = snowflakeConnection.verifyFromLoadHistory(pipeName,
            myFiles);
        }
        else
        {
          LOGGER.debug("Using Insert Report, " +
              "number of files {}, oldest file {}.",
            myFiles.size(), oldestFileName);
          myFilesStatus = snowflakeConnection.verifyFromIngestReport
            (pipeName, myFiles);
        }
      } catch (Exception ex)
      {
        String errorMsg = "Failed to verify status of files from pipe " +
          pipeName +
          ". This action will be retried. Exception: " + ex.toString();
        LOGGER.warn(errorMsg);
        telemetry.reportKafkaNonFatalError(errorMsg, connectorName);

        throw new RetriableException(errorMsg);
      }

      // list of files that are verified as successfully ingested.
      // They will be purged from stage
      List<String> filesSucceeded = new LinkedList<>();

      // list of files that have *any* failures
      // They will be moved to table stage
      List<String> filesFailed = new LinkedList<>();

      boolean continueUpdatingCommittedOffset = true;  // keep progressing
      // offset while this is true
      long committedOffsetValue = 0;

      // iterate over sorted list of files
      Iterator<String> fileIterator = myFiles.iterator();
      while (fileIterator.hasNext())
      {
        String fileName = fileIterator.next();
        Utils.IngestedFileStatus fileStatus = myFilesStatus.get(fileName);

        // latest file needs special handling to avoid getting it purged,
        // while ensuring that offset information is processed correctly
        if (!fileIterator.hasNext())
        {
          switch (fileStatus)
          {
            case LOADED:
            case PARTIALLY_LOADED:
            case FAILED:
            case EXPIRED:
              // update the status
              myFilesStatus.replace(fileName, Utils.IngestedFileStatus
                .LATEST_FILE_STATUS_FINALIZED);
              break;
            default:
              // update the status to not finalized
              myFilesStatus.replace(fileName, Utils.IngestedFileStatus
                .LATEST_FILE_STATUS_NOT_FINALIZED);
              break;
          }
        }


        switch (fileStatus)
        {
          case LOADED:
            filesSucceeded.add(fileName);
            myFilesMap.remove(fileName);
            LOGGER.debug("File {}, partition {}: successfully loaded. " +
                "File will be purged from internal stage.",
              fileName, partition);
            break;

          case PARTIALLY_LOADED:
            filesFailed.add(fileName);
            myFilesMap.remove(fileName);
            LOGGER.error("File {}, partition {}: some records failed to load." +
                " " +
                "File is being moved to table stage for further " +
                "investigation.",
              fileName, partition);
            break;

          case FAILED:
            filesFailed.add(fileName);
            myFilesMap.remove(fileName);
            LOGGER.error("File {}, partition {}: file failed to load. " +
                "File is being moved to table stage for further " +
                "investigation.",
              fileName, partition);
            break;

          case LATEST_FILE_STATUS_FINALIZED:
            // LATEST_FILE needs special handling and it should not be purged
            // to ensure correct recovery
            // But should be removed from in-memory structure, otherwise it
            // would cause Load History scans
            myFilesMap.remove(fileName);
            LOGGER.info("File {} is the latest file and is in a finalized " +
                "state." +
                " Leaving it in the stage for future recovery.", fileName,
              partition);
            break;

          case EXPIRED:
            // this is unlikely, unless the connector is unable to keep up
            filesFailed.add(fileName);
            myFilesMap.remove(fileName);
            LOGGER.error("File {}, partition {}: file too old." +
                " History is not available to check whether it was " +
                "successfully loaded." +
                " File is being moved to table stage for further " +
                "investigation.",
              fileName, partition);
            break;

//                    case LOST:
//                        // This should never happen. It's a bug in snowflake.
//                        filesFailed.add(fileName);
//                        LOGGER.error("file {}, partition {}, file is lost
// from history. " +
//                                        "This is a bug in Snowflake. Please
// escalate to Snowflake Support. " +
//                                        "File is being moved to table stage
// for further investigation.",
//                                fileName, partition);
//                        break;

          case LOAD_IN_PROGRESS:
          case LATEST_FILE_STATUS_NOT_FINALIZED:
          case NOT_FOUND:
          default:
            // stop updating the committed offset for now until the next call
            // to preCommit,
            // but continue the file traversal to purge the files which are
            // 'DONE'
            continueUpdatingCommittedOffset = false;
            break;
        }

        // move forward offset value if the file had a 'finalized' status
        committedOffsetValue = (continueUpdatingCommittedOffset) ?
          Utils.fileNameToEndOffset(fileName) :
          committedOffsetValue;
      }

      // purge the successful files
      try
      {
        snowflakeConnection.purge(stageName, filesSucceeded);
      } catch (Exception ex)
      {
        String errorMsg = "Failed to purge files from stage " + stageName +
          ". This action will be retried. Exception: " + ex.toString();
        LOGGER.warn(errorMsg);
        telemetry.reportKafkaNonFatalError(errorMsg, connectorName);

        throw new RetriableException(errorMsg);
      }
            /*
            finally
            {
                filesSucceeded.clear();
            }
            */

      // move the failed files
      try
      {
        snowflakeConnection.moveToTableStage(stageName, tableName, filesFailed);
      } catch (Exception ex)
      {
        String errorMsg = "Failed to move files from internal stage " +
          stageName +
          " to table stage for table " + tableName +
          ". This action will be retried. Exception: " + ex.toString();
        LOGGER.warn(errorMsg);
        telemetry.reportKafkaNonFatalError(errorMsg, connectorName);

        throw new RetriableException(errorMsg);
      }
            /*
            finally
            {
                filesFailed.clear();
            }
            */

      // update committed offset in the in-memory structure and output
      partitionBuffers.get(partition).setCommittedOffset
        (committedOffsetValue);
      committedOffsets.put(partition, new OffsetAndMetadata
        (committedOffsetValue));
    }

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
      try
      {
        if (!snowflakeConnection.tableIsCompatible(table))  // checks for
        // existence and compatibility
        {
          String errorMsg = "Table " + table +
            " is configured to receive records from topic " + topic +
            " but it has either been dropped or a schema change has made it" +
            " incompatible.";
          LOGGER.error(errorMsg);
          telemetry.reportKafkaFatalError(errorMsg, connectorName);

          // cleanup
          stop();

          // stop the connector as we are unlikely to automatically recover
          // from this error
          throw new ConnectException(errorMsg);
        }
      } catch (SQLException ex)
      {
        String errorMsg = "SQLException while validating server objects. This" +
          " action will be retried. " +
          "If this persists, please contact Snowflake support. " +
          "Exception: " + ex.toString();
        LOGGER.warn(errorMsg);
        telemetry.reportKafkaNonFatalError(errorMsg, connectorName);

        // continue on error
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