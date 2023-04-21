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

import static com.snowflake.kafka.connector.internal.streaming.TopicPartitionChannel.NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE;

import com.google.common.annotations.VisibleForTesting;
import com.snowflake.kafka.connector.dlq.KafkaRecordErrorReporter;
import com.snowflake.kafka.connector.internal.KCLogger;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionServiceFactory;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import com.snowflake.kafka.connector.internal.SnowflakeSinkService;
import com.snowflake.kafka.connector.internal.SnowflakeSinkServiceFactory;
import com.snowflake.kafka.connector.internal.streaming.IngestionMethodConfig;
import com.snowflake.kafka.connector.records.SnowflakeMetadataConfig;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

/**
 * SnowflakeSinkTask implements SinkTask for Kafka Connect framework.
 *
 * <p>Expects configuration from SnowflakeSinkConnector
 *
 * <p>Creates sink service instance, takes records loaded from those Kafka partitions and ingests to
 * Snowflake via Sink service
 */
public class SnowflakeSinkTask extends SinkTask {
  // SfTask[ID:taskId.taskOpenCount, #totalTaskCreationCount]
  // Example: SfTask[ID:0.1, #2] would indicate that this is a task with id 0, it has been opened
  // once, and that
  // this instance of KC has created two tasks
  public static final String TASK_INSTANCE_TAG_FORMAT = "SfTask[ID:{}.{}, #{}]";

  private static final long WAIT_TIME = 5 * 1000; // 5 sec
  private static final int REPEAT_TIME = 12; // 60 sec

  // tracks total number of tasks created in this kc instance, default (when KC isn't running) is -1
  private static int totalTaskCreationCount = -1;

  // the dynamic logger is intended to be attached per task instance. the instance id will be set
  // during task start, however if it is not set, it falls back to the static logger
  private static final KCLogger STATIC_LOGGER =
      new KCLogger(SnowflakeSinkTask.class.getName() + "_STATIC");
  private KCLogger DYNAMIC_LOGGER;

  // After 5 put operations, we will insert a sleep which will cause a rebalance since heartbeat is
  // not found
  private final int REBALANCING_THRESHOLD = 10;

  // This value should be more than max.poll.interval.ms
  // check connect-distributed.properties file used to start kafka connect
  private final int rebalancingSleepTime = 370000;

  private SnowflakeSinkService sink = null;
  private Map<String, String> topic2table = null;

  // snowflake JDBC connection provides methods to interact with user's
  // snowflake
  // account and execute queries
  private SnowflakeConnectionService conn = null;

  // tracks number of tasks the config wants to create
  private String taskConfigId = "-1";

  // Rebalancing Test
  private boolean enableRebalancing = SnowflakeSinkConnectorConfig.REBALANCING_DEFAULT;
  // After REBALANCING_THRESHOLD put operations, insert a thread.sleep which will trigger rebalance
  private int rebalancingCounter = 0;

  private long taskStartTime;

  private long taskOpenCount;

  public static void setTotalTaskCreationCount(int newCreationCount) {
    STATIC_LOGGER.info("Setting task creation count to {} for logging", newCreationCount);
    totalTaskCreationCount = newCreationCount;
  }

  /** default constructor, invoked by kafka connect framework */
  public SnowflakeSinkTask() {
    DYNAMIC_LOGGER = new KCLogger(this.getClass().getName());
    // only increment task creation count if we know kc has been started
    totalTaskCreationCount =
        totalTaskCreationCount != -1 ? totalTaskCreationCount + 1 : totalTaskCreationCount;
    this.taskOpenCount = 0;
  }

  @VisibleForTesting
  public SnowflakeSinkTask(
      SnowflakeSinkService service, SnowflakeConnectionService connectionService) {
    DYNAMIC_LOGGER = new KCLogger(this.getClass().getName());
    // only increment task creation count if we know kc has been started
    totalTaskCreationCount =
        totalTaskCreationCount != -1 ? totalTaskCreationCount + 1 : totalTaskCreationCount;
    this.taskOpenCount = 0;
    this.sink = service;
    this.conn = connectionService;
  }

  private SnowflakeConnectionService getConnection() {
    try {
      waitFor(() -> conn != null);
    } catch (Exception e) {
      throw SnowflakeErrors.ERROR_5013.getException();
    }
    return conn;
  }

  /**
   * Return an instance of SnowflakeConnection if it was set previously by calling Start(). Else,
   * return an empty
   *
   * @return Optional of SnowflakeConnectionService
   */
  public Optional<SnowflakeConnectionService> getSnowflakeConnection() {
    return Optional.ofNullable(getConnection());
  }

  private SnowflakeSinkService getSink() {
    try {
      waitFor(() -> sink != null && !sink.isClosed());
    } catch (Exception e) {
      throw SnowflakeErrors.ERROR_5014.getException();
    }
    return sink;
  }

  /**
   * start method handles configuration parsing and one-time setup of the task. loads configuration
   *
   * @param parsedConfig - has the configuration settings
   */
  @Override
  public void start(final Map<String, String> parsedConfig) {
    this.taskStartTime = System.currentTimeMillis();

    // connector configuration
    this.taskConfigId = parsedConfig.getOrDefault(Utils.TASK_ID, "-1");

    // setup logging
    this.DYNAMIC_LOGGER.info(
        "Defining SnowflakeSinkTask instance tag to SfTask[ID:{taskId}.{taskOpenCount},"
            + " #{totalTaskCreationCount}], where taskId is pulled from the config, taskOpenCount"
            + " is the number of times this task has been opened and totalTaskCreationCount is the"
            + " total number of tasks created during this run of Snowflake Kafka Connector");

    this.DYNAMIC_LOGGER.setLoggerInstanceTag(this.getTaskLoggingTag());

    this.DYNAMIC_LOGGER.debug("starting task...");

    // generate topic to table map
    this.topic2table = getTopicToTableMap(parsedConfig);

    // generate metadataConfig table
    SnowflakeMetadataConfig metadataConfig = new SnowflakeMetadataConfig(parsedConfig);

    // enable jvm proxy
    Utils.enableJVMProxy(parsedConfig);

    // config buffer.count.records -- how many records to buffer
    final long bufferCountRecords =
        Long.parseLong(parsedConfig.get(SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS));
    // config buffer.size.bytes -- aggregate size in bytes of all records to
    // buffer
    final long bufferSizeBytes =
        Long.parseLong(parsedConfig.get(SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES));
    final long bufferFlushTime =
        Long.parseLong(parsedConfig.get(SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC));

    // Falling back to default behavior which is to ingest an empty json string if we get null
    // value. (Tombstone record)
    SnowflakeSinkConnectorConfig.BehaviorOnNullValues behavior =
        SnowflakeSinkConnectorConfig.BehaviorOnNullValues.DEFAULT;
    if (parsedConfig.containsKey(SnowflakeSinkConnectorConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG)) {
      // we can always assume here that value passed in would be an allowed value, otherwise the
      // connector would never start or reach the sink task stage
      behavior =
          SnowflakeSinkConnectorConfig.BehaviorOnNullValues.valueOf(
              parsedConfig.get(SnowflakeSinkConnectorConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG));
    }

    // we would have already validated the config inside SFConnector start()
    boolean enableCustomJMXMonitoring = SnowflakeSinkConnectorConfig.JMX_OPT_DEFAULT;
    if (parsedConfig.containsKey(SnowflakeSinkConnectorConfig.JMX_OPT)) {
      enableCustomJMXMonitoring =
          Boolean.parseBoolean(parsedConfig.get(SnowflakeSinkConnectorConfig.JMX_OPT));
    }

    enableRebalancing =
        Boolean.parseBoolean(parsedConfig.get(SnowflakeSinkConnectorConfig.REBALANCING));

    KafkaRecordErrorReporter kafkaRecordErrorReporter = createKafkaRecordErrorReporter();

    // default to snowpipe
    IngestionMethodConfig ingestionType = IngestionMethodConfig.SNOWPIPE;
    if (parsedConfig.containsKey(SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT)) {
      ingestionType =
          IngestionMethodConfig.valueOf(
              parsedConfig.get(SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT).toUpperCase());
    }

    conn =
        SnowflakeConnectionServiceFactory.builder()
            .setProperties(parsedConfig)
            .setTaskID(this.taskConfigId)
            .build();

    if (this.sink != null) {
      this.sink.closeAll();
    }
    this.sink =
        SnowflakeSinkServiceFactory.builder(getConnection(), ingestionType, parsedConfig)
            .setFileSize(bufferSizeBytes)
            .setRecordNumber(bufferCountRecords)
            .setFlushTime(bufferFlushTime)
            .setTopic2TableMap(topic2table)
            .setMetadataConfig(metadataConfig)
            .setBehaviorOnNullValuesConfig(behavior)
            .setCustomJMXMetrics(enableCustomJMXMonitoring)
            .setErrorReporter(kafkaRecordErrorReporter)
            .setSinkTaskContext(this.context)
            .build();

    DYNAMIC_LOGGER.debug(
        "task started, execution time: {} seconds",
        this.taskConfigId,
        getExecutionTimeSec(this.taskStartTime, System.currentTimeMillis()));
  }

  /**
   * stop method is invoked only once outstanding calls to other methods have completed. e.g. after
   * current put, and a final preCommit has completed.
   */
  @Override
  public void stop() {
    if (this.sink != null) {
      this.sink.setIsStoppedToTrue(); // close cleaner thread
    }

    this.DYNAMIC_LOGGER.debug(
        "task stopped, total task runtime: {} seconds",
        getExecutionTimeSec(this.taskStartTime, System.currentTimeMillis()));
    this.DYNAMIC_LOGGER.clearLoggerInstanceIdTag();
  }

  /**
   * init ingestion task in Sink service
   *
   * @param partitions - The list of all partitions that are now assigned to the task
   */
  @Override
  public void open(final Collection<TopicPartition> partitions) {
    this.taskOpenCount++;
    this.DYNAMIC_LOGGER.setLoggerInstanceTag(this.getTaskLoggingTag());

    long startTime = System.currentTimeMillis();
    partitions.forEach(
        tp -> this.sink.startTask(Utils.tableName(tp.topic(), this.topic2table), tp));
    this.DYNAMIC_LOGGER.debug(
        "task opened with {} partitions, execution time: {} seconds",
        partitions.size(),
        getExecutionTimeSec(startTime, System.currentTimeMillis()));
  }

  /**
   * Closes sink service
   *
   * <p>Closes all running task because the parameter of open function contains all partition info
   * but not only the new partition
   *
   * @param partitions - The list of all partitions that were assigned to the task
   */
  @Override
  public void close(final Collection<TopicPartition> partitions) {
    long startTime = System.currentTimeMillis();
    if (this.sink != null) {
      this.sink.close(partitions);
    }

    this.DYNAMIC_LOGGER.debug(
        "task closed, execution time: {} seconds",
        this.taskConfigId,
        getExecutionTimeSec(startTime, System.currentTimeMillis()));
  }

  /**
   * ingest records to Snowflake
   *
   * @param records - collection of records from kafka topic/partitions for this connector
   */
  @Override
  public void put(final Collection<SinkRecord> records) {
    if (enableRebalancing && records.size() > 0) {
      processRebalancingTest();
    }

    long startTime = System.currentTimeMillis();

    getSink().insert(records);

    logWarningForPutAndPrecommit(startTime, records.size(), "put");
  }

  /**
   * Sync committed offsets
   *
   * @param offsets - the current map of offsets as of the last call to put
   * @return an empty map if Connect-managed offset commit is not desired, otherwise a map of
   *     offsets by topic-partition that are safe to commit. If we return the same offsets that was
   *     passed in, Kafka Connect assumes that all offsets that are already passed to put() are safe
   *     to commit.
   * @throws RetriableException when meet any issue during processing
   */
  @Override
  public Map<TopicPartition, OffsetAndMetadata> preCommit(
      Map<TopicPartition, OffsetAndMetadata> offsets) throws RetriableException {
    long startTime = System.currentTimeMillis();

    // return an empty map means that offset commitment is not desired
    if (sink == null || sink.isClosed()) {
      this.DYNAMIC_LOGGER.warn(
          "sink not initialized or closed before preCommit", this.taskConfigId);
      return new HashMap<>();
    } else if (sink.getPartitionCount() == 0) {
      this.DYNAMIC_LOGGER.warn("no partition is assigned", this.taskConfigId);
      return new HashMap<>();
    }

    Map<TopicPartition, OffsetAndMetadata> committedOffsets = new HashMap<>();
    // it's ok to just log the error since commit can retry
    try {
      offsets.forEach(
          (topicPartition, offsetAndMetadata) -> {
            long offSet = sink.getOffset(topicPartition);
            if (offSet != NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE) {
              committedOffsets.put(topicPartition, new OffsetAndMetadata(offSet));
            }
          });
    } catch (Exception e) {
      this.DYNAMIC_LOGGER.error("PreCommit error: {} ", e.getMessage());
    }

    logWarningForPutAndPrecommit(startTime, offsets.size(), "precommit");
    return committedOffsets;
  }

  /** @return connector version */
  @Override
  public String version() {
    return Utils.VERSION;
  }

  /**
   * parse topic to table map
   *
   * @param config connector config file
   * @return result map
   */
  static Map<String, String> getTopicToTableMap(Map<String, String> config) {
    if (config.containsKey(SnowflakeSinkConnectorConfig.TOPICS_TABLES_MAP)) {
      Map<String, String> result =
          Utils.parseTopicToTableMap(config.get(SnowflakeSinkConnectorConfig.TOPICS_TABLES_MAP));
      if (result != null) {
        return result;
      }
      STATIC_LOGGER.error("Invalid Input, Topic2Table Map disabled");
    }
    return new HashMap<>();
  }

  /**
   * wait for specific status
   *
   * @param func status checker
   */
  private static void waitFor(Supplier<Boolean> func)
      throws InterruptedException, TimeoutException {
    for (int i = 0; i < REPEAT_TIME; i++) {
      if (func.get()) {
        return;
      }
      Thread.sleep(WAIT_TIME);
    }
    throw new TimeoutException();
  }

  private static long getExecutionTimeSec(long startTime, long endTime) {
    return (endTime - startTime) / 1000;
  }

  void logWarningForPutAndPrecommit(long startTime, int size, String apiName) {
    long executionTime = getExecutionTimeSec(startTime, System.currentTimeMillis());
    if (executionTime > 300) {
      // This won't be frequently printed. It is vary rare to have execution greater than 300
      // seconds.
      // But having this warning helps customer to debug their Kafka Connect config.
      this.DYNAMIC_LOGGER.warn(
          "{} {}. Time: {} seconds > 300 seconds. If there is CommitFailedException in the log or"
              + " there is duplicated records, refer to this link for solution: "
              + "https://docs.snowflake.com/en/user-guide/kafka-connector-ts.html#resolving-specific-issues",
          apiName,
          size,
          executionTime);
    } else {
      this.DYNAMIC_LOGGER.debug(
          "successfully called {} with {} records, execution time: {} seconds",
          apiName,
          size,
          getExecutionTimeSec(startTime, System.currentTimeMillis()));
    }
  }

  /** When rebalancing test is enabled, trigger sleep after rebalacing threshold is reached */
  void processRebalancingTest() {
    rebalancingCounter++;
    if (rebalancingCounter == REBALANCING_THRESHOLD) {
      try {
        this.DYNAMIC_LOGGER.debug(
            "[TEST_ONLY] Sleeping :{} ms to trigger a rebalance", rebalancingSleepTime);
        Thread.sleep(rebalancingSleepTime);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  /* Used to report a record back to DLQ if error tolerance is specified */
  private KafkaRecordErrorReporter createKafkaRecordErrorReporter() {
    KafkaRecordErrorReporter result = noOpKafkaRecordErrorReporter();
    if (context != null) {
      try {
        ErrantRecordReporter errantRecordReporter = context.errantRecordReporter();
        if (errantRecordReporter != null) {
          result =
              (record, error) -> {
                try {
                  // Blocking this until record is delivered to DLQ
                  DYNAMIC_LOGGER.debug(
                      "Sending Sink Record to DLQ with recordOffset:{}, partition:{}",
                      record.kafkaOffset(),
                      record.kafkaPartition());
                  errantRecordReporter.report(record, error).get();
                } catch (InterruptedException | ExecutionException e) {
                  final String errMsg = "ERROR reporting records to ErrantRecordReporter";
                  this.DYNAMIC_LOGGER.error(errMsg, e);
                  throw new ConnectException(errMsg, e);
                }
              };
        } else {
          this.DYNAMIC_LOGGER.info("Errant record reporter is not configured.");
        }
      } catch (NoClassDefFoundError | NoSuchMethodError e) {
        // Will occur in Connect runtimes earlier than 2.6
        this.DYNAMIC_LOGGER.info(
            "Kafka versions prior to 2.6 do not support the errant record reporter.");
      }
    } else {
      DYNAMIC_LOGGER.warn("SinkTaskContext is not set");
    }
    return result;
  }

  private String getTaskLoggingTag() {
    int countThreshold = 999;

    if (totalTaskCreationCount > countThreshold) {
      this.DYNAMIC_LOGGER.warn(
          "More than {} tasks have been created. Resetting to 0", countThreshold);
      totalTaskCreationCount = 0;
    }

    return Utils.formatString(
        TASK_INSTANCE_TAG_FORMAT, this.taskConfigId, this.taskOpenCount, totalTaskCreationCount);
  }

  /**
   * For versions older than 2.6
   *
   * @see <a
   *     href="https://javadoc.io/doc/org.apache.kafka/connect-api/2.6.0/org/apache/kafka/connect/sink/ErrantRecordReporter.html">
   *     link </a>
   */
  @VisibleForTesting
  static KafkaRecordErrorReporter noOpKafkaRecordErrorReporter() {
    return (record, e) -> {
      STATIC_LOGGER.warn(
          "DLQ Kafka Record Error Reporter is not set, requires Kafka Version to be >= 2.6");
    };
  }
}
