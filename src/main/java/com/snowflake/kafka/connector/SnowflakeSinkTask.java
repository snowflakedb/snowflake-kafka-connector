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

import static com.snowflake.kafka.connector.internal.streaming.channel.TopicPartitionChannel.NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE;

import com.google.common.annotations.VisibleForTesting;
import com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams;
import com.snowflake.kafka.connector.dlq.KafkaRecordErrorReporter;
import com.snowflake.kafka.connector.internal.KCLogger;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionServiceFactory;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException;
import com.snowflake.kafka.connector.internal.SnowflakeSinkService;
import com.snowflake.kafka.connector.internal.streaming.SnowflakeSinkServiceV2;
import com.snowflake.kafka.connector.internal.streaming.channel.TopicPartitionChannel;
import com.snowflake.kafka.connector.internal.streaming.telemetry.PeriodicTelemetryReporter;
import java.util.Arrays;
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
 * <p>Expects configuration from SnowflakeStreamingSinkConnector
 *
 * <p>Creates sink service instance, takes records loaded from those Kafka partitions and ingests to
 * Snowflake via Sink service
 */
public class SnowflakeSinkTask extends SinkTask {
  private static final long WAIT_TIME = 5 * 1000; // 5 sec
  private static final int REPEAT_TIME = 12; // 60 sec

  // the dynamic logger is intended to be attached per task instance. the instance id will be set
  // during task start, however if it is not set, it falls back to the static logger
  private static final KCLogger STATIC_LOGGER =
      new KCLogger(SnowflakeSinkTask.class.getName() + "_STATIC");
  private KCLogger DYNAMIC_LOGGER;

  private volatile SnowflakeSinkService sink = null;
  private Map<String, String> topic2table = null;

  // snowflake JDBC connection provides methods to interact with user's
  // snowflake
  // account and execute queries
  private SnowflakeConnectionService conn = null;

  // tracks number of tasks the config wants to create
  private String taskConfigId = "-1";

  private long taskStartTime;

  private final SnowflakeSinkTaskAuthorizationExceptionTracker authorizationExceptionTracker =
      new SnowflakeSinkTaskAuthorizationExceptionTracker();

  // Stores channel error exception detected in preCommit to fail on next put() call
  private volatile SnowflakeKafkaConnectorException channelErrorToFailOn = null;

  // Periodic telemetry reporter for channel status
  private PeriodicTelemetryReporter telemetryReporter = null;

  /** default constructor, invoked by kafka connect framework */
  public SnowflakeSinkTask() {
    DYNAMIC_LOGGER = new KCLogger(this.getClass().getName());
  }

  @VisibleForTesting
  public SnowflakeSinkTask(
      SnowflakeSinkService service, SnowflakeConnectionService connectionService) {
    DYNAMIC_LOGGER = new KCLogger(this.getClass().getName());
    this.sink = service;
    this.conn = connectionService;
  }

  @VisibleForTesting
  // @codeCoverageIgnore
  public SnowflakeSinkTask(
      SnowflakeSinkService service,
      SnowflakeConnectionService connectionService,
      Map<String, String> topic2table) {
    this(service, connectionService);
    this.topic2table = topic2table;
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

  protected SnowflakeSinkService getSink() {
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
    this.DYNAMIC_LOGGER.info("starting task...");

    // get task id and start time
    this.taskStartTime = System.currentTimeMillis();
    this.taskConfigId = parsedConfig.get(Utils.TASK_ID);
    if (this.taskConfigId == null || this.taskConfigId.trim().isEmpty()) {
      throw new IllegalArgumentException(
          "Task ID ('" + Utils.TASK_ID + "') must be set and cannot be null or empty");
    }

    // generate topic to table map
    this.topic2table = getTopicToTableMap(parsedConfig);

    this.authorizationExceptionTracker.updateStateOnTaskStart(parsedConfig);

    // enable jvm proxy
    Utils.enableJVMProxy(parsedConfig);

    // Falling back to default behavior which is to ingest an empty json string if we get null
    // value. (Tombstone record)
    ConnectorConfigTools.BehaviorOnNullValues behavior =
        ConnectorConfigTools.BehaviorOnNullValues.DEFAULT;
    if (parsedConfig.containsKey(KafkaConnectorConfigParams.BEHAVIOR_ON_NULL_VALUES)) {
      // we can always assume here that value passed in would be an allowed value, otherwise the
      // connector would never start or reach the sink task stage
      behavior =
          ConnectorConfigTools.BehaviorOnNullValues.valueOf(
              parsedConfig.get(KafkaConnectorConfigParams.BEHAVIOR_ON_NULL_VALUES));
    }

    // we would have already validated the config inside SFConnector start()
    boolean enableCustomJMXMonitoring = KafkaConnectorConfigParams.JMX_OPT_DEFAULT;
    if (parsedConfig.containsKey(KafkaConnectorConfigParams.JMX_OPT)) {
      enableCustomJMXMonitoring =
          Boolean.parseBoolean(parsedConfig.get(KafkaConnectorConfigParams.JMX_OPT));
    }

    KafkaRecordErrorReporter kafkaRecordErrorReporter = createKafkaRecordErrorReporter();

    conn =
        SnowflakeConnectionServiceFactory.builder()
            .setProperties(parsedConfig)
            .setTaskID(this.taskConfigId)
            .build();

    if (this.sink != null) {
      this.sink.closeAll();
    }

    this.sink =
        new SnowflakeSinkServiceV2(
            conn,
            parsedConfig,
            kafkaRecordErrorReporter,
            this.context,
            enableCustomJMXMonitoring,
            topic2table,
            behavior);

    // Initialize and start periodic telemetry reporter for channel status

    Supplier<Map<String, TopicPartitionChannel>> channelSupplier =
        () -> sink.getPartitionChannels();
    String connectorName = parsedConfig.get(Constants.KafkaConnectorConfigParams.NAME);
    this.telemetryReporter =
        new PeriodicTelemetryReporter(
            conn.getTelemetryClient(), channelSupplier, connectorName, this.taskConfigId);
    this.telemetryReporter.start();

    DYNAMIC_LOGGER.info(
        "task started, execution time: {} milliseconds",
        this.taskConfigId,
        getDurationFromStartMs(this.taskStartTime));
  }

  /**
   * stop method is invoked only once outstanding calls to other methods have completed. e.g. after
   * current put, and a final preCommit has completed.
   *
   * <p>Note that calling this method does not perform synchronous cleanup in Snowpipe based
   * implementation
   */
  @Override
  public void stop() {
    this.DYNAMIC_LOGGER.info("stopping task {}", this.taskConfigId);

    // Stop telemetry reporter first
    if (this.telemetryReporter != null) {
      this.telemetryReporter.stop();
    }

    if (this.sink != null) {
      try {
        this.sink.stop();
      } catch (Exception e) {
        // Broad catch: conn.close() must still run regardless of sink.stop() failure
        this.DYNAMIC_LOGGER.error("Failed to stop sink service: {}", e.getMessage(), e);
      }
    }

    if (this.conn != null) {
      try {
        this.conn.close();
      } catch (Exception e) {
        // Broad catch: stop() must not throw to avoid masking earlier errors
        this.DYNAMIC_LOGGER.warn("Failed to close connection: {}", e.getMessage(), e);
      }
    }

    this.DYNAMIC_LOGGER.info(
        "task stopped, total task runtime: {} milliseconds",
        getDurationFromStartMs(this.taskStartTime));
  }

  /**
   * init ingestion task in Sink service
   *
   * @param partitions - The list of all partitions that are now assigned to the task
   */
  @Override
  public void open(final Collection<TopicPartition> partitions) {
    long startTime = System.currentTimeMillis();
    this.sink.startPartitions(partitions);
    this.DYNAMIC_LOGGER.info(
        "task opened with {} partitions, execution time: {} milliseconds",
        partitions.size(),
        getDurationFromStartMs(startTime));
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
    this.DYNAMIC_LOGGER.info(
        "closing task {} with {} partitions", this.taskConfigId, partitions.size());
    if (this.sink != null) {
      this.sink.close(partitions);
    }

    this.DYNAMIC_LOGGER.info(
        "task closed, execution time: {} milliseconds",
        this.taskConfigId,
        getDurationFromStartMs(startTime));
  }

  /**
   * ingest records to Snowflake
   *
   * @param records - collection of records from kafka topic/partitions for this connector
   */
  @Override
  public void put(final Collection<SinkRecord> records) {
    this.authorizationExceptionTracker.throwExceptionIfAuthorizationFailed();

    // Check for channel errors detected in preCommit and fail the task
    if (this.channelErrorToFailOn != null) {
      SnowflakeKafkaConnectorException error = this.channelErrorToFailOn;
      this.channelErrorToFailOn = null; // Clear so we don't throw again on restart
      throw new ConnectException(error.getMessage(), error);
    }

    final long recordSize = records.size();
    DYNAMIC_LOGGER.debug("Calling PUT with {} records", recordSize);

    final long startTime = System.currentTimeMillis();

    getSink().insert(records);

    logWarningForPutAndPrecommit(
        startTime, Utils.formatString("Executed PUT with {} records", recordSize), false);
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
    DYNAMIC_LOGGER.info("Precommit started for {} partitions", offsets.size());

    if (DYNAMIC_LOGGER.isDebugEnabled()) {
      DYNAMIC_LOGGER.debug(
          "Precommit partitions and offsets: {}", Arrays.toString(offsets.entrySet().toArray()));
    }

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
            long offset = sink.getOffset(topicPartition);
            if (offset != NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE) {
              committedOffsets.put(topicPartition, new OffsetAndMetadata(offset));
            }
          });
    } catch (SnowflakeKafkaConnectorException e) {
      this.authorizationExceptionTracker.reportPrecommitException(e);
      this.DYNAMIC_LOGGER.error("PreCommit error: {} ", e.getMessage());
      // Channel error count exceeded - store to fail on next put() call
      if (e.checkErrorCode(SnowflakeErrors.ERROR_5030)) {
        this.channelErrorToFailOn = e;
      }
    } catch (Exception e) {
      this.authorizationExceptionTracker.reportPrecommitException(e);
      this.DYNAMIC_LOGGER.error("PreCommit error: {} ", e.getMessage());
    }

    logWarningForPutAndPrecommit(
        startTime,
        Utils.formatString(
            "Executed PRECOMMIT on all {} partitions, safe to commit {} partitions",
            offsets.size(),
            committedOffsets.size()),
        true);
    return committedOffsets;
  }

  /**
   * @return connector version
   */
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
  public static Map<String, String> getTopicToTableMap(Map<String, String> config) {
    if (config.containsKey(KafkaConnectorConfigParams.SNOWFLAKE_TOPICS2TABLE_MAP)) {
      Map<String, String> result =
          Utils.parseTopicToTableMap(
              config.get(KafkaConnectorConfigParams.SNOWFLAKE_TOPICS2TABLE_MAP));
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

  private static long getDurationFromStartMs(long startTime) {
    final long currTime = System.currentTimeMillis();
    return currTime - startTime;
  }

  void logWarningForPutAndPrecommit(long startTime, String logContent, boolean isPrecommit) {
    final long executionTimeMs = getDurationFromStartMs(startTime);
    String logExecutionContent =
        Utils.formatString("{}, executionTime: {} ms", logContent, executionTimeMs);

    if (executionTimeMs > 300000) {
      // This won't be frequently printed. It is vary rare to have execution greater than 300
      // seconds.
      // But having this warning helps customer to debug their Kafka Connect config.
      this.DYNAMIC_LOGGER.warn(
          "{}. Expected call to be under {} ms. If there is CommitFailedException in the log or"
              + " there is duplicated records, refer to this link for solution: "
              + "https://docs.snowflake.com/en/user-guide/kafka-connector-ts.html#resolving-specific-issues",
          logExecutionContent,
          executionTimeMs);
    } else {
      if (isPrecommit) {
        this.DYNAMIC_LOGGER.info(logExecutionContent);
      } else {
        this.DYNAMIC_LOGGER.debug(logExecutionContent);
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
