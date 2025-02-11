package com.snowflake.kafka.connector.internal;

import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.dlq.KafkaRecordErrorReporter;
import com.snowflake.kafka.connector.internal.streaming.IngestionMethodConfig;
import com.snowflake.kafka.connector.internal.streaming.SnowflakeSinkServiceV2;
import com.snowflake.kafka.connector.records.SnowflakeMetadataConfig;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkTaskContext;

/** A factory to create {@link SnowflakeSinkService} */
public class SnowflakeSinkServiceFactory {
  /**
   * create service builder. To be used when Snowpipe streaming is the method of ingestion.
   *
   * @param conn snowflake connection service
   * @param ingestionType ingestion Type based on config
   * @param connectorConfig KC config map
   * @return a builder instance
   */
  public static SnowflakeSinkServiceBuilder builder(
      SnowflakeConnectionService conn,
      IngestionMethodConfig ingestionType,
      Map<String, String> connectorConfig) {
    return new SnowflakeSinkServiceBuilder(conn, ingestionType, connectorConfig);
  }

  /**
   * Basic builder which internally uses SinkServiceV1 (Snowpipe)
   *
   * @param conn connection instance for connecting to snowflake
   * @return A wrapper(Builder) having SinkService instance
   */
  public static SnowflakeSinkServiceBuilder builder(SnowflakeConnectionService conn) {
    return new SnowflakeSinkServiceBuilder(conn);
  }

  /** Builder class to create instance of {@link SnowflakeSinkService} */
  public static class SnowflakeSinkServiceBuilder {
    private final KCLogger LOGGER = new KCLogger(SnowflakeSinkServiceBuilder.class.getName());

    private final SnowflakeSinkService service;

    private SnowflakeSinkServiceBuilder(
        SnowflakeConnectionService conn,
        IngestionMethodConfig ingestionType,
        Map<String, String> connectorConfig) {
      if (ingestionType == IngestionMethodConfig.SNOWPIPE) {
        long v2CleanerIntervalSeconds =
            SnowflakeSinkConnectorConfig.SNOWPIPE_FILE_CLEANER_INTERVAL_SECONDS_DEFAULT;
        if (connectorConfig != null
            && connectorConfig.containsKey(
                SnowflakeSinkConnectorConfig.SNOWPIPE_FILE_CLEANER_INTERVAL_SECONDS)) {
          v2CleanerIntervalSeconds =
              Long.parseLong(
                  connectorConfig.get(
                      SnowflakeSinkConnectorConfig.SNOWPIPE_FILE_CLEANER_INTERVAL_SECONDS));
        }

        SnowflakeSinkServiceV1 svc = new SnowflakeSinkServiceV1(conn, v2CleanerIntervalSeconds);
        this.service = svc;
        boolean useStageFilesProcessor =
            SnowflakeSinkConnectorConfig.SNOWPIPE_FILE_CLEANER_FIX_ENABLED_DEFAULT;
        int threadCount = SnowflakeSinkConnectorConfig.SNOWPIPE_FILE_CLEANER_THREADS_DEFAULT;

        if (connectorConfig != null
            && connectorConfig.containsKey(
                SnowflakeSinkConnectorConfig.SNOWPIPE_FILE_CLEANER_FIX_ENABLED)) {
          useStageFilesProcessor =
              Boolean.parseBoolean(
                  connectorConfig.get(
                      SnowflakeSinkConnectorConfig.SNOWPIPE_FILE_CLEANER_FIX_ENABLED));
        }
        if (connectorConfig != null
            && connectorConfig.containsKey(
                SnowflakeSinkConnectorConfig.SNOWPIPE_FILE_CLEANER_THREADS)) {
          threadCount =
              Integer.parseInt(
                  connectorConfig.get(SnowflakeSinkConnectorConfig.SNOWPIPE_FILE_CLEANER_THREADS));
        }

        if (useStageFilesProcessor) {
          svc.enableStageFilesProcessor(threadCount);
        }

        boolean extendedStageFileNameFix =
            SnowflakeSinkConnectorConfig.SNOWPIPE_SINGLE_TABLE_MULTIPLE_TOPICS_FIX_ENABLED_DEFAULT;
        if (connectorConfig != null
            && connectorConfig.containsKey(
                SnowflakeSinkConnectorConfig.SNOWPIPE_SINGLE_TABLE_MULTIPLE_TOPICS_FIX_ENABLED)) {
          extendedStageFileNameFix =
              Boolean.parseBoolean(
                  connectorConfig.get(
                      SnowflakeSinkConnectorConfig
                          .SNOWPIPE_SINGLE_TABLE_MULTIPLE_TOPICS_FIX_ENABLED));
        }
        svc.configureSingleTableLoadFromMultipleTopics(extendedStageFileNameFix);

        boolean enableReprocessFilesCleanup =
            SnowflakeSinkConnectorConfig.SNOWPIPE_ENABLE_REPROCESS_FILES_CLEANUP_DEFAULT;
        if (connectorConfig != null
            && connectorConfig.containsKey(
                SnowflakeSinkConnectorConfig.SNOWPIPE_ENABLE_REPROCESS_FILES_CLEANUP)) {
          enableReprocessFilesCleanup =
              Boolean.parseBoolean(
                  connectorConfig.get(
                      SnowflakeSinkConnectorConfig.SNOWPIPE_ENABLE_REPROCESS_FILES_CLEANUP));
        }
        svc.configureEnableReprocessFilesCleanup(enableReprocessFilesCleanup);
      } else {
        this.service = new SnowflakeSinkServiceV2(conn, connectorConfig);
      }

      LOGGER.info("{} created", this.service.getClass().getName());
    }

    private SnowflakeSinkServiceBuilder(SnowflakeConnectionService conn) {
      this(conn, IngestionMethodConfig.SNOWPIPE, null /* Not required for V1 */);
    }

    /**
     * Add task for table and TopicPartition. Mostly used only for testing. When connector starts,
     * startTask is directly called.
     *
     * @param tableName tableName in Snowflake
     * @param topicPartition topicPartition containing topic and partition number
     * @return Builder instance
     */
    public SnowflakeSinkServiceBuilder addTask(String tableName, TopicPartition topicPartition) {
      this.service.startPartition(tableName, topicPartition);
      LOGGER.info(
          "create new task in {} - table: {}, topicPartition: {}",
          SnowflakeSinkService.class.getName(),
          tableName,
          topicPartition);
      return this;
    }

    public SnowflakeSinkServiceBuilder setRecordNumber(long num) {
      this.service.setRecordNumber(num);
      LOGGER.info("record number is limited to {}", num);
      return this;
    }

    public SnowflakeSinkServiceBuilder setFileSize(long size) {
      this.service.setFileSize(size);
      LOGGER.info("file size is limited to {}", size);
      return this;
    }

    public SnowflakeSinkServiceBuilder setFlushTime(long time) {
      this.service.setFlushTime(time);
      LOGGER.info("flush time is limited to {}", time);
      return this;
    }

    public SnowflakeSinkServiceBuilder setTopic2TableMap(Map<String, String> topic2TableMap) {
      this.service.setTopic2TableMap(topic2TableMap);
      StringBuilder map = new StringBuilder();
      for (Map.Entry<String, String> entry : topic2TableMap.entrySet()) {
        map.append(entry.getKey()).append(" -> ").append(entry.getValue()).append("\n");
      }
      LOGGER.info("set topic 2 table map \n {}", map.toString());
      return this;
    }

    public SnowflakeSinkServiceBuilder setMetadataConfig(SnowflakeMetadataConfig configMap) {
      this.service.setMetadataConfig(configMap);
      LOGGER.info("metadata config map is {}", configMap.toString());
      return this;
    }

    public SnowflakeSinkServiceBuilder setBehaviorOnNullValuesConfig(
        SnowflakeSinkConnectorConfig.BehaviorOnNullValues behavior) {
      this.service.setBehaviorOnNullValuesConfig(behavior);
      LOGGER.info("Config Behavior on null value is {}", behavior.toString());
      return this;
    }

    public SnowflakeSinkServiceBuilder setCustomJMXMetrics(final boolean enableJMX) {
      this.service.setCustomJMXMetrics(enableJMX);
      LOGGER.info("Config JMX value {}. (true = Enabled, false = Disabled)", enableJMX);
      return this;
    }

    public SnowflakeSinkServiceBuilder setErrorReporter(
        KafkaRecordErrorReporter kafkaRecordErrorReporter) {
      this.service.setErrorReporter(kafkaRecordErrorReporter);
      return this;
    }

    /**
     * Set SinkTaskContext for the respective SnowflakeSinkService instance at runtime.
     *
     * @param sinkTaskContext obtained from {@link org.apache.kafka.connect.sink.SinkTask}
     * @return Builder
     */
    public SnowflakeSinkServiceBuilder setSinkTaskContext(SinkTaskContext sinkTaskContext) {
      this.service.setSinkTaskContext(sinkTaskContext);
      return this;
    }

    public SnowflakeSinkService build() {
      LOGGER.info("{} created", SnowflakeSinkService.class.getName());
      return service;
    }
  }
}
