package com.snowflake.kafka.connector.internal;

import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.internal.streaming.IngestionMethodConfig;
import com.snowflake.kafka.connector.records.SnowflakeMetadataConfig;
import java.util.Map;

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
  public static class SnowflakeSinkServiceBuilder extends Logging {
    private final SnowflakeSinkService service;

    private SnowflakeSinkServiceBuilder(
        SnowflakeConnectionService conn,
        IngestionMethodConfig ingestionType,
        Map<String, String> connectorConfig) {
      if (ingestionType == IngestionMethodConfig.SNOWPIPE) {
        this.service = new SnowflakeSinkServiceV1(conn);
      } else {
        // Use SinkServiceV2
        // TODO:SNOW-447418
        service = null;
      }

      logInfo("{} created", this.service.getClass().getName());
    }

    private SnowflakeSinkServiceBuilder(SnowflakeConnectionService conn) {
      this.service = new SnowflakeSinkServiceV1(conn);
    }

    public SnowflakeSinkServiceBuilder addTask(String tableName, String topic, int partition) {
      this.service.startTask(tableName, topic, partition);
      logInfo(
          "create new task in {} - table: {}, topic: {}, partition: {}",
          SnowflakeSinkService.class.getName(),
          tableName,
          topic,
          partition);
      return this;
    }

    public SnowflakeSinkServiceBuilder setRecordNumber(long num) {
      this.service.setRecordNumber(num);
      logInfo("record number is limited to {}", num);
      return this;
    }

    public SnowflakeSinkServiceBuilder setFileSize(long size) {
      this.service.setFileSize(size);
      logInfo("file size is limited to {}", size);
      return this;
    }

    public SnowflakeSinkServiceBuilder setFlushTime(long time) {
      this.service.setFlushTime(time);
      logInfo("flush time is limited to {}", time);
      return this;
    }

    public SnowflakeSinkServiceBuilder setTopic2TableMap(Map<String, String> topic2TableMap) {
      this.service.setTopic2TableMap(topic2TableMap);
      StringBuilder map = new StringBuilder();
      for (Map.Entry<String, String> entry : topic2TableMap.entrySet()) {
        map.append(entry.getKey()).append(" -> ").append(entry.getValue()).append("\n");
      }
      logInfo("set topic 2 table map \n {}", map.toString());
      return this;
    }

    public SnowflakeSinkServiceBuilder setMetadataConfig(SnowflakeMetadataConfig configMap) {
      this.service.setMetadataConfig(configMap);
      logInfo("metadata config map is {}", configMap.toString());
      return this;
    }

    public SnowflakeSinkServiceBuilder setBehaviorOnNullValuesConfig(
        SnowflakeSinkConnectorConfig.BehaviorOnNullValues behavior) {
      this.service.setBehaviorOnNullValuesConfig(behavior);
      logInfo("Config Behavior on null value is {}", behavior.toString());
      return this;
    }

    public SnowflakeSinkServiceBuilder setCustomJMXMetrics(final boolean enableJMX) {
      this.service.setCustomJMXMetrics(enableJMX);
      logInfo("Config JMX value {}. (true = Enabled, false = Disabled)", enableJMX);
      return this;
    }

    public SnowflakeSinkServiceBuilder setDeliveryGuarantee(
        SnowflakeSinkConnectorConfig.IngestionDeliveryGuarantee ingestionDeliveryGuarantee) {
      this.service.setDeliveryGuarantee(ingestionDeliveryGuarantee);
      logInfo("Config Delivery Guarantee type {}.", ingestionDeliveryGuarantee.toString());
      return this;
    }

    public SnowflakeSinkService build() {
      logInfo("{} created", SnowflakeSinkService.class.getName());
      return service;
    }
  }
}
