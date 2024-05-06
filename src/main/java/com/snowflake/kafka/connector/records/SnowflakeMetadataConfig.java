package com.snowflake.kafka.connector.records;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.SNOWFLAKE_METADATA_ALL;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.SNOWFLAKE_METADATA_CREATETIME;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.SNOWFLAKE_METADATA_OFFSET_AND_PARTITION;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.SNOWFLAKE_METADATA_TOPIC;

import com.google.common.base.MoreObjects;
import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class SnowflakeMetadataConfig {
  final boolean createtimeFlag;
  final boolean topicFlag;
  final boolean offsetAndPartitionFlag;
  final boolean allFlag;

  /** initialize with default config */
  public SnowflakeMetadataConfig() {
    this(new HashMap<>());
  }

  /**
   * Set flag to false only if metadata config is not set to "true" in config.
   *
   * @param config a String to String map of configs
   */
  public SnowflakeMetadataConfig(Map<String, String> config) {
    createtimeFlag = getMetadataProperty(config, SNOWFLAKE_METADATA_CREATETIME);
    topicFlag = getMetadataProperty(config, SNOWFLAKE_METADATA_TOPIC);
    offsetAndPartitionFlag = getMetadataProperty(config, SNOWFLAKE_METADATA_OFFSET_AND_PARTITION);
    allFlag = getMetadataProperty(config, SNOWFLAKE_METADATA_ALL);
  }

  private static boolean getMetadataProperty(Map<String, String> config, String property) {
    String value =
        Optional.ofNullable(config.get(property))
            .orElse(SnowflakeSinkConnectorConfig.SNOWFLAKE_METADATA_DEFAULT);

    // Cannot use Boolean.parseBoolean in order to ensure backward compatibility.
    // The equality check must be case sensitive.
    return "true".equals(value);
  }

  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("createtimeFlag", createtimeFlag)
        .add("topicFlag", topicFlag)
        .add("offsetAndPartitionFlag", offsetAndPartitionFlag)
        .add("allFlag", allFlag)
        .toString();
  }
}
