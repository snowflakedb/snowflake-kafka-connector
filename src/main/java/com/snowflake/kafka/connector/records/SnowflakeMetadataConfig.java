package com.snowflake.kafka.connector.records;

import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;

import java.util.HashMap;
import java.util.Map;

public class SnowflakeMetadataConfig {
  final boolean createtimeFlag;
  final boolean topicFlag;
  final boolean offsetAndPartitionFlag;
  final boolean allFlag;

  /**
   * initialize with default config
   */
  public SnowflakeMetadataConfig()
  {
    this(new HashMap<String, String>());
  }

  /**
   * set flag to false only if metadata config is set to false in config
   * @param config a String to String map of configs
   */
  public SnowflakeMetadataConfig(Map<String, String> config)
  {
    // have those local variable to avoid assigning to final values multiple times
    // these values are the default values of the configuration
    boolean createtime = true;
    boolean topic = true;
    boolean offsetAndPartition = true;
    boolean all = true;
    if (config.containsKey(SnowflakeSinkConnectorConfig.SNOWFLAKE_METADATA_CREATETIME) &&
        !config.get(SnowflakeSinkConnectorConfig.SNOWFLAKE_METADATA_CREATETIME)
          .equals(SnowflakeSinkConnectorConfig.SNOWFLAKE_METADATA_DEFAULT))
    {
      createtime = false;
    }
    if (config.containsKey(SnowflakeSinkConnectorConfig.SNOWFLAKE_METADATA_TOPIC) &&
      !config.get(SnowflakeSinkConnectorConfig.SNOWFLAKE_METADATA_TOPIC)
        .equals(SnowflakeSinkConnectorConfig.SNOWFLAKE_METADATA_DEFAULT))
    {
      topic = false;
    }
    if (config.containsKey(SnowflakeSinkConnectorConfig.SNOWFLAKE_METADATA_OFFSET_AND_PARTITION) &&
      !config.get(SnowflakeSinkConnectorConfig.SNOWFLAKE_METADATA_OFFSET_AND_PARTITION)
        .equals(SnowflakeSinkConnectorConfig.SNOWFLAKE_METADATA_DEFAULT))
    {
      offsetAndPartition = false;
    }
    if (config.containsKey(SnowflakeSinkConnectorConfig.SNOWFLAKE_METADATA_ALL) &&
      !config.get(SnowflakeSinkConnectorConfig.SNOWFLAKE_METADATA_ALL)
        .equals(SnowflakeSinkConnectorConfig.SNOWFLAKE_METADATA_DEFAULT))
    {
      all = false;
    }

    createtimeFlag = createtime;
    topicFlag = topic;
    offsetAndPartitionFlag = offsetAndPartition;
    allFlag = all;

  }

  public String toString()
  {
    return
      "{createtimeFlag: " + createtimeFlag + ", " +
      "topicFlag: " + topicFlag + ", " +
      "offsetAndPartitionFlag: " + offsetAndPartitionFlag + ", " +
      "allFlag: " + allFlag + "}";
  }
}
