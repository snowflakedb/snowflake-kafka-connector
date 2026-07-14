package com.snowflake.kafka.connector.records;

import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_FEATURE_STRUCTURED_HEADERS;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_FEATURE_STRUCTURED_HEADERS_DEFAULT;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_METADATA_ALL;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_METADATA_CREATETIME;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_METADATA_OFFSET_AND_PARTITION;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_METADATA_TOPIC;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_STREAMING_METADATA_CONNECTOR_PUSH_TIME;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_STREAMING_METADATA_CONNECTOR_PUSH_TIME_DEFAULT;

import com.google.common.base.MoreObjects;
import com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class SnowflakeMetadataConfig {
  final boolean createtimeFlag;
  final boolean connectorPushTimeFlag;
  final boolean topicFlag;
  final boolean offsetAndPartitionFlag;
  final boolean allFlag;
  final boolean structuredHeadersFlag;

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

    connectorPushTimeFlag =
        Optional.ofNullable(config.get(SNOWFLAKE_STREAMING_METADATA_CONNECTOR_PUSH_TIME))
            .map(Boolean::parseBoolean)
            .orElse(SNOWFLAKE_STREAMING_METADATA_CONNECTOR_PUSH_TIME_DEFAULT);

    structuredHeadersFlag =
        Optional.ofNullable(config.get(SNOWFLAKE_FEATURE_STRUCTURED_HEADERS))
            .map(Boolean::parseBoolean)
            .orElse(SNOWFLAKE_FEATURE_STRUCTURED_HEADERS_DEFAULT);
  }

  public boolean shouldIncludeAllMetadata() {
    return allFlag;
  }

  /**
   * Returns {@code true} if all metadata fields that map to the managed-Iceberg {@code
   * RECORD_METADATA} structured schema are enabled: topic, offset+partition, createtime, and
   * connectorPushTime. Managed-Iceberg casts {@code RECORD_METADATA} to a typed OBJECT with a
   * strict cast that fails when a declared field is absent from the map.
   */
  public boolean isFullIcebergMetadataEnabled() {
    return topicFlag && offsetAndPartitionFlag && createtimeFlag && connectorPushTimeFlag;
  }

  private static boolean getMetadataProperty(Map<String, String> config, String property) {
    String value =
        Optional.ofNullable(config.get(property))
            .orElse(KafkaConnectorConfigParams.SNOWFLAKE_METADATA_ALL_DEFAULT);

    return Boolean.parseBoolean(value);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("createtimeFlag", createtimeFlag)
        .add("connectorPushTimeFlag", connectorPushTimeFlag)
        .add("topicFlag", topicFlag)
        .add("offsetAndPartitionFlag", offsetAndPartitionFlag)
        .add("allFlag", allFlag)
        .add("structuredHeadersFlag", structuredHeadersFlag)
        .toString();
  }
}
