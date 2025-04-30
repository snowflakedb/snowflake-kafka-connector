package com.snowflake.kafka.connector.records;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;

/**
 * POJO for writing metadata with SSv2. Passing this class to sdk instead of String containing JSON
 * makes pipe definition simpler.
 */
class RecordMetadata {
  private final Long offset;
  private final String topic;
  private final Integer partition;
  private final String key;
  private final Integer schema_id;
  private final Integer key_schema_id;
  private final Long CreateTime;
  private final Long LogAppendTime;
  private final Long SnowflakeConnectorPushTime;
  private final Map<String, String> headers;

  public RecordMetadata(
      long offset,
      String topic,
      int partition,
      String key,
      Integer schemaId,
      Integer keySchemaId,
      Long createTime,
      Long logAppendTime,
      Long snowflakeConnectorPushTime,
      Map<String, String> headers) {
    this.offset = offset;
    this.topic = topic;
    this.partition = partition;
    this.key = key;
    this.schema_id = schemaId;
    this.key_schema_id = keySchemaId;
    this.CreateTime = createTime;
    this.LogAppendTime = logAppendTime;
    this.SnowflakeConnectorPushTime = snowflakeConnectorPushTime;
    this.headers = headers;
  }

  public long getOffset() {
    return offset;
  }

  public String getTopic() {
    return topic;
  }

  public int getPartition() {
    return partition;
  }

  public String getKey() {
    return key;
  }

  public Integer getSchema_id() {
    return schema_id;
  }

  public Integer getKey_schema_id() {
    return key_schema_id;
  }

  @JsonProperty("CreateTime")
  public Long getCreateTime() {
    return CreateTime;
  }

  @JsonProperty("LogAppendTime")
  public Long getLogAppendTime() {
    return LogAppendTime;
  }

  @JsonProperty("SnowflakeConnectorPushTime")
  public Long getSnowflakeConnectorPushTime() {
    return SnowflakeConnectorPushTime;
  }

  public Map<String, String> getHeaders() {
    return headers;
  }
}
