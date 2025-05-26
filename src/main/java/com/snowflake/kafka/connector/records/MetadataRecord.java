package com.snowflake.kafka.connector.records;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import java.util.Objects;

/**
 * POJO for writing metadata with SSv2. Passing this class to sdk instead of Map makes pipe
 * definition simpler.
 */
public class MetadataRecord {
  private final Long offset;
  private final String topic;
  private final Integer partition;
  private final String key;
  private final Integer schemaId;
  private final Integer keySchemaId;
  private final Long createTime;
  private final Long logAppendTime;
  private final Long snowflakeConnectorPushTime;
  private final Map<String, String> headers;

  @JsonCreator
  public MetadataRecord(
      @JsonProperty("offset") Long offset,
      @JsonProperty("topic") String topic,
      @JsonProperty("partition") Integer partition,
      @JsonProperty("key") String key,
      @JsonProperty("schema_id") Integer schemaId,
      @JsonProperty("key_schema_id") Integer keySchemaId,
      @JsonProperty("CreateTime") Long createTime,
      @JsonProperty("LogAppendTime") Long logAppendTime,
      @JsonProperty("SnowflakeConnectorPushTime") Long snowflakeConnectorPushTime,
      @JsonProperty("headers") Map<String, String> headers) {
    this.offset = offset;
    this.topic = topic;
    this.partition = partition;
    this.key = key;
    this.schemaId = schemaId;
    this.keySchemaId = keySchemaId;
    this.createTime = createTime;
    this.logAppendTime = logAppendTime;
    this.snowflakeConnectorPushTime = snowflakeConnectorPushTime;
    this.headers = headers;
  }

  // Getters for each field
  public Long getOffset() {
    return offset;
  }

  public String getTopic() {
    return topic;
  }

  public Integer getPartition() {
    return partition;
  }

  public String getKey() {
    return key;
  }

  public Integer getSchemaId() {
    return schemaId;
  }

  public Integer getKeySchemaId() {
    return keySchemaId;
  }

  public Long getCreateTime() {
    return createTime;
  }

  public Long getLogAppendTime() {
    return logAppendTime;
  }

  public Long getSnowflakeConnectorPushTime() {
    return snowflakeConnectorPushTime;
  }

  public Map<String, String> getHeaders() {
    return headers;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    MetadataRecord that = (MetadataRecord) o;
    return Objects.equals(offset, that.offset)
        && Objects.equals(topic, that.topic)
        && Objects.equals(partition, that.partition)
        && Objects.equals(key, that.key)
        && Objects.equals(schemaId, that.schemaId)
        && Objects.equals(keySchemaId, that.keySchemaId)
        && Objects.equals(createTime, that.createTime)
        && Objects.equals(logAppendTime, that.logAppendTime)
        && Objects.equals(snowflakeConnectorPushTime, that.snowflakeConnectorPushTime)
        && Objects.equals(headers, that.headers);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        offset,
        topic,
        partition,
        key,
        schemaId,
        keySchemaId,
        createTime,
        logAppendTime,
        snowflakeConnectorPushTime,
        headers);
  }

  @Override
  public String toString() {
    return "MetadataRecord{"
        + "offset="
        + offset
        + ", topic='"
        + topic
        + '\''
        + ", partition="
        + partition
        + ", key='"
        + key
        + '\''
        + ", schemaId="
        + schemaId
        + ", keySchemaId="
        + keySchemaId
        + ", createTime="
        + createTime
        + ", logAppendTime="
        + logAppendTime
        + ", snowflakeConnectorPushTime="
        + snowflakeConnectorPushTime
        + ", headers="
        + headers
        + '}';
  }
}
