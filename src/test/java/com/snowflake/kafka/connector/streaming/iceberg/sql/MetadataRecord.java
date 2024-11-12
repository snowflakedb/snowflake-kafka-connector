package com.snowflake.kafka.connector.streaming.iceberg.sql;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.snowflake.kafka.connector.Utils;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Objects;
import org.assertj.core.api.Assertions;

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

  private static final ObjectMapper MAPPER = new ObjectMapper();

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

  public static MetadataRecord fromMetadataSingleRow(ResultSet resultSet) {
    try {
      String jsonString = resultSet.getString(Utils.TABLE_COLUMN_METADATA);
      return MAPPER.readValue(jsonString, MetadataRecord.class);
    } catch (SQLException | IOException e) {
      Assertions.fail("Couldn't map ResultSet to MetadataRecord: " + e.getMessage());
    }
    return null;
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

  public static class RecordWithMetadata<T> {
    private final T record;
    private final MetadataRecord metadata;

    private RecordWithMetadata(MetadataRecord metadata, T record) {
      this.record = record;
      this.metadata = metadata;
    }

    public static <T> RecordWithMetadata<T> of(MetadataRecord metadata, T record) {
      return new RecordWithMetadata<>(metadata, record);
    }

    public T getRecord() {
      return record;
    }

    public MetadataRecord getMetadata() {
      return metadata;
    }
  }
}
