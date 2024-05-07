package com.snowflake.kafka.connector.builder;

import com.google.common.base.Preconditions;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.sink.SinkRecord;

public class SinkRecordBuilder {

  private final String topic;
  private final int partition;
  private Schema keySchema = Schema.STRING_SCHEMA;
  private Object key = "key";
  private Schema valueSchema = Schema.STRING_SCHEMA;
  private Object value = "{\"name\":123}";
  private long offset = 0;
  private Long timestamp = null;
  private TimestampType timestampType = TimestampType.NO_TIMESTAMP_TYPE;

  private SinkRecordBuilder(String topic, int partition) {
    this.topic = topic;
    this.partition = partition;
  }

  public static SinkRecordBuilder forTopicPartition(String topic, int partition) {
    return new SinkRecordBuilder(topic, partition);
  }

  public SinkRecord build() {
    return new SinkRecord(
        topic, partition, keySchema, key, valueSchema, value, offset, timestamp, timestampType);
  }

  public SinkRecordBuilder withKeySchema(Schema keySchema) {
    this.keySchema = keySchema;
    return this;
  }

  public SinkRecordBuilder withKey(Object key) {
    this.key = key;
    return this;
  }

  public SinkRecordBuilder withValueSchema(Schema valueSchema) {
    this.valueSchema = valueSchema;
    return this;
  }

  public SinkRecordBuilder withValue(Object value) {
    this.value = value;
    return this;
  }

  public SinkRecordBuilder withSchemaAndValue(SchemaAndValue schemaAndValue) {
    this.valueSchema = schemaAndValue.schema();
    this.value = schemaAndValue.value();
    return this;
  }

  public SinkRecordBuilder withOffset(long offset) {
    this.offset = offset;
    return this;
  }

  public SinkRecordBuilder withTimestamp(long timestamp, TimestampType timestampType) {
    Preconditions.checkArgument(
        timestampType != TimestampType.NO_TIMESTAMP_TYPE,
        "NO_TIMESTAMP_TYPE is the default timestampType");

    this.timestamp = timestamp;
    this.timestampType = timestampType;
    return this;
  }
}
