package com.snowflake.kafka.connector.builder;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;

public class SinkRecordBuilder {

  private final String topic;
  private final int partition;
  private Schema keySchema = Schema.STRING_SCHEMA;
  private Object key = "key";
  private Schema valueSchema = Schema.STRING_SCHEMA;
  private Object value = "{\"name\":123}";
  private long offset = 0;

  private SinkRecordBuilder(String topic, int partition) {
    this.topic = topic;
    this.partition = partition;
  }

  public static SinkRecordBuilder forTopicPartition(String topic, int partition) {
    return new SinkRecordBuilder(topic, partition);
  }

  public SinkRecord build() {
    return new SinkRecord(topic, partition, keySchema, key, valueSchema, value, offset);
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

  public SinkRecordBuilder withOffset(long offset) {
    this.offset = offset;
    return this;
  }
}
