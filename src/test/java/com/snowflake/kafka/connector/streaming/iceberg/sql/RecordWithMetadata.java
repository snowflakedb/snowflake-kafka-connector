package com.snowflake.kafka.connector.streaming.iceberg.sql;

public class RecordWithMetadata<T> {
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
