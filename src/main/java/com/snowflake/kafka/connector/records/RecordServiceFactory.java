package com.snowflake.kafka.connector.records;

import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper;

public class RecordServiceFactory {
  public static RecordService createRecordService(
      boolean isIcebergEnabled, boolean enableSchematization) {
    if (isIcebergEnabled) {
      return new RecordService(
          enableSchematization, new IcebergTableStreamingRecordMapper(new ObjectMapper()));
    } else {
      return new RecordService(
          enableSchematization, new SnowflakeTableStreamingRecordMapper(new ObjectMapper()));
    }
  }
}
