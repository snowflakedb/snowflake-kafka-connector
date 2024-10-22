package com.snowflake.kafka.connector.records;

import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper;

public class RecordServiceFactory {
  public static RecordService createRecordService(
      boolean isIcebergEnabled, boolean enableSchematization) {
    ObjectMapper objectMapper = new ObjectMapper();
    if (isIcebergEnabled) {
      return new RecordService(
          enableSchematization, new IcebergTableStreamingRecordMapper(objectMapper), objectMapper);
    } else {
      return new RecordService(
          enableSchematization,
          new SnowflakeTableStreamingRecordMapper(objectMapper),
          objectMapper);
    }
  }
}
