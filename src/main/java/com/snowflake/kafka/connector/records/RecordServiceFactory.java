package com.snowflake.kafka.connector.records;

import com.fasterxml.jackson.databind.ObjectMapper;

public class RecordServiceFactory {
  public static RecordService createRecordService(boolean isIcebergEnabled) {
    ObjectMapper objectMapper = new ObjectMapper();
    if (isIcebergEnabled) {
      return new RecordService(new IcebergTableStreamingRecordMapper(objectMapper), objectMapper);
    } else {
      return new RecordService(new SnowflakeTableStreamingRecordMapper(objectMapper), objectMapper);
    }
  }
}
