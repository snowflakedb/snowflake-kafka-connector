package com.snowflake.kafka.connector.records;

import com.fasterxml.jackson.databind.ObjectMapper;

public class RecordServiceFactory {
  public static RecordService createRecordService(
      boolean isIcebergEnabled, boolean enableSchematization, boolean ssv2Enabled) {
    ObjectMapper objectMapper = new ObjectMapper();
    if (isIcebergEnabled) {
      return new RecordService(
          new IcebergTableStreamingRecordMapper(objectMapper, enableSchematization, ssv2Enabled),
          objectMapper);
    } else {
      return new RecordService(
          new SnowflakeTableStreamingRecordMapper(objectMapper, enableSchematization, ssv2Enabled),
          objectMapper);
    }
  }
}
