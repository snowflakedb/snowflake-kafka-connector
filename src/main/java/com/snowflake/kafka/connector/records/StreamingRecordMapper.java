package com.snowflake.kafka.connector.records;

import com.snowflake.kafka.connector.records.RecordService.SnowflakeTableRow;
import java.util.Map;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.core.JsonProcessingException;

interface StreamingRecordMapper {
  Map<String, Object> processSnowflakeRecord(
      SnowflakeTableRow row, boolean schematizationEnabled, boolean includeAllMetadata)
      throws JsonProcessingException;
}
