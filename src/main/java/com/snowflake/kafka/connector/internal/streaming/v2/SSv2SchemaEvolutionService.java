package com.snowflake.kafka.connector.internal.streaming.v2;

import com.snowflake.kafka.connector.internal.streaming.validation.RowSchema;
import java.util.Map;
import org.apache.kafka.connect.sink.SinkRecord;

public interface SSv2SchemaEvolutionService {
  void evolveSchemaIfNeeded(
      SinkRecord kafkaSinkRecord, Map<String, Object> transformedRecord, RowSchema rowSchema);
}
