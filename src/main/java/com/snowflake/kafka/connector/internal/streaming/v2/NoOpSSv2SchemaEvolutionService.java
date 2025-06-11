package com.snowflake.kafka.connector.internal.streaming.v2;

import com.snowflake.kafka.connector.internal.streaming.validation.RowSchema;
import java.util.Map;
import org.apache.kafka.connect.sink.SinkRecord;

/** Implementation used when schema evolution is disabled */
public class NoOpSSv2SchemaEvolutionService implements SSv2SchemaEvolutionService {
  @Override
  public void evolveSchemaIfNeeded(
      SinkRecord kafkaSinkRecord, Map<String, Object> transformedRecord, RowSchema rowSchema) {}
}
