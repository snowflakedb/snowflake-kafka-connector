package com.snowflake.kafka.connector.internal.streaming;

import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.streaming.schemaevolution.SchemaEvolutionTargetItems;
import com.snowflake.kafka.connector.internal.streaming.schemaevolution.snowflake.SnowflakeSchemaEvolutionService;
import java.util.Map;
import net.snowflake.ingest.streaming.internal.ColumnProperties;
import org.apache.kafka.connect.sink.SinkRecord;

/** This fake class simulates a delay in running ALTER TABLE statement */
public class DelayedSchemaEvolutionService extends SnowflakeSchemaEvolutionService {

  private final long delayedInMillis;

  public DelayedSchemaEvolutionService(SnowflakeConnectionService conn, long delayedInMillis) {
    super(conn);
    this.delayedInMillis = delayedInMillis;
  }

  @Override
  public void evolveSchemaIfNeeded(
      SchemaEvolutionTargetItems targetItems,
      SinkRecord record,
      Map<String, ColumnProperties> existingSchema) {
    try {
      Thread.sleep(delayedInMillis);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    super.evolveSchemaIfNeeded(targetItems, record, existingSchema);
  }
}
