package com.snowflake.kafka.connector.internal.streaming.schemaevolution;

import org.apache.kafka.connect.sink.SinkRecord;

public interface SchemaEvolutionService {

  /**
   * Execute a ALTER TABLE command if there is any extra column that needs to be added, or any
   * column nullability that needs to be updated, used by schema evolution
   *
   * @param targetItems target items for schema evolution such as table name, columns to drop
   *     nullability, and columns to add
   * @param record the sink record that contains the schema and actual data
   */
  void evolveSchemaIfNeeded(SchemaEvolutionTargetItems targetItems, SinkRecord record);
}
