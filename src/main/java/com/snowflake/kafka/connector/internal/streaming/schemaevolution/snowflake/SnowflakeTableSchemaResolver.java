package com.snowflake.kafka.connector.internal.streaming.schemaevolution.snowflake;

import com.snowflake.kafka.connector.internal.streaming.schemaevolution.TableSchemaResolver;

public class SnowflakeTableSchemaResolver extends TableSchemaResolver {
  public SnowflakeTableSchemaResolver(SnowflakeColumnTypeMapper columnTypeMapper) {
    super(columnTypeMapper);
  }
}
