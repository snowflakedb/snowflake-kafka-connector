package com.snowflake.kafka.connector.internal.streaming.schemaevolution.snowflake;

import com.google.common.annotations.VisibleForTesting;
import com.snowflake.kafka.connector.internal.streaming.schemaevolution.TableSchemaResolver;

public class SnowflakeTableSchemaResolver extends TableSchemaResolver {

  @VisibleForTesting
  SnowflakeTableSchemaResolver(SnowflakeColumnTypeMapper columnTypeMapper) {
    super(columnTypeMapper);
  }

  public SnowflakeTableSchemaResolver() {
    super(new SnowflakeColumnTypeMapper());
  }
}
