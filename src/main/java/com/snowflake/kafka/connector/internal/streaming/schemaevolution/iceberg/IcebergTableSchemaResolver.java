package com.snowflake.kafka.connector.internal.streaming.schemaevolution.iceberg;

import com.snowflake.kafka.connector.internal.streaming.schemaevolution.TableSchemaResolver;

public class IcebergTableSchemaResolver extends TableSchemaResolver {
  public IcebergTableSchemaResolver(IcebergColumnTypeMapper columnTypeMapper) {
    super(columnTypeMapper);
  }
}
