package com.snowflake.kafka.connector.internal.streaming.schemaevolution.iceberg;

import com.google.common.annotations.VisibleForTesting;
import com.snowflake.kafka.connector.internal.streaming.schemaevolution.TableSchemaResolver;

public class IcebergTableSchemaResolver extends TableSchemaResolver {

  @VisibleForTesting
  IcebergTableSchemaResolver(IcebergColumnTypeMapper columnTypeMapper) {
    super(columnTypeMapper);
  }

  public IcebergTableSchemaResolver() {
    super(new IcebergColumnTypeMapper());
  }
}
