package com.snowflake.kafka.connector.internal.streaming.schemaevolution.iceberg;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import com.snowflake.kafka.connector.internal.streaming.schemaevolution.TableSchemaResolver;
import org.apache.iceberg.types.Type;

public class IcebergTableSchemaResolver extends TableSchemaResolver {

  @VisibleForTesting
  IcebergTableSchemaResolver(IcebergColumnTypeMapper columnTypeMapper) {
    super(columnTypeMapper);
  }

  public IcebergTableSchemaResolver() {
    super(new IcebergColumnTypeMapper());
  }

  public Type schemaFromJson(JsonNode jsonFromRecord) {
    return IcebergDataTypeParser.getTypeFromJson(jsonFromRecord);
  }
}
