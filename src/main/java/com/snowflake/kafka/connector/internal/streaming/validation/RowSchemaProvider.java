package com.snowflake.kafka.connector.internal.streaming.validation;

import java.util.Map;

public interface RowSchemaProvider {
  RowSchema getRowSchema(String tableName, Map<String, String> connectorConfig);
}
