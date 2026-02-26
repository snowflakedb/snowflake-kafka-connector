/*
 * Copyright (c) 2026 Snowflake Computing Inc. All rights reserved.
 *
 * Ported from KC v3.2 for client-side schema evolution in KC v4.
 */

package com.snowflake.kafka.connector.internal.schemaevolution;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.connect.data.Schema;

/**
 * Abstract base for mapping Kafka Connect types to Snowflake DDL types.
 */
public abstract class ColumnTypeMapper {

  public String mapToColumnType(Schema.Type kafkaType) {
    return mapToColumnType(kafkaType, null);
  }

  public abstract String mapToColumnType(Schema.Type kafkaType, String schemaName);

  /**
   * Map the JSON node type to Kafka type.
   *
   * @param value JSON node
   * @return Kafka type
   */
  public abstract Schema.Type mapJsonNodeTypeToKafkaType(JsonNode value);

  /**
   * Infer Snowflake DDL type from a Java object value.
   * Used for schema-less records where we need to determine column types from data.
   *
   * @param value Java object value
   * @return Snowflake DDL type string
   */
  public abstract String inferTypeFromJavaValue(Object value);
}
