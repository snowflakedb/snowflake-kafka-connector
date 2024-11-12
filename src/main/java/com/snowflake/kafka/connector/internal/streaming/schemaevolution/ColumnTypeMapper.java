package com.snowflake.kafka.connector.internal.streaming.schemaevolution;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.connect.data.Schema;

public abstract class ColumnTypeMapper {

  public String mapToColumnType(Schema.Type kafkaType) {
    return mapToColumnType(kafkaType, null);
  }

  public abstract String mapToColumnType(Schema.Type kafkaType, String schemaName);

  /**
   * Map the JSON node type to Kafka type
   *
   * @param value JSON node
   * @return Kafka type
   */
  public abstract Schema.Type mapJsonNodeTypeToKafkaType(JsonNode value);
}
