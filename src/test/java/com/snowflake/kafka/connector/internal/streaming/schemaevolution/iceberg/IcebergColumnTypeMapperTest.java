package com.snowflake.kafka.connector.internal.streaming.schemaevolution.iceberg;

import static com.snowflake.kafka.connector.internal.SnowflakeErrors.ERROR_5026;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException;
import java.util.stream.Stream;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class IcebergColumnTypeMapperTest {

  private final IcebergColumnTypeMapper mapper = new IcebergColumnTypeMapper();

  @ParameterizedTest(name = "should map Kafka type {0} to Snowflake column type {2}")
  @MethodSource("kafkaTypesToMap")
  void shouldMapKafkaTypeToSnowflakeColumnType(
      Schema.Type kafkaType, String schemaName, String expectedSnowflakeType) {
    assertThat(mapper.mapToColumnTypeFromKafkaSchema(kafkaType, schemaName))
        .isEqualTo(expectedSnowflakeType);
  }

  @ParameterizedTest()
  @MethodSource("jsonNodeTypesToMap")
  void shouldMapJsonNodeTypeToKafkaType(JsonNode value, Schema.Type expectedKafkaType) {
    assertThat(mapper.mapJsonNodeTypeToKafkaType("test", value)).isEqualTo(expectedKafkaType);
  }

  @ParameterizedTest()
  @MethodSource("jsonNodeValuesToThrowException")
  void shouldThrowExceptionWhenMappingEmptyOrNullNode(JsonNode value) {
    assertThatThrownBy(() -> mapper.mapJsonNodeTypeToKafkaType("test", value))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining("'test' field value is null or empty")
        .matches(
            e -> ((SnowflakeKafkaConnectorException) e).getCode().equals(ERROR_5026.getCode()));
  }

  @Test
  void shouldThrowExceptionForNullValue() {
    assertThatThrownBy(() -> mapper.mapJsonNodeTypeToKafkaType("test", null))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining("'test' field value is null or empty")
        .matches(
            e -> ((SnowflakeKafkaConnectorException) e).getCode().equals(ERROR_5026.getCode()));
  }

  private static Stream<Arguments> kafkaTypesToMap() {
    return Stream.of(
        Arguments.of(Schema.Type.INT8, null, "INT"),
        Arguments.of(Schema.Type.INT16, null, "INT"),
        Arguments.of(Schema.Type.INT32, Date.LOGICAL_NAME, "DATE"),
        Arguments.of(Schema.Type.INT32, Time.LOGICAL_NAME, "TIME(6)"),
        Arguments.of(Schema.Type.INT32, null, "INT"),
        Arguments.of(Schema.Type.INT64, Timestamp.LOGICAL_NAME, "TIMESTAMP(6)"),
        Arguments.of(Schema.Type.INT64, null, "LONG"),
        Arguments.of(Schema.Type.FLOAT32, null, "FLOAT"),
        Arguments.of(Schema.Type.FLOAT64, null, "DOUBLE"),
        Arguments.of(Schema.Type.BOOLEAN, null, "BOOLEAN"),
        Arguments.of(Schema.Type.STRING, null, "VARCHAR"),
        Arguments.of(Schema.Type.BYTES, Decimal.LOGICAL_NAME, "VARCHAR"),
        Arguments.of(Schema.Type.BYTES, null, "BINARY"),
        Arguments.of(Schema.Type.MAP, null, "MAP"),
        Arguments.of(Schema.Type.ARRAY, null, "ARRAY"),
        Arguments.of(Schema.Type.STRUCT, null, "OBJECT"));
  }

  private static Stream<Arguments> jsonNodeTypesToMap() {
    return Stream.of(
        Arguments.of(JsonNodeFactory.instance.numberNode((short) 1), Schema.Type.INT64),
        Arguments.of(JsonNodeFactory.instance.numberNode(1), Schema.Type.INT64),
        Arguments.of(JsonNodeFactory.instance.numberNode(1L), Schema.Type.INT64),
        Arguments.of(JsonNodeFactory.instance.numberNode(1.0), Schema.Type.FLOAT64),
        Arguments.of(JsonNodeFactory.instance.numberNode(1.0f), Schema.Type.FLOAT32),
        Arguments.of(JsonNodeFactory.instance.booleanNode(true), Schema.Type.BOOLEAN),
        Arguments.of(JsonNodeFactory.instance.textNode("text"), Schema.Type.STRING),
        Arguments.of(JsonNodeFactory.instance.binaryNode(new byte[] {1, 2, 3}), Schema.Type.BYTES),
        Arguments.of(JsonNodeFactory.instance.arrayNode().add(1), Schema.Type.ARRAY),
        Arguments.of(JsonNodeFactory.instance.objectNode().put("test", 1), Schema.Type.STRUCT));
  }

  private static Stream<Arguments> jsonNodeValuesToThrowException() {
    return Stream.of(
        Arguments.of(JsonNodeFactory.instance.nullNode()),
        Arguments.of(JsonNodeFactory.instance.arrayNode()),
        Arguments.of(JsonNodeFactory.instance.objectNode()));
  }
}
