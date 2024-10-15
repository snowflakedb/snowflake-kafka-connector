package com.snowflake.kafka.connector.internal.streaming.schemaevolution.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.stream.Stream;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.JsonNode;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class IcebergColumnTypeMapperTest {

  private final IcebergColumnTypeMapper mapper = new IcebergColumnTypeMapper();

  @ParameterizedTest(name = "should map Kafka type {0} to Snowflake column type {2}")
  @MethodSource("kafkaTypesToMap")
  void shouldMapKafkaTypeToSnowflakeColumnType(
      Schema.Type kafkaType, String schemaName, String expectedSnowflakeType) {
    assertThat(mapper.mapToColumnType(kafkaType, schemaName)).isEqualTo(expectedSnowflakeType);
  }

  @ParameterizedTest()
  @MethodSource("jsonNodeTypesToMap")
  void shouldMapJsonNodeTypeToKafkaType(JsonNode value, Schema.Type expectedKafkaType) {
    assertThat(mapper.mapJsonNodeTypeToKafkaType(value)).isEqualTo(expectedKafkaType);
  }

  @ParameterizedTest()
  @MethodSource("kafkaTypesToThrowException")
  void shouldThrowExceptionWhenMappingUnsupportedKafkaType(Schema.Type kafkaType) {
    assertThatThrownBy(() -> mapper.mapToColumnType(kafkaType, null))
        .isInstanceOf(IllegalArgumentException.class);
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
        Arguments.of(Schema.Type.BYTES, null, "BINARY"));
  }

  private static Stream<Arguments> kafkaTypesToThrowException() {
    return Stream.of(
        Arguments.of(Schema.Type.ARRAY),
        Arguments.of(Schema.Type.MAP),
        Arguments.of(Schema.Type.STRUCT));
  }

  private static Stream<Arguments> jsonNodeTypesToMap() {
    return Stream.of(
        Arguments.of(JsonNodeFactory.instance.nullNode(), Schema.Type.STRING),
        Arguments.of(JsonNodeFactory.instance.numberNode((short) 1), Schema.Type.INT64),
        Arguments.of(JsonNodeFactory.instance.numberNode(1), Schema.Type.INT64),
        Arguments.of(JsonNodeFactory.instance.numberNode(1L), Schema.Type.INT64),
        Arguments.of(JsonNodeFactory.instance.numberNode(1.0), Schema.Type.FLOAT64),
        Arguments.of(JsonNodeFactory.instance.numberNode(1.0f), Schema.Type.FLOAT32),
        Arguments.of(JsonNodeFactory.instance.booleanNode(true), Schema.Type.BOOLEAN),
        Arguments.of(JsonNodeFactory.instance.textNode("text"), Schema.Type.STRING),
        Arguments.of(JsonNodeFactory.instance.binaryNode(new byte[] {1, 2, 3}), Schema.Type.BYTES),
        Arguments.of(JsonNodeFactory.instance.arrayNode(), Schema.Type.ARRAY),
        Arguments.of(JsonNodeFactory.instance.objectNode(), Schema.Type.STRUCT));
  }
}
