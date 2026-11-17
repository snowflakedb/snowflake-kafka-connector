package com.snowflake.kafka.connector.internal.streaming.v2.evolveschema;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Timestamp;
import org.junit.jupiter.api.Test;

class ConnectSchemaJsonTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  void primitive() throws Exception {
    assertJson(Schema.INT64_SCHEMA, "{\"type\":\"int64\",\"optional\":false}");
    assertJson(Schema.OPTIONAL_STRING_SCHEMA, "{\"type\":\"string\",\"optional\":true}");
  }

  @Test
  void logicalTypeWritesNameVersionAndParameters() throws Exception {
    assertJson(
        Timestamp.SCHEMA,
        "{\"type\":\"int64\",\"optional\":false,"
            + "\"name\":\"org.apache.kafka.connect.data.Timestamp\",\"version\":1}");
    assertJson(
        Decimal.builder(2).parameter("connect.decimal.precision", "10").build(),
        "{\"type\":\"bytes\",\"optional\":false,"
            + "\"name\":\"org.apache.kafka.connect.data.Decimal\",\"version\":1,"
            + "\"parameters\":{\"scale\":\"2\",\"connect.decimal.precision\":\"10\"}}");
  }

  @Test
  void docIsWrittenAndDefaultIsOmitted() throws Exception {
    assertJson(
        SchemaBuilder.int32().doc("count").defaultValue(5).build(),
        "{\"type\":\"int32\",\"optional\":false,\"doc\":\"count\"}");
  }

  @Test
  void arrayAndMap() throws Exception {
    assertJson(
        SchemaBuilder.array(Schema.STRING_SCHEMA).optional().build(),
        "{\"type\":\"array\",\"items\":{\"type\":\"string\",\"optional\":false},"
            + "\"optional\":true}");
    assertJson(
        SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.FLOAT64_SCHEMA).build(),
        "{\"type\":\"map\",\"keys\":{\"type\":\"string\",\"optional\":false},"
            + "\"values\":{\"type\":\"float64\",\"optional\":false},\"optional\":false}");
  }

  @Test
  void nestedStruct() throws Exception {
    Schema inner = SchemaBuilder.struct().optional().field("b", Schema.BOOLEAN_SCHEMA).build();
    Schema outer =
        SchemaBuilder.struct()
            .name("Value")
            .field("id", Schema.INT64_SCHEMA)
            .field("inner", inner)
            .build();
    assertJson(
        outer,
        "{\"type\":\"struct\",\"fields\":["
            + "{\"type\":\"int64\",\"optional\":false,\"field\":\"id\"},"
            + "{\"type\":\"struct\",\"fields\":["
            + "{\"type\":\"boolean\",\"optional\":false,\"field\":\"b\"}],"
            + "\"optional\":true,\"field\":\"inner\"}],"
            + "\"optional\":false,\"name\":\"Value\"}");
  }

  private static void assertJson(Schema schema, String expected) throws Exception {
    JsonNode actual = ConnectSchemaJson.toJson(schema);
    assertThat(actual).isEqualTo(MAPPER.readTree(expected));
  }
}
